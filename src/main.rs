mod processor;
mod pds;
mod did;
mod utils;

mod crawler {
    use anyhow::Result;
    use atrium_api::com::atproto::sync::list_repos;
    use std::{collections::VecDeque, sync::Arc};
    use tokio::sync::Mutex;
    use tracing::{info, error, warn};

    use crate::{pds::{PdsManager, PdsEndpoint}, did::DidResolver};

    pub struct AccountCrawler {
        pds_manager: Arc<PdsManager>,
        discovered_accounts: Arc<Mutex<VecDeque<String>>>,
        did_resolver: Arc<DidResolver>,
    }

    impl AccountCrawler {
        pub fn new(pds_manager: Arc<PdsManager>, did_resolver: Arc<DidResolver>) -> Self {
            Self {
                pds_manager,
                discovered_accounts: Arc::new(Mutex::new(VecDeque::new())),
                did_resolver,
            }
        }

        pub async fn get_discovered_count(&self) -> usize {
            let queue = self.discovered_accounts.lock().await;
            queue.len()
        }

        pub async fn pop_discovered_account(&self) -> Option<String> {
            let mut queue = self.discovered_accounts.lock().await;
            queue.pop_front()
        }

        pub async fn crawl_all_pdss(&self) -> Result<()> {
            let endpoints = self.pds_manager.get_all_endpoints().await;
            info!("Starting crawl across {} PDS endpoints", endpoints.len());

            let tasks: Vec<_> = endpoints.into_iter()
                .map(|endpoint| {
                    let crawler = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = crawler.crawl_pds(endpoint.clone()).await {
                            error!("Failed to crawl PDS {}: {}", endpoint.url, e);
                        }
                    })
                })
                .collect();

            futures::future::join_all(tasks).await;
            Ok(())
        }

        async fn crawl_pds(&self, endpoint: PdsEndpoint) -> Result<()> {
            let mut cursor: Option<String> = None;
            let mut total_repos = 0;

            info!("Starting crawl of PDS: {}", endpoint.url);

            loop {
                // TODO: Rate limiting per PDS
                // self.rate_limiter.until_ready().await;

                let params = list_repos::ParametersData {
                    limit: Some(atrium_api::types::LimitedNonZeroU16::try_from(1000).unwrap()),
                    cursor: cursor.clone(),
                };

                let response = endpoint.agent
                    .api
                    .com
                    .atproto
                    .sync
                    .list_repos(params.into())
                    .await
                    .map_err(|e| anyhow::anyhow!("listRepos request to {}: {}", endpoint.url, e))?;

                let batch_size = response.repos.len();
                total_repos += batch_size;

                // Add discovered DIDs to queue and cache their PDS mapping
                {
                    let mut queue = self.discovered_accounts.lock().await;
                    for repo in &response.repos {
                        queue.push_back(repo.did.as_str().to_string());
                        
                        // Directly add DID->PDS mapping to cache since we know the PDS address
                        self.did_resolver.insert(repo.did.as_str(), &endpoint.url).await;
                        
                        // Try to resolve this DID to find new PDS endpoints
                        if let Some(pds_url) = self.did_resolver.resolve_did_to_pds(repo.did.as_str()).await {
                            if let Err(e) = self.pds_manager.add_endpoint(pds_url).await {
                                warn!("Failed to add new PDS endpoint: {}", e);
                            }
                        }
                    }
                }

                info!("Crawled {} repos from {} (total: {})", batch_size, endpoint.url, total_repos);

                cursor = response.cursor.clone();
                if cursor.is_none() {
                    break;
                }
            }

            info!("Completed crawl of PDS: {} ({} total repos)", endpoint.url, total_repos);
            Ok(())
        }
    }

    impl Clone for AccountCrawler {
        fn clone(&self) -> Self {
            Self {
                pds_manager: Arc::clone(&self.pds_manager),
                discovered_accounts: Arc::clone(&self.discovered_accounts),
                did_resolver: Arc::clone(&self.did_resolver),
            }
        }
    }
}

mod pipeline {
    use anyhow::Result;
    use atrium_api::types::string::Did;
    use std::{collections::VecDeque, sync::Arc, time::Duration};
    use tokio::{sync::Mutex, time::sleep};
    use tracing::{info, error};

    use crate::{
        pds::PdsManager, 
        did::DidResolver, 
        processor::{process_repo_bytes, FullRecord, RepositoryStats}
    };

    #[derive(Debug, Clone)]
    pub struct ProcessingJob {
        pub did: String,
        pub repo_bytes: Vec<u8>,
        pub source_pds: String,
    }

    #[derive(Debug, Clone)]
    pub struct ProcessingResult {
        pub did: String,
        pub records: Vec<FullRecord>,
        pub stats: RepositoryStats,
    }

    #[derive(Debug, Clone)]
    pub struct PipelineConfig {
        pub download_workers: usize,
        pub processing_workers: usize,
    }

    impl Default for PipelineConfig {
        fn default() -> Self {
            Self {
                download_workers: 20,
                processing_workers: num_cpus::get(),
            }
        }
    }

    pub struct ProcessingPipeline {
        download_queue: Arc<Mutex<VecDeque<String>>>,
        processing_queue: Arc<Mutex<VecDeque<ProcessingJob>>>,
        results_queue: Arc<Mutex<VecDeque<ProcessingResult>>>,
        pds_manager: Arc<PdsManager>,
        did_resolver: Arc<DidResolver>,
        client: Arc<reqwest::Client>,
    }

    impl ProcessingPipeline {
        pub fn new(
            pds_manager: Arc<PdsManager>, 
            did_resolver: Arc<DidResolver>,
            client: Arc<reqwest::Client>
        ) -> Self {
            Self {
                download_queue: Arc::new(Mutex::new(VecDeque::new())),
                processing_queue: Arc::new(Mutex::new(VecDeque::new())),
                results_queue: Arc::new(Mutex::new(VecDeque::new())),
                pds_manager,
                did_resolver,
                client,
            }
        }

        pub async fn add_download_job(&self, did: String) {
            let mut queue = self.download_queue.lock().await;
            queue.push_back(did);
        }

        pub async fn add_download_jobs(&self, dids: Vec<String>) {
            let mut queue = self.download_queue.lock().await;
            for did in dids {
                queue.push_back(did);
            }
        }

        pub async fn get_queue_stats(&self) -> (usize, usize, usize) {
            let download_queue = self.download_queue.lock().await;
            let processing_queue = self.processing_queue.lock().await;
            let results_queue = self.results_queue.lock().await;
            (download_queue.len(), processing_queue.len(), results_queue.len())
        }

        pub async fn pop_result(&self) -> Option<ProcessingResult> {
            let mut queue = self.results_queue.lock().await;
            queue.pop_front()
        }

        pub async fn start(&self, config: PipelineConfig) {
            info!("Starting processing pipeline with {} download workers, {} processing workers", 
                config.download_workers, config.processing_workers);

            let mut tasks = Vec::new();

            // Start download workers (IO-bound)
            for i in 0..config.download_workers {
                let pipeline = self.clone();
                let task = tokio::spawn(async move {
                    pipeline.download_worker(i).await;
                });
                tasks.push(task);
            }

            // Start processing workers (CPU-bound)
            for i in 0..config.processing_workers {
                let pipeline = self.clone();
                let task = tokio::spawn(async move {
                    pipeline.processing_worker(i).await;
                });
                tasks.push(task);
            }

            // Wait for all workers (they run indefinitely)
            futures::future::join_all(tasks).await;
        }

        async fn pop_download_job(&self) -> Option<String> {
            let mut queue = self.download_queue.lock().await;
            queue.pop_front()
        }

        async fn push_processing_job(&self, job: ProcessingJob) {
            let mut queue = self.processing_queue.lock().await;
            queue.push_back(job);
        }

        async fn pop_processing_job(&self) -> Option<ProcessingJob> {
            let mut queue = self.processing_queue.lock().await;
            queue.pop_front()
        }

        async fn push_result(&self, result: ProcessingResult) {
            let mut queue = self.results_queue.lock().await;
            queue.push_back(result);
        }

        async fn download_worker(&self, worker_id: usize) {
            info!("Download worker {} started", worker_id);

            loop {
                if let Some(did) = self.pop_download_job().await {
                    // TODO: Rate limiting per worker/PDS
                    // self.rate_limiter.until_ready().await;

                    match self.download_repo(&did).await {
                        Ok((repo_bytes, source_pds)) => {
                            let job = ProcessingJob { 
                                did: did.clone(), 
                                repo_bytes, 
                                source_pds 
                            };
                            self.push_processing_job(job).await;
                            info!("Worker {} downloaded repo for {}", worker_id, did);
                        }
                        Err(e) => {
                            error!("Worker {} download failed for {}: {}", worker_id, did, e);
                        }
                    }
                } else {
                    // No work available, sleep briefly
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        async fn processing_worker(&self, worker_id: usize) {
            info!("Processing worker {} started", worker_id);

            loop {
                if let Some(job) = self.pop_processing_job().await {
                    let start = std::time::Instant::now();
                    
                    match process_repo_bytes(job.repo_bytes.clone()).await {
                        Ok(records) => {
                            let stats = RepositoryStats::from(&job.repo_bytes, records.clone());
                            let records_len = records.len();
                            let result = ProcessingResult {
                                did: job.did.clone(),
                                records,
                                stats,
                            };
                            
                            self.push_result(result).await;
                            
                            let duration = start.elapsed();
                            info!("Worker {} processed repo for {} in {:?} ({} records)", 
                                worker_id, job.did, duration, records_len);
                        }
                        Err(e) => {
                            error!("Worker {} processing failed for {}: {}", worker_id, job.did, e);
                        }
                    }
                } else {
                    // No work available, sleep briefly
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        async fn download_repo(&self, did: &str) -> Result<(Vec<u8>, String)> {
            let did_obj = Did::new(did.to_string())
                .map_err(|e| anyhow::anyhow!("Invalid DID {}: {}", did, e))?;

            // Resolve DID to PDS endpoint
            let pds_url = if let Some(resolved_pds) = self.did_resolver.resolve_did_to_pds(did).await {
                self.pds_manager.add_endpoint(resolved_pds.clone()).await?;
                resolved_pds
            } else {
                return Err(anyhow::anyhow!("PDS endpoint not available for DID: {}", did));
            };

            let pds_endpoint = self
                .pds_manager
                .get_endpoint(&pds_url)
                .await
                .ok_or_else(|| anyhow::anyhow!("PDS endpoint not available: {}", pds_url))?;

            // Use the existing download logic from RepoDownloader
            use atrium_api::com::atproto::sync::get_repo;
            let get_repo_params_data = get_repo::ParametersData {
                did: did_obj,
                since: None,
            };

            let repo_data = pds_endpoint
                .agent
                .api
                .com
                .atproto
                .sync
                .get_repo(get_repo_params_data.into())
                .await
                .map_err(|e| {
                    anyhow::anyhow!("getRepo request to {} for {}: {}", pds_url, did, e)
                })?;

            let repo_bytes: Vec<u8> = repo_data.to_vec();
            Ok((repo_bytes, pds_url))
        }
    }

    impl Clone for ProcessingPipeline {
        fn clone(&self) -> Self {
            Self {
                download_queue: Arc::clone(&self.download_queue),
                processing_queue: Arc::clone(&self.processing_queue),
                results_queue: Arc::clone(&self.results_queue),
                pds_manager: Arc::clone(&self.pds_manager),
                did_resolver: Arc::clone(&self.did_resolver),
                client: Arc::clone(&self.client),
            }
        }
    }
}

mod coordinator {
    use anyhow::Result;
    use std::{sync::Arc, time::Duration};
    use tokio::time::{sleep, interval};
    use tracing::{info, warn};

    use crate::{
        pds::PdsManager,
        did::DidResolver,
        crawler::AccountCrawler,
        pipeline::{ProcessingPipeline, PipelineConfig},
    };

    pub struct SkylightCoordinator {
        pds_manager: Arc<PdsManager>,
        did_resolver: Arc<DidResolver>,
        crawler: AccountCrawler,
        pipeline: ProcessingPipeline,
        client: Arc<reqwest::Client>,
    }

    pub struct CoordinatorConfig {
        pub pipeline_config: PipelineConfig,
        pub crawler_interval_secs: u64,
        pub stats_interval_secs: u64,
    }

    impl Default for CoordinatorConfig {
        fn default() -> Self {
            Self {
                pipeline_config: PipelineConfig::default(),
                crawler_interval_secs: 3600, // Crawl every hour
                stats_interval_secs: 30,     // Stats every 30 seconds
            }
        }
    }

    impl SkylightCoordinator {
        pub async fn new() -> Result<Self> {
            let pds_manager = Arc::new(PdsManager::new());
            let did_resolver = Arc::new(DidResolver::new());
            let client = Arc::new(reqwest::Client::new());

            // Load existing PDS endpoints from hosts.txt
            pds_manager.load_hosts().await?;

            // Add default PDS if no hosts loaded
            if pds_manager.get_endpoint_count().await == 0 {
                info!("No existing PDS hosts found, adding default endpoints");
                pds_manager.add_endpoint("https://bsky.social".to_string()).await?;
                // TODO: Add more known PDS endpoints
                // pds_manager.add_endpoint("https://atproto.com".to_string()).await?;
            }

            let crawler = AccountCrawler::new(
                Arc::clone(&pds_manager),
                Arc::clone(&did_resolver),
            );

            let pipeline = ProcessingPipeline::new(
                Arc::clone(&pds_manager),
                Arc::clone(&did_resolver),
                Arc::clone(&client),
            );

            Ok(Self {
                pds_manager,
                did_resolver,
                crawler,
                pipeline,
                client,
            })
        }

        pub async fn run(&self, config: CoordinatorConfig) -> Result<()> {
            info!("Starting Skylight coordinator");
            info!("PDS endpoints loaded: {}", self.pds_manager.get_endpoint_count().await);

            // Start the processing pipeline
            let pipeline_task = {
                let pipeline = self.pipeline.clone();
                let pipeline_config = config.pipeline_config.clone();
                tokio::spawn(async move {
                    pipeline.start(pipeline_config).await;
                })
            };

            // Start periodic crawler
            let crawler_task = {
                let coordinator = self.clone();
                let crawler_interval = config.crawler_interval_secs;
                tokio::spawn(async move {
                    coordinator.run_crawler_loop(crawler_interval).await;
                })
            };

            // Start stats reporting
            let stats_task = {
                let coordinator = self.clone();
                let stats_interval = config.stats_interval_secs;
                tokio::spawn(async move {
                    coordinator.run_stats_loop(stats_interval).await;
                })
            };

            // Start DID feeding loop
            let feeder_task = {
                let coordinator = self.clone();
                tokio::spawn(async move {
                    coordinator.run_did_feeder_loop().await;
                })
            };

            // Start result consumption loop
            let consumer_task = {
                let coordinator = self.clone();
                tokio::spawn(async move {
                    coordinator.run_result_consumer_loop().await;
                })
            };

            // Wait for all tasks
            tokio::select! {
                _ = pipeline_task => warn!("Pipeline task exited"),
                _ = crawler_task => warn!("Crawler task exited"),
                _ = stats_task => warn!("Stats task exited"),
                _ = feeder_task => warn!("Feeder task exited"),
                _ = consumer_task => warn!("Consumer task exited"),
            }

            Ok(())
        }

        async fn run_crawler_loop(&self, interval_secs: u64) {
            let mut interval = interval(Duration::from_secs(interval_secs));
            
            loop {
                interval.tick().await;
                info!("Starting periodic account discovery crawl");
                
                if let Err(e) = self.crawler.crawl_all_pdss().await {
                    warn!("Crawler error: {}", e);
                } else {
                    // Save newly discovered PDS endpoints
                    if let Err(e) = self.pds_manager.save_hosts().await {
                        warn!("Failed to save hosts: {}", e);
                    }
                }
                
                let discovered_count = self.crawler.get_discovered_count().await;
                info!("Crawl completed, {} DIDs discovered", discovered_count);
            }
        }

        async fn run_stats_loop(&self, interval_secs: u64) {
            let mut interval = interval(Duration::from_secs(interval_secs));
            
            loop {
                interval.tick().await;
                
                let discovered_count = self.crawler.get_discovered_count().await;
                let (download_queue, processing_queue, results_queue) = self.pipeline.get_queue_stats().await;
                let pds_count = self.pds_manager.get_endpoint_count().await;
                
                info!("📊 Stats - PDS: {}, Discovered: {}, Queues - Download: {}, Processing: {}, Results: {}", 
                    pds_count, discovered_count, download_queue, processing_queue, results_queue);
            }
        }

        async fn run_did_feeder_loop(&self) {
            loop {
                // Feed discovered DIDs to the processing pipeline
                let mut fed_count = 0;
                while let Some(did) = self.crawler.pop_discovered_account().await {
                    self.pipeline.add_download_job(did).await;
                    fed_count += 1;
                    
                    // Batch feed to avoid holding locks too long
                    if fed_count >= 100 {
                        break;
                    }
                }
                
                if fed_count > 0 {
                    info!("Fed {} DIDs to processing pipeline", fed_count);
                }
                
                sleep(Duration::from_millis(500)).await;
            }
        }

        async fn run_result_consumer_loop(&self) {
            loop {
                if let Some(result) = self.pipeline.pop_result().await {
                    // TODO: Process/store results appropriately
                    info!("🎉 Processed repo for {} - {} records, {} bytes", 
                        result.did, 
                        result.records.len(), 
                        result.stats.raw_repo_size_bytes
                    );
                    
                    // TODO: Extract and queue new DIDs found in records
                    // TODO: Store records in database
                    // TODO: Update metrics/analytics
                } else {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    impl Clone for SkylightCoordinator {
        fn clone(&self) -> Self {
            Self {
                pds_manager: Arc::clone(&self.pds_manager),
                did_resolver: Arc::clone(&self.did_resolver),
                crawler: self.crawler.clone(),
                pipeline: self.pipeline.clone(),
                client: Arc::clone(&self.client),
            }
        }
    }
}

use coordinator::{SkylightCoordinator, CoordinatorConfig};
use pipeline::PipelineConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    utils::init_tracing_subscriber();
    
    tracing::info!("🚀 Starting Skylight - Decentralized Social Media Analytics");

    // Create and configure the coordinator
    let coordinator = SkylightCoordinator::new().await?;
    
    let config = CoordinatorConfig {
        pipeline_config: PipelineConfig {
            download_workers: 10,     // Reduce for demo
            processing_workers: 2,    // Reduce for demo
        },
        crawler_interval_secs: 300,   // Crawl every 5 minutes for demo
        stats_interval_secs: 10,      // Stats every 10 seconds for demo
    };

    // Run the coordinator
    coordinator.run(config).await?;

    Ok(())
}