use atrium_api::{com::atproto::sync::list_repos, types::string::Did};
use futures::Stream;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{info, debug, warn, instrument};

use crate::{pds::{PdsEndpoint, PdsManager}, did::DidResolver};

#[derive(Debug, Clone)]
pub struct RepoItem {
    pub did: Did,
    pub source_pds: String,
}

pub struct RepoCrawler {
    pds_manager: Arc<PdsManager>,
    did_resolver: Arc<DidResolver>,
    pub discovered_counter: Arc<AtomicUsize>,
}

impl RepoCrawler {
    pub fn new(pds_manager: Arc<PdsManager>, did_resolver: Arc<DidResolver>) -> Self {
        Self {
            pds_manager,
            did_resolver,
            discovered_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_discovered_count(&self) -> usize {
        self.discovered_counter.load(Ordering::Relaxed)
    }

    #[instrument(skip(self))]
    pub fn create_repo_stream(&self) -> impl Stream<Item = RepoItem> {
        info!("Creating repository stream");
        let (repo_tx, repo_rx) = tokio::sync::mpsc::unbounded_channel::<RepoItem>();

        // Spawn discovery coordinator
        let pds_manager = self.pds_manager.clone();
        let did_resolver = self.did_resolver.clone();
        let discovered_counter = self.discovered_counter.clone();
        let repo_tx_clone = repo_tx.clone();

        tokio::spawn(async move {
            Self::discovery_coordinator(pds_manager, did_resolver, discovered_counter, repo_tx_clone)
                .await;
        });

        UnboundedReceiverStream::new(repo_rx)
    }

    #[instrument(skip_all)]
    async fn discovery_coordinator(
        pds_manager: Arc<PdsManager>,
        did_resolver: Arc<DidResolver>,
        discovered_counter: Arc<AtomicUsize>,
        repo_tx: tokio::sync::mpsc::UnboundedSender<RepoItem>,
    ) {
        info!("Starting discovery coordinator");
        let mut active_pds_tasks = HashSet::<String>::new();
        let discovered_dids = Arc::new(Mutex::new(HashSet::<String>::new()));

        loop {
            let endpoints = pds_manager.get_all_endpoints().await;
            debug!("Checking {} endpoints for new discoveries", endpoints.len());

            for endpoint in endpoints {
                let endpoint_url = endpoint.url.clone();

                if active_pds_tasks.contains(&endpoint_url) {
                    continue;
                }
                active_pds_tasks.insert(endpoint_url.clone());

                // Spawn discovery task for this PDS
                debug!("Starting discovery task for PDS: {}", endpoint_url);
                let repo_tx_clone = repo_tx.clone();
                let discovered_counter_clone = discovered_counter.clone();
                let discovered_dids_clone = discovered_dids.clone();
                let pds_manager_clone = pds_manager.clone();
                let did_resolver_clone = did_resolver.clone();
                tokio::spawn(async move {
                    Self::discover_from_pds(
                        endpoint,
                        repo_tx_clone,
                        discovered_counter_clone,
                        discovered_dids_clone,
                        pds_manager_clone,
                        did_resolver_clone,
                    )
                    .await;
                });
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    #[instrument(skip_all, fields(pds_url = %endpoint.url))]
    async fn discover_from_pds(
        endpoint: PdsEndpoint,
        repo_tx: tokio::sync::mpsc::UnboundedSender<RepoItem>,
        discovered_counter: Arc<AtomicUsize>,
        discovered_dids: Arc<Mutex<HashSet<String>>>,
        pds_manager: Arc<PdsManager>,
        did_resolver: Arc<DidResolver>,
    ) {
        let mut cursor: Option<String> = None;
        let endpoint_url = endpoint.url.clone();
        info!("Starting discovery from PDS");

        loop {
            let repos_response = match Self::fetch_repo_batch(&endpoint, cursor.clone()).await {
                Ok(response) if response.repos.is_empty() => {
                    debug!("No more repos found, ending discovery");
                    break;
                }
                Ok(response) => {
                    debug!("Fetched {} repos from batch", response.repos.len());
                    response
                }
                Err(e) => {
                    warn!("Failed to fetch repo batch: {}", e);
                    break;
                }
            };

            cursor = repos_response.cursor.clone();

            let new_repos = Self::collect_new_repos(&repos_response.repos, &discovered_dids).await;
            if new_repos.is_empty() {
                debug!("No new repos in this batch");
                continue;
            }

            debug!("Found {} new repositories", new_repos.len());

            // Cache new DID -> PDS mappings from this PDS
            for (repo_info, is_new) in &new_repos {
                if *is_new {
                    did_resolver.insert(repo_info.did.as_str(), &endpoint_url).await;
                }
            }

            let resolved_pds_results =
                Self::resolve_repos_concurrently(&new_repos, &did_resolver).await;

            let repo_items = Self::create_repo_items(
                &new_repos,
                &resolved_pds_results,
                &endpoint_url,
                &pds_manager,
            )
            .await;

            let new_dids_count = new_repos.iter().filter(|(_, is_new)| *is_new).count();

            if repo_items
                .into_iter()
                .filter_map(|repo_item| repo_tx.send(repo_item).ok())
                .count()
                == 0
            {
                warn!("Repository channel closed, stopping discovery");
                return; // Channel closed
            }

            discovered_counter.fetch_add(new_dids_count, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn fetch_repo_batch(
        endpoint: &PdsEndpoint,
        cursor: Option<String>,
    ) -> Result<
        atrium_api::com::atproto::sync::list_repos::Output,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        const BATCH_SIZE: usize = 50;
        let params = list_repos::ParametersData {
            cursor,
            limit: Some(atrium_api::types::LimitedNonZeroU16::try_from(BATCH_SIZE as u16).unwrap()),
        };

        endpoint.rate_limiter.until_ready().await;

        Ok(endpoint
            .agent
            .api
            .com
            .atproto
            .sync
            .list_repos(params.into())
            .await?)
    }

    async fn collect_new_repos(
        repos: &[atrium_api::com::atproto::sync::list_repos::Repo],
        discovered_dids: &Arc<Mutex<HashSet<String>>>,
    ) -> Vec<(atrium_api::com::atproto::sync::list_repos::Repo, bool)> {
        let mut dids = discovered_dids.lock().await;
        repos
            .iter()
            .map(|repo_info| {
                let did_str = repo_info.did.as_str().to_string();
                let is_new_did = dids.insert(did_str);
                (repo_info.clone(), is_new_did)
            })
            .collect()
    }

    async fn resolve_repos_concurrently(
        new_repos: &[(atrium_api::com::atproto::sync::list_repos::Repo, bool)],
        did_resolver: &Arc<DidResolver>,
    ) -> Vec<Option<String>> {
        let resolution_futures = new_repos
            .iter()
            .map(|(repo_info, _)| did_resolver.resolve_did_to_pds(repo_info.did.as_str()));

        futures::future::join_all(resolution_futures).await
    }

    async fn create_repo_items(
        new_repos: &[(atrium_api::com::atproto::sync::list_repos::Repo, bool)],
        resolved_pds_results: &[Option<String>],
        endpoint_url: &str,
        pds_manager: &Arc<PdsManager>,
    ) -> Vec<RepoItem> {
        let mut repo_items = Vec::new();

        for ((repo_info, _), resolved_pds) in new_repos.iter().zip(resolved_pds_results.iter()) {
            let actual_pds = match resolved_pds {
                Some(host_pds) => {
                    let _ = pds_manager.add_endpoint(host_pds.clone()).await;
                    host_pds.clone()
                }
                None => endpoint_url.to_string(),
            };

            let repo_item = RepoItem {
                did: repo_info.did.clone(),
                source_pds: actual_pds,
            };

            repo_items.push(repo_item);
        }

        repo_items
    }

    fn send_repo_items(
        repo_items: Vec<RepoItem>,
        repo_tx: &tokio::sync::mpsc::UnboundedSender<RepoItem>,
    ) -> usize {
        repo_items
            .into_iter()
            .filter_map(|repo_item| repo_tx.send(repo_item).ok())
            .count()
    }
}
