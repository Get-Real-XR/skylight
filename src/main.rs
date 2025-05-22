use anyhow::Result;
use atrium_api::{
    agent::atp_agent::{AtpAgent, store::MemorySessionStore},
    app::bsky::{
        actor::profile,
        feed::{like, post, repost},
        graph::follow,
    },
    com::atproto::sync::{get_repo, list_repos},
};
use atrium_xrpc_client::reqwest::ReqwestClient;
use governor::{Quota, RateLimiter};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{BufWriter, Write},
    num::NonZeroU32,
    sync::Arc,
};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::Mutex;

use atrium_repo::{Repository, blockstore::CarStore};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RepositoryInfo {
    did: String,
    record_counts: HashMap<String, usize>,
    sample_records: Vec<SampleRecord>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SampleRecord {
    collection: String,
    key: String,
    content: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let agent = AtpAgent::new(
        ReqwestClient::new("https://bsky.social"),
        MemorySessionStore::default(),
    );

    let bsky_username = env::var("BSKY_USERNAME")?;
    let bsky_password = env::var("BSKY_PASSWORD")?;

    let _session = agent.login(bsky_username, bsky_password).await?;

    let did = agent.did().await.unwrap();
    println!("User DID: {:?}", did);

    // Create rate limiter - 10 requests per second
    let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(
        NonZeroU32::new(10).unwrap(),
    )));

    // Start crawling with listRepos
    crawl_repositories(&agent, rate_limiter).await?;

    Ok(())
}

async fn crawl_repositories(
    agent: &AtpAgent<MemorySessionStore, ReqwestClient>,
    rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
) -> Result<()> {
    let mut cursor: Option<String> = None;
    let mut total_repos_processed = 0;
    let all_repo_info = Arc::new(Mutex::new(Vec::new()));
    
    // Concurrent processing settings
    const CONCURRENT_REPOS: usize = 5; // Process up to 5 repos concurrently

    println!("Starting repository crawl...");
    println!("Rate limit: 10 requests per second");
    println!("Concurrent processing: {} repositories", CONCURRENT_REPOS);

    loop {
        println!("\nFetching repos batch (cursor: {:?})...", cursor);

        let list_repo_params_data = list_repos::ParametersData {
            cursor: cursor.clone(),
            limit: Some(atrium_api::types::LimitedNonZeroU16::try_from(50).unwrap()),
        };

        // Wait for rate limiter before making request
        rate_limiter.until_ready().await;

        let repos_response = agent
            .api
            .com
            .atproto
            .sync
            .list_repos(list_repo_params_data.into())
            .await?;

        let repos_count = repos_response.repos.len();
        println!("Found {} repositories in this batch", repos_count);

        if repos_count == 0 {
            println!("No more repositories to process");
            break;
        }

        // Process repositories concurrently
        let mut futures = FuturesUnordered::new();
        
        for (idx, repo_info) in repos_response.repos.iter().enumerate() {
            let did = repo_info.did.clone();
            let rate_limiter_clone = rate_limiter.clone();
            let repo_idx = idx + 1;
            let did_str = did.as_str().to_string();
            
            // Create concurrent tasks without tokio::spawn to avoid lifetime issues
            let future = async move {
                println!(
                    "[{}/{}] Processing repository: {}",
                    repo_idx,
                    repos_count,
                    did_str
                );
                
                process_repository(agent, &did, rate_limiter_clone).await
            };
            
            futures.push(future);
            
            // Limit concurrent tasks
            if futures.len() >= CONCURRENT_REPOS {
                // Wait for at least one to complete before adding more
                if let Some(result) = futures.next().await {
                    match result {
                        Ok(info) => {
                            all_repo_info.lock().await.push(info);
                            total_repos_processed += 1;
                        }
                        Err(e) => {
                            eprintln!("Error processing repository: {}", e);
                        }
                    }
                }
            }
        }
        
        // Process remaining futures
        while let Some(result) = futures.next().await {
            match result {
                Ok(info) => {
                    all_repo_info.lock().await.push(info);
                    total_repos_processed += 1;
                    
                    // Save progress periodically
                    if total_repos_processed % 10 == 0 {
                        let repo_info = all_repo_info.lock().await;
                        save_progress_async(&repo_info, total_repos_processed).await?;
                    }
                }
                Err(e) => {
                    eprintln!("Error processing repository: {}", e);
                }
            }
        }

        // Update cursor for next batch
        cursor = repos_response.cursor.clone();

        if cursor.is_none() {
            println!("\nReached end of repository list");
            break;
        }
    }

    // Final save
    let repo_info = all_repo_info.lock().await;
    save_progress_async(&repo_info, total_repos_processed).await?;

    println!("\n=== Crawl Complete ===");
    println!("Total repositories processed: {}", total_repos_processed);

    Ok(())
}

async fn process_repository(
    agent: &AtpAgent<MemorySessionStore, ReqwestClient>,
    did: &atrium_api::types::string::Did,
    rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
) -> Result<RepositoryInfo> {
    let get_repo_params_data = get_repo::ParametersData {
        did: did.clone(),
        since: None,
    };

    // Wait for rate limiter before making request
    rate_limiter.until_ready().await;

    let repo_data = agent
        .api
        .com
        .atproto
        .sync
        .get_repo(get_repo_params_data.into())
        .await?;

    // Process repo data - convert to Vec<u8> to avoid lifetime issues
    let repo_bytes: Vec<u8> = repo_data.to_vec();

    // Offload CPU-intensive processing to blocking thread pool
    let result = tokio::task::spawn_blocking(move || -> Result<(HashMap<String, usize>, Vec<SampleRecord>)> {
        // Block here to perform synchronous operations
        let rt = tokio::runtime::Handle::current();
        
        rt.block_on(async move {
            let mut cursor = std::io::Cursor::new(repo_bytes);
            let reader = tokio::io::BufReader::new(&mut cursor);
            let cs = CarStore::open(reader).await?;
            let root_cid = cs.roots().next().unwrap();

            let repo = Repository::open(cs, root_cid).await?;
            use futures::TryStreamExt;
            let mut repo = repo;
            let mut mst = repo.tree();
            let mut record_counts: HashMap<String, usize> = HashMap::new();
            let mut sample_records: Vec<SampleRecord> = Vec::new();

            // First, collect all entries
            let entries: Vec<(String, atrium_repo::Cid)> = {
                let mut entries = Box::pin(mst.entries());
                let mut collected = Vec::new();
                while let Some(entry_result) = entries.try_next().await? {
                    collected.push(entry_result);
                }
                collected
            };

            // Now process the collected entries
            for (key, cid) in entries {
        // Extract collection name from the key path (format: collection/rkey)
        if let Some(collection) = key.split('/').next() {
            *record_counts.entry(collection.to_string()).or_insert(0) += 1;

            // Collect sample records (limit samples per type)
            let samples_for_type = sample_records
                .iter()
                .filter(|s| s.collection == collection)
                .count();

            if samples_for_type < 2 {
                let content = match collection {
                    "app.bsky.feed.post" => {
                        if let Ok(Some(record)) = repo.get_raw_cid::<post::Record>(cid).await {
                            format!(
                                "Post: {}",
                                record.text.chars().take(100).collect::<String>()
                            )
                        } else {
                            "Failed to decode post".to_string()
                        }
                    }
                    "app.bsky.actor.profile" => {
                        if let Ok(Some(record)) = repo.get_raw_cid::<profile::Record>(cid).await {
                            format!(
                                "Profile: {} - {}",
                                record.display_name.as_deref().unwrap_or("No name"),
                                record
                                    .description
                                    .as_deref()
                                    .unwrap_or("No bio")
                                    .chars()
                                    .take(50)
                                    .collect::<String>()
                            )
                        } else {
                            "Failed to decode profile".to_string()
                        }
                    }
                    "app.bsky.feed.like" => {
                        if let Ok(Some(record)) = repo.get_raw_cid::<like::Record>(cid).await {
                            format!("Like created at: {:?}", record.created_at)
                        } else {
                            "Failed to decode like".to_string()
                        }
                    }
                    "app.bsky.feed.repost" => {
                        if let Ok(Some(record)) = repo.get_raw_cid::<repost::Record>(cid).await {
                            format!("Repost created at: {:?}", record.created_at)
                        } else {
                            "Failed to decode repost".to_string()
                        }
                    }
                    "app.bsky.graph.follow" => {
                        if let Ok(Some(record)) = repo.get_raw_cid::<follow::Record>(cid).await {
                            format!(
                                "Follow: {:?} (created {:?})",
                                record.subject, record.created_at
                            )
                        } else {
                            "Failed to decode follow".to_string()
                        }
                    }
                    _ => format!("Unknown record type"),
                };

                sample_records.push(SampleRecord {
                    collection: collection.to_string(),
                    key: key.clone(),
                    content,
                });
            }
        }
    }

            Ok((record_counts, sample_records))
        })
    }).await??;
    
    let (record_counts, sample_records) = result;
    
    // Print summary for this repo
    println!("  Record counts: {:?}", record_counts);

    Ok(RepositoryInfo {
        did: did.as_str().to_string(),
        record_counts,
        sample_records,
    })
}

fn save_progress(repo_info: &[RepositoryInfo], total_count: usize) -> Result<()> {
    println!("\nSaving progress ({} repositories)...", total_count);

    let file = File::create("crawl_progress.json")?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, repo_info)?;
    writer.flush()?;

    println!("Progress saved to crawl_progress.json");
    Ok(())
}

async fn save_progress_async(repo_info: &[RepositoryInfo], total_count: usize) -> Result<()> {
    let repo_info = repo_info.to_vec();
    
    // Offload the I/O operation to a blocking thread
    tokio::task::spawn_blocking(move || {
        save_progress(&repo_info, total_count)
    }).await??;
    
    Ok(())
}
