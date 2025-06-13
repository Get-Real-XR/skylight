use anyhow::Result;
use atrium_api::{
    app::bsky::{
        actor::{profile, Profile},
        feed::{generator, like, post, postgate, repost, threadgate, Generator, Like, Post, Postgate, Repost, Threadgate},
        graph::{block, follow, list, listblock, listitem, starterpack, verification, Block, Follow, List, Listblock, Listitem, Starterpack, Verification},
        labeler::{service, Service},
    },
    chat::bsky::actor::{declaration, Declaration},
    com::atproto::{identity::resolve_handle, lexicon::{schema, Schema}, sync::get_repo},
    types::string::{Did, Handle, RecordKey},
};
use atrium_repo::{Repository, blockstore::CarStore};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, io::Cursor, pin::pin, sync::Arc};
use tokio::io::BufReader;

use crate::{pds::PdsManager, did::DidResolver};

// TODO store all data appropriately

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecordData {
    Post(post::Record),
    Profile(profile::Record),
    Like(like::Record),
    Repost(repost::Record),
    Follow(follow::Record),
    Generator(generator::Record),
    Postgate(postgate::Record),
    Threadgate(threadgate::Record),
    Block(block::Record),
    List(list::Record),
    Listblock(listblock::Record),
    Listitem(listitem::Record),
    Starterpack(starterpack::Record),
    Verification(verification::Record),
    ChatDeclaration(declaration::Record),
    LexiconSchema(schema::Record),
    LabelerService(service::Record),
}

impl fmt::Display for RecordData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:#?}")
    }
}

macro_rules! impl_from_record {
    ($module:ident, $variant:ident) => {
        impl From<$module::Record> for RecordData {
            fn from(record: $module::Record) -> Self {
                Self::$variant(record)
            }
        }
    };
}

impl_from_record!(post, Post);
impl_from_record!(profile, Profile);
impl_from_record!(like, Like);
impl_from_record!(repost, Repost);
impl_from_record!(follow, Follow);
impl_from_record!(generator, Generator);
impl_from_record!(postgate, Postgate);
impl_from_record!(threadgate, Threadgate);
impl_from_record!(block, Block);
impl_from_record!(list, List);
impl_from_record!(listblock, Listblock);
impl_from_record!(listitem, Listitem);
impl_from_record!(starterpack, Starterpack);
impl_from_record!(verification, Verification);
impl_from_record!(declaration, ChatDeclaration);
impl_from_record!(schema, LexiconSchema);
impl_from_record!(service, LabelerService);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RepositoryStats {
    pub record_counts: HashMap<String, usize>,
    pub raw_repo_size_bytes: usize,
}

impl RepositoryStats {
    pub fn from(
        repo_bytes: &[u8],
        all_records: Vec<FullRecord>,
    ) -> Self {
        // Aggregate record counts by collection
        let mut record_counts: HashMap<String, usize> = HashMap::new();
        for record in &all_records {
            *record_counts.entry(record.collection.clone()).or_insert(0) += 1;
        }

        Self {
            record_counts,
            raw_repo_size_bytes: repo_bytes.len(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FullRecord {
    pub collection: String,
    pub key: String,
    pub record_data: RecordData,
}

pub struct RepoDownloader {
    pds_manager: Arc<PdsManager>,
    did_resolver: Arc<DidResolver>,
    client: Arc<reqwest::Client>,
}

/// Processes repository bytes locally to extract records (standalone function)
pub async fn process_repo_bytes(
    repo_bytes: Vec<u8>,
) -> Result<Vec<FullRecord>> {
    let mut cursor = Cursor::new(repo_bytes);
    let reader = BufReader::new(&mut cursor);
    let cs = CarStore::open(reader).await?;
    let root_cid = cs.roots().next().unwrap();

    let mut repo = Repository::open(cs, root_cid).await?;
    let mut all_records: Vec<FullRecord> = Vec::new();

    // Process records by collection to avoid large allocations
    let collections = [
        "app.bsky.feed.post",
        "app.bsky.actor.profile", 
        "app.bsky.feed.like",
        "app.bsky.feed.repost",
        "app.bsky.graph.follow",
        "app.bsky.feed.generator",
        "app.bsky.feed.postgate",
        "app.bsky.feed.threadgate",
        "app.bsky.graph.block",
        "app.bsky.graph.list",
        "app.bsky.graph.listblock",
        "app.bsky.graph.listitem",
        "app.bsky.graph.starterpack",
        "app.bsky.graph.verification",
        "chat.bsky.actor.declaration",
        "com.atproto.lexicon.schema",
        "app.bsky.labeler.service",
    ];

    for &collection in &collections {
        // First collect all keys for this collection
        let mut keys = Vec::new();
        {
            let mut mst = repo.tree();
            let mut entries = pin!(mst.entries_prefixed(collection));
            
            while let Some((key, _cid)) = entries.try_next().await? {
                keys.push(key);
            }
        }
        
        // Then process the records
        for key in keys {
            // Extract rkey from the full path (collection/rkey -> rkey)
            // Skip keys that don't match this exact collection (e.g., when searching for
            // "app.bsky.graph.list" we might get "app.bsky.graph.listblock" due to prefix matching)
            let record_key_str = match key.strip_prefix(&format!("{}/", collection)) {
                Some(rkey) => rkey,
                None => continue, // Skip this key if it doesn't match the exact collection
            };
            let record_key = RecordKey::new(record_key_str.to_string())
                .map_err(|e| anyhow::anyhow!("invalid record key: {} - {}", record_key_str, e))?;
            
            if let Some(record_data) = decode_record(&mut repo, collection, record_key).await {
                all_records.push(FullRecord {
                    collection: collection.to_string(),
                    key: record_key_str.to_string(), // Store just the rkey part
                    record_data,
                });
            } else {
                println!("Failed to decode record: {record_key_str}");
            }
        }
    }
    Ok(all_records)
}

async fn decode_record(
    repo: &mut Repository<CarStore<BufReader<&mut Cursor<Vec<u8>>>>>,
    collection: &str,
    key: RecordKey,
) -> Option<RecordData> {
    macro_rules! try_decode {
        ($record_type:ty) => {
            repo.get::<$record_type>(key.clone()).await.ok().flatten().map(RecordData::from)
        };
    }

    // No clue if this is exhaustive -- HM
    // TODO: look into better code-gen method fo this
    match collection {
        "app.bsky.feed.post" => try_decode!(Post),
        "app.bsky.actor.profile" => try_decode!(Profile),
        "app.bsky.feed.like" => try_decode!(Like),
        "app.bsky.feed.repost" => try_decode!(Repost),
        "app.bsky.graph.follow" => try_decode!(Follow),
        "app.bsky.feed.generator" => try_decode!(Generator),
        "app.bsky.feed.postgate" => try_decode!(Postgate),
        "app.bsky.feed.threadgate" => try_decode!(Threadgate),
        "app.bsky.graph.block" => try_decode!(Block),
        "app.bsky.graph.list" => try_decode!(List),
        "app.bsky.graph.listblock" => try_decode!(Listblock),
        "app.bsky.graph.listitem" => try_decode!(Listitem),
        "app.bsky.graph.starterpack" => try_decode!(Starterpack),
        "app.bsky.graph.verification" => try_decode!(Verification),
        "chat.bsky.actor.declaration" => try_decode!(Declaration),
        "com.atproto.lexicon.schema" => try_decode!(Schema),
        "app.bsky.labeler.service" => try_decode!(Service),
        _ => None,
    }
}

impl RepoDownloader {
    pub fn new(pds_manager: Arc<PdsManager>, did_resolver: Arc<DidResolver>, client: Arc<reqwest::Client>) -> Self {
        Self { pds_manager, did_resolver, client }
    }

    /// Downloads repository bytes from the network
    pub async fn download_repository(
        &self,
        did: &Did,
        source_pds: Option<String>,
    ) -> Result<Vec<u8>> {
        let did_str = did.as_str();


        // We need to resolve the DID to a PDS endpoint in order to not bottlenck on the entryway rate limits.
        let pds_url = match source_pds {
            Some(source) => source,
            None => {
                if let Some(resolved_pds) =
                    self.did_resolver.resolve_did_to_pds(did_str).await
                {
                    self.pds_manager.add_endpoint(resolved_pds.clone()).await?;
                    resolved_pds
                } else {
                    panic!("PDS endpoint not available for DID: {}", did_str);
                    // "https://bsky.social".to_string()
                }
            }
        };

        let pds_endpoint = self
            .pds_manager
            .get_endpoint(&pds_url)
            .await
            .ok_or_else(|| anyhow::anyhow!("PDS endpoint not available: {}", pds_url))?;

        let get_repo_params_data = get_repo::ParametersData {
            did: did.clone(),
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
                anyhow::anyhow!("getRepo request to {} for {}: {}", pds_url, did_str, e)
            })?;

        let repo_bytes: Vec<u8> = repo_data.to_vec();
        Ok(repo_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;





    #[tokio::test]
    async fn test_decode_repo() {
        crate::utils::init_tracing_subscriber();

        let agent = crate::utils::create_authenticated_agent("https://bsky.social")
            .await
            .expect("Failed to create authenticated agent").agent;

        let handle = "henrym.bsky.social";
        println!("Resolving handle: {}", handle);
        
        let resolve_params = resolve_handle::ParametersData {
            handle: Handle::new(handle.to_string()).expect("Failed to create Handle"),
        };
        
        let resolve_result = agent
            .api
            .com
            .atproto
            .identity
            .resolve_handle(resolve_params.into())
            .await
            .expect("Failed to resolve handle");
        
        let did = resolve_result.did.clone();
        println!("Resolved DID: {}", did.as_str());

        // Get repository directly using authenticated agent
        let get_repo_params_data = get_repo::ParametersData {
            did: did.clone(),
            since: None,
        };

        let repo_data = agent
            .api
            .com
            .atproto
            .sync
            .get_repo(get_repo_params_data.into())
            .await
            .map_err(|e| {
                anyhow::anyhow!("getRepo request for {}: {}", did.as_str(), e)
            })
            .expect("Failed to get repository");

        let repo_bytes: Vec<u8> = repo_data.to_vec();


        let start = std::time::Instant::now();

        let all_records = process_repo_bytes(repo_bytes.clone()).await.expect("Failed to process repo data");

        let duration = start.elapsed();
        println!("Time taken: {:?}", duration);

        let repo_info = RepositoryStats::from(&repo_bytes, all_records);

        // Test results
        println!("Successfully processed repository!");
        println!("Raw repo size: {} bytes", repo_info.raw_repo_size_bytes);
        println!("Total record types: {}", repo_info.record_counts.len());
        
        // Print record counts by collection
        println!("\nRecord counts by collection:");
        for (collection, count) in &repo_info.record_counts {
            println!("  {}: {}", collection, count);
        }

        // Print first few records for inspection
        // for (i, record) in repo_info.all_records.iter().enumerate() {
        //     println!("  {}: {} -> {}", i + 1, record.collection, record.record_data);
        // }

        // Assertions to ensure we got meaningful data
        assert!(repo_info.raw_repo_size_bytes > 0, "Repo size should be greater than 0");
        assert!(!repo_info.record_counts.is_empty(), "Should have at least one record type");

        // Check for expected record types (profile should exist)
        assert!(
            repo_info.record_counts.contains_key("app.bsky.actor.profile"),
            "Should have a profile record"
        );
    }
}
