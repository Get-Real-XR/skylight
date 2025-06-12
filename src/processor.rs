use anyhow::Result;
use atrium_api::{
    app::bsky::{
        actor::profile,
        feed::{generator, like, post, postgate, repost, threadgate},
        graph::{block, follow, list, listblock, listitem, starterpack, verification},
        labeler::service,
    },
    chat::bsky::actor::declaration,
    com::atproto::{identity::resolve_handle, lexicon::schema, sync::get_repo},
    types::string::{Did, Handle},
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

    let repo = Repository::open(cs, root_cid).await?;
    let mut repo = repo;
    let mut mst = repo.tree();
    let mut all_records: Vec<FullRecord> = Vec::new();

    let entries: Vec<(String, atrium_repo::Cid)> = {
        let mut entries = pin!(mst.entries());
        let mut collected = Vec::new();
        while let Some(entry_result) = entries.try_next().await? {
            println!("Processing entry: {:?}", entry_result);
            collected.push(entry_result);
        }
        collected
    };

    // Process all entries directly
    for (key, cid) in entries {
        if let Some(collection) = key.split('/').next() {
            let collection = collection.to_string();

            if let Some(record_data) = decode_record(&mut repo, &collection, cid).await {
   
                all_records.push(FullRecord {
                    collection,
                    key,
                    record_data,
                });
            } else {
                println!("Failed to decode record: {key}");
            }
        }
    }

    Ok(all_records)
}



async fn decode_record(
    repo: &mut Repository<CarStore<BufReader<&mut Cursor<Vec<u8>>>>>,
    collection: &str,
    cid: atrium_repo::Cid,
) -> Option<RecordData> {
    macro_rules! try_decode {
        ($record_type:ty) => {
            repo.get_raw_cid::<$record_type>(cid).await.ok().flatten().map(RecordData::from)
        };
    }

    match collection {
        "app.bsky.feed.post" => try_decode!(post::Record),
        "app.bsky.actor.profile" => try_decode!(profile::Record),
        "app.bsky.feed.like" => try_decode!(like::Record),
        "app.bsky.feed.repost" => try_decode!(repost::Record),
        "app.bsky.graph.follow" => try_decode!(follow::Record),
        "app.bsky.feed.generator" => try_decode!(generator::Record),
        "app.bsky.feed.postgate" => try_decode!(postgate::Record),
        "app.bsky.feed.threadgate" => try_decode!(threadgate::Record),
        "app.bsky.graph.block" => try_decode!(block::Record),
        "app.bsky.graph.list" => try_decode!(list::Record),
        "app.bsky.graph.listblock" => try_decode!(listblock::Record),
        "app.bsky.graph.listitem" => try_decode!(listitem::Record),
        "app.bsky.graph.starterpack" => try_decode!(starterpack::Record),
        "app.bsky.graph.verification" => try_decode!(verification::Record),
        "chat.bsky.actor.declaration" => try_decode!(declaration::Record),
        "com.atproto.lexicon.schema" => try_decode!(schema::Record),
        "app.bsky.labeler.service" => try_decode!(service::Record),
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
                    "https://bsky.social".to_string()
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

        // Rate limiting
        pds_endpoint.rate_limiter.until_ready().await;

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
