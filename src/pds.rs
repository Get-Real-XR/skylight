use anyhow::Result;
use atrium_api::agent::atp_agent::{store::MemorySessionStore, CredentialSession};
use atrium_api::agent::Agent;
use atrium_xrpc_client::reqwest::ReqwestClient;
use std::{collections::HashMap, num::NonZeroU32, sync::Arc};
use tokio::sync::Mutex;



#[derive(Clone)]
pub struct PdsEndpoint {
    pub url: String,
    pub agent: Arc<Agent<CredentialSession<MemorySessionStore, ReqwestClient>>>,
    pub rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
}

pub struct PdsManager {
    endpoints: Arc<Mutex<HashMap<String, PdsEndpoint>>>,
}

impl PdsManager {
    pub fn new() -> Self {
        Self {
            endpoints: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_endpoint(&self, endpoint_url: String) -> Result<()> {
        let mut endpoints = self.endpoints.lock().await;




        if !endpoints.contains_key(&endpoint_url) {
             let session = CredentialSession::new(
     ReqwestClient::new(&endpoint_url),
     MemorySessionStore::default(),
 );
 let agent = Agent::new(session);
            let agent = Arc::new(agent);

            // Per-PDS rate limiter (higher since global controls overall)
            let rate_limiter = Arc::new(governor::RateLimiter::direct(
                governor::Quota::per_second(NonZeroU32::new(10).unwrap()),
            ));

            let pds_endpoint = PdsEndpoint {
                url: endpoint_url.clone(),
                agent,
                rate_limiter,
            };

            endpoints.insert(endpoint_url.clone(), pds_endpoint);
        }

        Ok(())
    }

    pub async fn get_endpoint(&self, endpoint_url: &str) -> Option<PdsEndpoint> {
        let endpoints = self.endpoints.lock().await;
        endpoints.get(endpoint_url).cloned()
    }

    pub async fn get_endpoint_count(&self) -> usize {
        let endpoints = self.endpoints.lock().await;
        endpoints.len()
    }

    pub async fn get_all_endpoints(&self) -> Vec<PdsEndpoint> {
        let endpoints = self.endpoints.lock().await;
        endpoints.values().cloned().collect()
    }

    pub async fn save_hosts(&self) -> Result<()> {
        let endpoints = self.endpoints.lock().await;
        let hosts: Vec<String> = endpoints.keys().cloned().collect();
        let content = hosts.join("\n");
        tokio::fs::write("hosts.txt", content).await?;
        Ok(())
    }

    pub async fn load_hosts(&self) -> Result<()> {
        if let Ok(content) = tokio::fs::read_to_string("hosts.txt").await {
            for line in content.lines() {
                let host = line.trim();
                if !host.is_empty() && host.starts_with("https://") {
                    let _ = self.add_endpoint(host.to_string()).await;
                }
            }
        }
        Ok(())
    }

}
