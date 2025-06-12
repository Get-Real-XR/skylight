use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DidDocument {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: String,
    #[serde(rename = "alsoKnownAs")]
    pub also_known_as: Option<Vec<String>>,
    pub service: Option<Vec<DidService>>,
    #[serde(rename = "verificationMethod")]
    pub verification_method: Option<Vec<VerificationMethod>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DidService {
    pub id: String,
    #[serde(rename = "type")]
    pub service_type: String,
    #[serde(rename = "serviceEndpoint")]
    pub service_endpoint: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VerificationMethod {
    pub id: String,
    #[serde(rename = "type")]
    pub verification_type: String,
    pub controller: String,
    #[serde(rename = "publicKeyMultibase")]
    pub public_key_multibase: Option<String>,
}

pub async fn resolve(client: &reqwest::Client, did: &str) -> Result<DidDocument> {
    let url = match did {
        did if did.starts_with("did:plc:") => format!("https://plc.directory/{}", did),
        did if did.starts_with("did:web:") => {
            let domain = did.strip_prefix("did:web:").unwrap();
            format!("https://{}/.well-known/did.json", domain)
        }
        _ => return Err(anyhow::anyhow!("Unsupported DID method: {}", did)),
    };

    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await?;

    if !response.status().is_success() {
        if response.status() == 429 {
            todo!("If you are seeing this, it means did resolution needs rate limiting.");
        }
        return Err(anyhow::anyhow!(
            "Failed to resolve DID: HTTP {}",
            response.status()
        ));
    }

    Ok(response.json().await?)
}

pub fn get_service_endpoint(did_doc: &DidDocument, service_type: &str) -> Option<String> {
    did_doc
        .service
        .as_ref()?
        .iter()
        .find(|service| service.service_type == service_type)
        .map(|service| service.service_endpoint.clone())
}

pub async fn resolve_pds_endpoint(client: &reqwest::Client, did: &str) -> Option<String> {
    let did_doc = resolve(client, did).await.ok()?;
    get_service_endpoint(&did_doc, "AtprotoPersonalDataServer")
}

pub struct DidResolver {
    cache: Arc<Mutex<HashMap<String, String>>>,
    client: reqwest::Client,
}

impl DidResolver {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            client: reqwest::Client::new(),
        }
    }

    pub async fn resolve_did_to_pds(&self, did: &str) -> Option<String> {
        {
            let cache = self.cache.lock().await;
            if let Some(pds_url) = cache.get(did) {
                return Some(pds_url.clone());
            }
        }

        if let Some(pds_endpoint) = resolve_pds_endpoint(&self.client, did).await {
            let mut cache = self.cache.lock().await;
            cache.insert(did.to_string(), pds_endpoint.clone());
            return Some(pds_endpoint);
        }

        None
    }

    /// Directly insert a DID -> PDS URL mapping into the cache.
    pub async fn insert(&self, did: &str, pds_url: &str) {
        let mut cache = self.cache.lock().await;
        cache.insert(did.to_string(), pds_url.to_string());
    }
}
