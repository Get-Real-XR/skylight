use anyhow::Result;
use atrium_api::agent::{atp_agent::{store::MemorySessionStore, CredentialSession}, Agent};
use atrium_xrpc_client::reqwest::ReqwestClient;

use atrium_api::com::atproto::server::create_session;

pub struct AuthenticationResult {
    pub agent: Agent<CredentialSession<MemorySessionStore, ReqwestClient>>,
    pub create_session_result: create_session::Output,
}



/// Creates an authenticated Bluesky agent using credentials from environment variables
/// Expects BSKY_USERNAME and BSKY_PASSWORD environment variables to be set.
pub async fn create_authenticated_agent(uri: impl AsRef<str>) -> Result<AuthenticationResult> {
    // Get authentication credentials from environment variables
    let username = std::env::var("BSKY_USERNAME")
        .map_err(|_| anyhow::anyhow!("BSKY_USERNAME environment variable is required for authentication"))?;
    let password = std::env::var("BSKY_PASSWORD")
        .map_err(|_| anyhow::anyhow!("BSKY_PASSWORD environment variable is required for authentication"))?;

    println!("Authenticating with username: {}", username);

    let session = CredentialSession::new(
        ReqwestClient::new(uri.as_ref()),
        MemorySessionStore::default(),
    );

    let create_session_result = session
        .login(&username, &password)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to authenticate: {}", e))?;

    let agent = Agent::new(session);


    Ok(AuthenticationResult {
        agent,
        create_session_result,
    })
} 

pub fn init_tracing_subscriber() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
}