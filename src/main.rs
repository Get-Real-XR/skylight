use std::env;
use anyhow::Result;
use atrium_api::{
    agent::atp_agent::{AtpAgent, store::MemorySessionStore},
    com::atproto::sync::list_repos,
};
use atrium_xrpc_client::reqwest::ReqwestClient;

#[tokio::main]
async fn main() -> Result<()> {
    let agent = AtpAgent::new(
        ReqwestClient::new("https://bsky.social"),
        MemorySessionStore::default(),
    );

    // Try to get BSKY_USERNAME
    let bsky_username = match env::var("BSKY_USERNAME") {
        Ok(username) => username,
        Err(e) => {
            eprintln!("Error getting BSKY_USERNAME: {}", e);
            return Err(anyhow::anyhow!("BSKY_USERNAME environment variable is required"));
        }
    };

    // Try to get BSKY_PASSWORD
    let bsky_password = match env::var("BSKY_PASSWORD") {
        Ok(password) => password,
        Err(e) => {
            eprintln!("Error getting BSKY_PASSWORD: {}", e);
            return Err(anyhow::anyhow!("BSKY_PASSWORD environment variable is required"));
        }
    };

    println!("Attempting to login with username: {}", bsky_username);
    let _session = agent.login(bsky_username, bsky_password).await?;

    let did = agent.did().await.unwrap();
    println!("User DID: {:?}", did);

    let list_repo_params_data = list_repos::ParametersData {
        cursor: None,
        limit: None,
    };
    let partial_repos_list = agent
        .api
        .com
        .atproto
        .sync
        .list_repos(list_repo_params_data.into())
        .await?;


    println!("Repos found in first request: {}", partial_repos_list.repos.len());

    Ok(())
}
