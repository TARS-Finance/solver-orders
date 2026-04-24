use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};

use tars::{
    orderbook::{OrderbookProvider, primitives::MatchedOrderVerbose},
    utils::setup_tracing_with_webhook,
};
use moka::future::{Cache, CacheBuilder};
use vault_rs::ConfigType;

mod cache;
mod server;

// File name of the Cache server Settings.toml file
const SETTINGS_FILE_NAME: &str = "Settings.toml";
// Maximum number of entries in the cache
const MAX_CACHE_SIZE: u64 = 2000;
// Time to live for the cache
const CACHE_TTL: Duration = Duration::from_secs(60 * 60); // 1 hour
// Time to idle for the cache (removes entries not accessed within this time)
const CACHE_TTI: Duration = Duration::from_secs(30 * 60); // 30 minutes

type PendingOrdersCache = Cache<String, HashMap<String, Vec<MatchedOrderVerbose>>>;

#[derive(serde::Deserialize)]
struct Settings {
    // Orderbook database URL
    pub db_url: String,
    // Port number on which the server will listen
    pub port: i32,
    // Polling interval in milliseconds
    pub polling_interval: u64,
    // Discord webhook URL for logging errors
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discord_webhook_url: Option<String>,
}

#[tokio::main]
async fn main() {
    let settings: Settings = vault_rs::get_config(SETTINGS_FILE_NAME, ConfigType::Toml)
        .await
        .expect("error loading config from vault");

    // Setup tracing with webhook if discord webhook url is provided
    match settings.discord_webhook_url.clone() {
        Some(discord_webhook_url) => setup_tracing_with_webhook(
            &discord_webhook_url,
            "Solver Orders Cache",
            tracing::Level::ERROR,
            None,
        )
        .expect("Failed to setup tracing with webhook"),
        None => tracing_subscriber::fmt().pretty().init(),
    }

    // Setup orderbook provider
    let orderbook = Arc::new(
        OrderbookProvider::from_db_url(&settings.db_url)
            .await
            .expect("Failed to create orderbook provider"),
    );

    // Setup cache with optimized configuration
    let cache: Arc<PendingOrdersCache> = Arc::new(
        CacheBuilder::new(MAX_CACHE_SIZE)
            .time_to_live(CACHE_TTL)
            .time_to_idle(CACHE_TTI)
            .initial_capacity(100)
            .build(),
    );

    // Shared timestamp (unix millis) of the last successful sync, used by /health
    // to detect a dead syncer instead of silently serving an empty cache.
    let last_sync = Arc::new(AtomicI64::new(0));

    let cache_syncer = cache::CacheSyncer::new(
        Arc::clone(&orderbook),
        settings.polling_interval,
        Arc::clone(&cache),
        Arc::clone(&last_sync),
    );

    let syncer_handle = tokio::spawn(async move {
        cache_syncer.run().await;
    });

    let server = server::Server::new(
        settings.port,
        Arc::clone(&cache),
        Arc::clone(&last_sync),
        settings.polling_interval,
    );

    // If the syncer task ever exits (panic or otherwise), take the whole process
    // down so the external supervisor restarts us. Silent syncer death is the
    // bug we keep hitting in prod.
    tokio::select! {
        res = syncer_handle => {
            tracing::error!(?res, "cache syncer task exited unexpectedly; shutting down");
            std::process::exit(1);
        }
        _ = server.run() => {
            tracing::error!("http server exited; shutting down");
            std::process::exit(1);
        }
    }
}
