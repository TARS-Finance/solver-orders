use std::{collections::HashMap, sync::Arc, time::Duration};

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
            // Set time-to-live (TTL) - entry expires after this duration since creation
            .time_to_live(CACHE_TTL)
            // Set time-to-idle (TTI) - entry expires if not accessed for this duration
            .time_to_idle(CACHE_TTI)
            // Add direct initial capacity to avoid resizing
            .initial_capacity(100)
            // Reduce delay for expired entries removal to improve memory usage
            .build(),
    );

    // Setup cache syncer that will update the cache every polling interval
    let cache_syncer = cache::CacheSyncer::new(
        Arc::clone(&orderbook),
        settings.polling_interval,
        Arc::clone(&cache),
    );

    // Spawn cache syncer in a separate thread
    let _ = tokio::spawn(async move {
        cache_syncer.run().await;
    });

    // Setup server
    let server = server::Server::new(settings.port, Arc::clone(&cache));
    // Run server
    server.run().await;
}
