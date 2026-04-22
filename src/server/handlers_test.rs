use futures::future;
use tars::api::primitives::Response as ApiResponse;
use tars::orderbook::primitives::MatchedOrderVerbose;
use std::time::Instant;
use tokio::time::Duration;
use tracing::{Level, info};

const API_URL: &str = "http://localhost:4596";
const TEST_CHAIN: &str = "solana_testnet";
const PARALLEL_REQUESTS: usize = 1;

async fn get_pending_orders(
    chain_identifier: &str,
) -> Result<(Vec<MatchedOrderVerbose>, Duration), reqwest::Error> {
    // Create a client with keepalive configured
    let client = reqwest::Client::builder()
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .pool_max_idle_per_host(10)
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let url = format!("{}/{}", API_URL, chain_identifier);

    let start: Instant = Instant::now();

    // Add keepalive header explicitly
    let response = client
        .get(&url)
        .header("Connection", "keep-alive")
        .send()
        .await?;

    // Deserialize directly to the expected type instead of going through Value
    let api_response = response
        .json::<ApiResponse<Vec<MatchedOrderVerbose>>>()
        .await?;
    let duration = start.elapsed();

    // Extract orders directly
    let orders = api_response.result.unwrap_or_default();

    Ok((orders, duration))
}

#[tokio::test]
async fn test_parallel_pending_orders() {
    // initialize the tracing subscriber
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    // Create 10 parallel futures
    let futures = (0..PARALLEL_REQUESTS)
        .map(|i| async move {
            info!("Starting request {}", i);
            match get_pending_orders(TEST_CHAIN).await {
                Ok((orders, duration)) => {
                    info!(
                        "Request {} completed in {} ms with {} orders",
                        i,
                        duration.as_millis(),
                        orders.len()
                    );
                    Some(duration)
                }
                Err(e) => {
                    eprintln!("Request {} failed: {}", i, e);
                    None
                }
            }
        })
        .collect::<Vec<_>>();

    // Execute all futures in parallel
    let results = future::join_all(futures).await;

    // Calculate average duration
    let successful_requests: Vec<Duration> = results.into_iter().filter_map(|r| r).collect();

    let avg_millis = if successful_requests.is_empty() {
        0.0
    } else {
        let total_millis: u128 = successful_requests.iter().map(|d| d.as_millis()).sum();

        total_millis as f64 / successful_requests.len() as f64
    };

    println!("============================================");
    println!(
        "Completed {} successful requests out of {}",
        successful_requests.len(),
        PARALLEL_REQUESTS
    );
    println!("Average response time: {:.2} ms", avg_millis);
    println!("============================================");

    assert!(
        !successful_requests.is_empty(),
        "No successful requests completed"
    );
}
