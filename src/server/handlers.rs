use std::{collections::HashSet, sync::Arc};

use axum::extract::{Path, Query, State};
use tars::{
    api::primitives::{ApiResult, Response},
    orderbook::primitives::MatchedOrderVerbose,
};
use moka::future::Cache;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Clone)]
pub struct HandlerState {
    pub orders_cache: Arc<Cache<String, HashMap<String, Vec<MatchedOrderVerbose>>>>,
}

#[derive(Deserialize)]
pub struct SolverQuery {
    pub solver: Option<String>,
}

/// Health check endpoint that returns the service status
///
/// # Returns
/// * A static string indicating the service is online
pub async fn get_health() -> &'static str {
    "Online"
}

/// Returns the pending orders for a specific chain identifier.
///
/// # Arguments
/// * `chain_identifier` - The identifier of the chain to retrieve pending orders for.
/// * `solver` - Optional solver ID to filter orders by specific solver.
///
/// # Returns
/// * `ApiResult<Vec<MatchedOrderVerbose>>` - On success, a vector of pending orders for the specified chain identifier. On error, a string describing the error.
pub async fn get_pending_orders_by_chain(
    State(state): State<Arc<HandlerState>>,
    Path(chain_identifier): Path<String>,
    Query(query): Query<SolverQuery>,
) -> ApiResult<Vec<MatchedOrderVerbose>> {
    // Get solver orders map from cache
    let solver_orders_map = match state.orders_cache.get(&chain_identifier).await {
        Some(map) => map,
        None => {
            // No entry in cache, log and return empty result
            warn!(
                chain = %chain_identifier,
                "no cache entry found"
            );
            return Ok(Response::ok(Vec::new()));
        }
    };

    let pending_orders = if let Some(solver_id) = &query.solver {
        // Return orders for specific solver
        solver_orders_map.get(&solver_id.to_lowercase()).cloned().unwrap_or_default()
    } else {
        // Return orders for all solvers
        let mut all_orders = Vec::new();
        for orders in solver_orders_map.values() {
            all_orders.extend(orders.iter().cloned());
        }
        all_orders
    };

    info!(
        chain = %chain_identifier,
        solver = ?query.solver,
        orders_count = pending_orders.len(),
    );

    Ok(Response::ok(pending_orders))
}

/// Returns all pending orders for all chains
///
/// # Arguments
/// * `solver` - Optional solver ID to filter orders by specific solver.
///
/// # Returns
/// * `ApiResult<Vec<MatchedOrderVerbose>>` - On success, a vector of pending orders of all chains. On error, a string describing the error.
pub async fn get_all_pending_orders(
    State(state): State<Arc<HandlerState>>,
    Query(query): Query<SolverQuery>,
) -> ApiResult<Vec<MatchedOrderVerbose>> {
    // Collect all orders from all chains and all solvers
    let mut all_pending_orders = Vec::new();
    let mut seen_ids = HashSet::new();
    
    for entry in state.orders_cache.iter() {
        let chain_identifier = entry.0.as_str();
        if let Some(solver_orders_map) = state.orders_cache.get(chain_identifier).await {
            if let Some(solver_id) = &query.solver {
                // Filter by specific solver
                if let Some(orders) = solver_orders_map.get(&solver_id.to_lowercase()) {
                    for order in orders {
                        if seen_ids.insert(order.create_order.create_id.clone()) {
                            all_pending_orders.push(order.clone());
                        }
                    }
                }
            } else {
                // Include all solvers
                for orders in solver_orders_map.values() {
                    for order in orders {
                        if seen_ids.insert(order.create_order.create_id.clone()) {
                            all_pending_orders.push(order.clone());
                        }
                    }
                }
            }
        }
    }
    
    if all_pending_orders.is_empty() {
        // Fast path for empty results - avoid unnecessary processing
        return Ok(Response::ok(Vec::new()));
    }
    
    info!(
        solver = ?query.solver,
        orders_count = all_pending_orders.len(), 
        "collected all pending orders"
    );
    Ok(Response::ok(all_pending_orders))
}
#[cfg(test)]
mod tests {
    use crate::server::Server;

    use super::*;
    use moka::future::Cache;
    use std::sync::{Arc, Once};

    const API_URL: &str = "http://localhost:4596";
    static INIT: Once = Once::new();

    async fn get_pending_orders_by_chain(
        chain_identifier: &str,
    ) -> Result<Vec<MatchedOrderVerbose>, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let url = format!("{}/{}", API_URL, chain_identifier);

        // Simple retry with exponential backoff for network issues
        let mut retry_delay = 100; // Start at 100ms
        for attempt in 1..=3 {
            match client.get(&url).send().await {
                Ok(response) => {
                    let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
                    return Ok(api_response.result.unwrap_or_default());
                }
                Err(e) if e.is_connect() || e.is_timeout() && attempt < 3 => {
                    // Sleep with exponential backoff for connection issues
                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay)).await;
                    retry_delay *= 2; // Exponential backoff
                }
                Err(e) => return Err(e),
            }
        }

        // Final attempt
        let response = client.get(url).send().await?;
        let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
        Ok(api_response.result.unwrap_or_default())
    }
    
    async fn get_all_pending_orders() -> Result<Vec<MatchedOrderVerbose>, reqwest::Error> {
        let url = format!("{}/", API_URL);
        let client = reqwest::Client::new();

        // Simple retry with exponential backoff for network issues
        let mut retry_delay = 100; // Start at 100ms
        for attempt in 1..=3 {
            match client.get(&url).send().await {
                Ok(response) => {
                    let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
                    return Ok(api_response.result.unwrap_or_default());
                }
                Err(e) if e.is_connect() || e.is_timeout() && attempt < 3 => {
                    // Sleep with exponential backoff for connection issues
                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay)).await;
                    retry_delay *= 2; // Exponential backoff
                }
                Err(e) => return Err(e),
            }
        }

        // Final attempt
        let response = client.get(url).send().await?;
        let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
        Ok(api_response.result.unwrap_or_default())
    }

    async fn setup_server() {
        let cache = Arc::new(Cache::builder().build());
        let server = Server::new(4596, cache);
        INIT.call_once(|| {
            tokio::spawn(async move {
                server.run().await;
            });
        });
    }

    #[tokio::test]
    async fn test_pending_orders_handler() {
        setup_server().await;
        let pending_orders = get_pending_orders_by_chain("ethereum_localnet").await;
        dbg!(&pending_orders);
        assert!(pending_orders.is_ok());
    }
    
    #[tokio::test]
    async fn test_all_pending_orders_handler() {
        setup_server().await;
        let pending_orders = get_all_pending_orders().await;
        dbg!(&pending_orders);
        assert!(pending_orders.is_ok());
    }
}
