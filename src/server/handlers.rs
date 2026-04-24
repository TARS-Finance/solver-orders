use std::{
    collections::HashSet,
    sync::{Arc, atomic::{AtomicI64, Ordering}},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
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
    pub last_sync: Arc<AtomicI64>,
    pub polling_interval_ms: u64,
}

#[derive(Deserialize)]
pub struct SolverQuery {
    pub solver: Option<String>,
}

/// Returns 200 "Online" when the cache syncer has produced a successful poll
/// within 3 polling intervals; otherwise 503. This is what turns a dead syncer
/// into a loud failure instead of a silently empty cache.
pub async fn get_health(State(state): State<Arc<HandlerState>>) -> impl IntoResponse {
    let last = state.last_sync.load(Ordering::Relaxed);
    if last == 0 {
        return (StatusCode::SERVICE_UNAVAILABLE, "Starting").into_response();
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let staleness_ms = (now - last).max(0) as u64;
    let stale_threshold = state.polling_interval_ms.saturating_mul(3);
    if staleness_ms > stale_threshold {
        warn!(staleness_ms, stale_threshold, "cache is stale");
        return (StatusCode::SERVICE_UNAVAILABLE, "Stale").into_response();
    }
    (StatusCode::OK, "Online").into_response()
}

pub async fn get_pending_orders_by_chain(
    State(state): State<Arc<HandlerState>>,
    Path(chain_identifier): Path<String>,
    Query(query): Query<SolverQuery>,
) -> ApiResult<Vec<MatchedOrderVerbose>> {
    let solver_orders_map = match state.orders_cache.get(&chain_identifier).await {
        Some(map) => map,
        None => {
            warn!(chain = %chain_identifier, "no cache entry found");
            return Ok(Response::ok(Vec::new()));
        }
    };

    let pending_orders = if let Some(solver_id) = &query.solver {
        solver_orders_map.get(&solver_id.to_lowercase()).cloned().unwrap_or_default()
    } else {
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

pub async fn get_all_pending_orders(
    State(state): State<Arc<HandlerState>>,
    Query(query): Query<SolverQuery>,
) -> ApiResult<Vec<MatchedOrderVerbose>> {
    let mut all_pending_orders = Vec::new();
    let mut seen_ids = HashSet::new();

    for entry in state.orders_cache.iter() {
        let chain_identifier = entry.0.as_str();
        if let Some(solver_orders_map) = state.orders_cache.get(chain_identifier).await {
            if let Some(solver_id) = &query.solver {
                if let Some(orders) = solver_orders_map.get(&solver_id.to_lowercase()) {
                    for order in orders {
                        if seen_ids.insert(order.create_order.create_id.clone()) {
                            all_pending_orders.push(order.clone());
                        }
                    }
                }
            } else {
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

        let mut retry_delay = 100;
        for attempt in 1..=3 {
            match client.get(&url).send().await {
                Ok(response) => {
                    let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
                    return Ok(api_response.result.unwrap_or_default());
                }
                Err(e) if e.is_connect() || e.is_timeout() && attempt < 3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay)).await;
                    retry_delay *= 2;
                }
                Err(e) => return Err(e),
            }
        }

        let response = client.get(url).send().await?;
        let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
        Ok(api_response.result.unwrap_or_default())
    }

    async fn get_all_pending_orders() -> Result<Vec<MatchedOrderVerbose>, reqwest::Error> {
        let url = format!("{}/", API_URL);
        let client = reqwest::Client::new();

        let mut retry_delay = 100;
        for attempt in 1..=3 {
            match client.get(&url).send().await {
                Ok(response) => {
                    let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
                    return Ok(api_response.result.unwrap_or_default());
                }
                Err(e) if e.is_connect() || e.is_timeout() && attempt < 3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay)).await;
                    retry_delay *= 2;
                }
                Err(e) => return Err(e),
            }
        }

        let response = client.get(url).send().await?;
        let api_response: Response<Vec<MatchedOrderVerbose>> = response.json().await?;
        Ok(api_response.result.unwrap_or_default())
    }

    async fn setup_server() {
        let cache = Arc::new(Cache::builder().build());
        let last_sync = Arc::new(AtomicI64::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        ));
        let server = Server::new(4596, cache, last_sync, 2000);
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
