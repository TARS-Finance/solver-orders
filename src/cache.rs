use crate::PendingOrdersCache;
use futures::stream::{FuturesUnordered, StreamExt};
use tars::orderbook::{OrderbookProvider, primitives::MatchedOrderVerbose, traits::Orderbook};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

// Maximum number of orders to cache per solver per chain
const MAX_SOLVER_ORDERS_PER_CHAIN: usize = 1000;

pub struct CacheSyncer {
    orderbook: Arc<OrderbookProvider>,
    polling_interval: u64,
    cache: Arc<PendingOrdersCache>,
    last_sync: Arc<AtomicI64>,
    max_backoff_ms: u64,
    error_count: std::sync::atomic::AtomicUsize,
}

impl CacheSyncer {
    pub fn new(
        orderbook: Arc<OrderbookProvider>,
        polling_interval: u64,
        cache: Arc<PendingOrdersCache>,
        last_sync: Arc<AtomicI64>,
    ) -> Self {
        Self {
            orderbook,
            polling_interval,
            cache,
            last_sync,
            max_backoff_ms: 5000,
            error_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[inline]
    fn calculate_backoff(&self) -> Duration {
        let error_count = self.error_count.load(Ordering::Relaxed) as u64;
        let base_ms = self.polling_interval;

        let backoff_ms = std::cmp::min(
            base_ms * (1 << std::cmp::min(error_count, 6)),
            self.max_backoff_ms,
        );

        let jitter = fastrand::f64() * 0.2 - 0.1;
        let backoff_with_jitter = ((backoff_ms as f64) * (1.0 + jitter)) as u64;

        Duration::from_millis(backoff_with_jitter)
    }

    pub async fn run(&self) {
        tracing::info!("cache syncer: starting to fetch pending orders");
        loop {
            let start = Instant::now();
            let pending_orders = match self.orderbook.get_solver_pending_orders().await {
                Ok(pending_orders) => {
                    self.error_count.store(0, Ordering::Relaxed);
                    pending_orders
                }
                Err(e) => {
                    self.error_count.fetch_add(1, Ordering::Relaxed);
                    let backoff = self.calculate_backoff();

                    tracing::error!(
                        error = %e,
                        backoff = %backoff.as_secs(),
                        "failed to get pending orders",
                    );

                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };

            let pending_orders_len = pending_orders.len();
            tracing::info!(orders_count = pending_orders_len, "fetched pending orders");

            self.process_orders(pending_orders).await;

            // Record successful sync timestamp so /health can detect staleness.
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            self.last_sync.store(now_ms, Ordering::Relaxed);

            let elapsed = start.elapsed();
            let sleep_duration = if elapsed < Duration::from_millis(self.polling_interval) {
                Duration::from_millis(self.polling_interval) - elapsed
            } else {
                Duration::from_millis(10)
            };
            tokio::time::sleep(sleep_duration).await;
        }
    }

    /// Rebuilds the cache to match `orders`, diff-style: inserts/overwrites new
    /// chain keys and removes chains that no longer have pending orders.
    ///
    /// Why: the previous `invalidate_all` + re-insert approach created a window
    /// where readers saw an empty cache on every poll.
    async fn process_orders(&self, orders: Vec<MatchedOrderVerbose>) {
        let mut chain_solver_orders_map: HashMap<
            String,
            HashMap<String, Vec<MatchedOrderVerbose>>,
        > = HashMap::new();

        for order in orders {
            let source_chain = &order.create_order.source_chain;
            let dest_chain = &order.create_order.destination_chain;

            let source_solver_id = order.source_swap.redeemer.to_lowercase();
            let dest_solver_id = order.destination_swap.initiator.to_lowercase();

            chain_solver_orders_map
                .entry(source_chain.clone())
                .or_default()
                .entry(source_solver_id)
                .or_default()
                .push(order.clone());

            if source_chain != dest_chain {
                chain_solver_orders_map
                    .entry(dest_chain.clone())
                    .or_default()
                    .entry(dest_solver_id)
                    .or_default()
                    .push(order);
            }
        }

        let new_keys: HashSet<String> = chain_solver_orders_map.keys().cloned().collect();

        // Existing keys in cache before this sync.
        let existing_keys: HashSet<String> = self
            .cache
            .iter()
            .map(|(k, _)| (*k).clone())
            .collect();

        let mut tasks = FuturesUnordered::new();

        for (chain, solver_orders_map) in chain_solver_orders_map {
            let cache = Arc::clone(&self.cache);
            let mut limited_solver_orders = HashMap::new();
            for (solver_id, mut orders) in solver_orders_map {
                orders.truncate(MAX_SOLVER_ORDERS_PER_CHAIN);
                limited_solver_orders.insert(solver_id, orders);
            }
            tasks.push(tokio::spawn(async move {
                cache.insert(chain, limited_solver_orders).await;
            }));
        }

        // Remove keys that no longer have pending orders.
        for stale_key in existing_keys.difference(&new_keys) {
            let cache = Arc::clone(&self.cache);
            let key = stale_key.clone();
            tasks.push(tokio::spawn(async move {
                cache.invalidate(&key).await;
            }));
        }

        while let Some(_) = tasks.next().await {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tars::orderbook::test_utils::{
        TestMatchedOrderConfig, create_test_matched_order, simulate_test_swap_initiate,
    };
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    const DB_URL: &str = "postgres://postgres:postgres@localhost:5433/unipay";
    const POLLING_INTERVAL: u64 = 100;

    async fn setup_cache_syncer() -> Arc<PendingOrdersCache> {
        let orderbook = Arc::new(OrderbookProvider::from_db_url(DB_URL).await.unwrap());
        let cache = Arc::new(PendingOrdersCache::builder().build());
        let last_sync = Arc::new(AtomicI64::new(0));
        let watcher = CacheSyncer::new(orderbook, POLLING_INTERVAL, Arc::clone(&cache), last_sync);
        tokio::spawn(async move {
            watcher.run().await;
        });
        Arc::clone(&cache)
    }

    pub async fn get_pool() -> sqlx::postgres::PgPool {
        PgPoolOptions::new()
            .max_connections(10)
            .connect(DB_URL)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_cache_syncer() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let cache = setup_cache_syncer().await;
        let pool = get_pool().await;
        let filler = "bcd6f4cfa96358c74dbc03fec5ba25da66bbc92a31b714ce339dd93db1a9ffac";

        let order_config = TestMatchedOrderConfig {
            destination_chain_initiator_address: filler.to_string(),
            ..Default::default()
        };

        let order_1 = create_test_matched_order(&pool, order_config.clone())
            .await
            .unwrap();
        let order_2 = create_test_matched_order(&pool, order_config.clone())
            .await
            .unwrap();

        simulate_test_swap_initiate(&pool, &order_1.source_swap.swap_id, None)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(POLLING_INTERVAL * 2)).await;

        let cache_data = cache.get(&order_1.create_order.destination_chain).await;
        assert!(cache_data.is_some());
        assert_eq!(cache_data.unwrap().len(), 1);

        simulate_test_swap_initiate(&pool, &order_2.source_swap.swap_id, None)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(POLLING_INTERVAL * 2)).await;

        let cache_data = cache.get(&order_2.create_order.destination_chain).await;
        assert!(cache_data.is_some());
        assert_eq!(cache_data.unwrap().len(), 2);
    }
}
