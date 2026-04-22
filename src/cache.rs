use crate::PendingOrdersCache;
use futures::stream::{FuturesUnordered, StreamExt};
use tars::orderbook::{OrderbookProvider, primitives::MatchedOrderVerbose, traits::Orderbook};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

// Maximum number of orders to cache per solver per chain
const MAX_SOLVER_ORDERS_PER_CHAIN: usize = 1000;

/// CacheSyncer is responsible for fetching pending orders from the orderbook and updating the cache.
pub struct CacheSyncer {
    orderbook: Arc<OrderbookProvider>,
    polling_interval: u64,
    cache: Arc<PendingOrdersCache>,
    max_backoff_ms: u64,
    error_count: std::sync::atomic::AtomicUsize,
}

impl CacheSyncer {
    /// Creates a new CacheSyncer instance.
    ///
    /// # Arguments
    /// * `orderbook` - The orderbook provider to use for fetching pending orders.
    /// * `polling_interval` - The interval between polling for pending orders.
    /// * `cache` - The cache to use for storing pending orders.
    ///
    /// # Returns
    /// * A new CacheSyncer instance.
    pub fn new(
        orderbook: Arc<OrderbookProvider>,
        polling_interval: u64,
        cache: Arc<PendingOrdersCache>,
    ) -> Self {
        Self {
            orderbook,
            polling_interval,
            cache,
            max_backoff_ms: 5000, // 5 second max backoff
            error_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Calculates the backoff duration based on the error count.
    ///
    /// # Arguments
    /// * `error_count` - The number of errors that have occurred.
    ///
    /// # Returns
    /// * The backoff duration in milliseconds.
    #[inline]
    fn calculate_backoff(&self) -> Duration {
        let error_count = self.error_count.load(std::sync::atomic::Ordering::Relaxed) as u64;
        let base_ms = self.polling_interval;

        // Calculate exponential backoff with a cap
        let backoff_ms = std::cmp::min(
            base_ms * (1 << std::cmp::min(error_count, 6)), // 2^6 = 64x max multiplier
            self.max_backoff_ms,
        );

        // Add jitter (±10%)
        let jitter = fastrand::f64() * 0.2 - 0.1; // -10% to +10%
        let backoff_with_jitter = ((backoff_ms as f64) * (1.0 + jitter)) as u64;

        Duration::from_millis(backoff_with_jitter)
    }

    /// Starts the cache syncer.
    pub async fn run(&self) {
        tracing::info!("cache syncer: starting to fetch pending orders");
        loop {
            let start = Instant::now();
            let pending_orders = match self.orderbook.get_solver_pending_orders().await {
                Ok(pending_orders) => {
                    // Reset error count on success
                    self.error_count
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                    pending_orders
                }
                Err(e) => {
                    // Increment error count for backoff calculation
                    self.error_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

            tracing::info!(orders_count = pending_orders_len, "fetched pending orders",);

            // clear cache
            self.cache.invalidate_all();
            // Process and update the cache
            self.process_orders(pending_orders).await;

            // Calculate time elapsed and determine sleep duration
            let elapsed = start.elapsed();
            let sleep_duration = if elapsed < Duration::from_millis(self.polling_interval) {
                Duration::from_millis(self.polling_interval) - elapsed
            } else {
                // If processing took longer than polling interval, sleep for minimal time
                Duration::from_millis(10)
            };
            tokio::time::sleep(sleep_duration).await;
        }
    }

    /// Processes the pending orders and updates the cache.
    ///
    /// # Arguments
    /// * `orders` - The pending orders to process.
    ///
    /// # Returns
    /// * `()`
    async fn process_orders(&self, orders: Vec<MatchedOrderVerbose>) {
        // Skip processing if empty
        if orders.is_empty() {
            return;
        }

        // Map of chain to solver to orders
        let mut chain_solver_orders_map: HashMap<
            String,
            HashMap<String, Vec<MatchedOrderVerbose>>,
        > = HashMap::new();

        for order in orders {
            let source_chain = &order.create_order.source_chain;
            let dest_chain = &order.create_order.destination_chain;

            let source_solver_id = order.source_swap.redeemer.to_lowercase();
            let dest_solver_id = order.destination_swap.initiator.to_lowercase();

            // Add to source chain with source solver_id
            chain_solver_orders_map
                .entry(source_chain.clone())
                .or_default()
                .entry(source_solver_id)
                .or_default()
                .push(order.clone());

            // Add to destination chain with dest solver_id if different from source
            if source_chain != dest_chain {
                chain_solver_orders_map
                    .entry(dest_chain.clone())
                    .or_default()
                    .entry(dest_solver_id)
                    .or_default()
                    .push(order);
            }
        }

        // Parallel insertion into cache
        let mut tasks = FuturesUnordered::new();

        for (chain, solver_orders_map) in chain_solver_orders_map {
            let cache = &self.cache;

            // Limit orders to max per solver per chain
            let mut limited_solver_orders = HashMap::new();

            for (solver_id, mut orders) in solver_orders_map {
                // Truncate orders for each solver to the max allowed per solver
                orders.truncate(MAX_SOLVER_ORDERS_PER_CHAIN);
                limited_solver_orders.insert(solver_id, orders);
            }

            tasks.push(async move {
                cache.insert(chain, limited_solver_orders).await;
            });
        }

        // Wait for all cache insertions to complete
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
    const DB_URL: &str = "postgres://postgres:postgres@localhost:5433/garden";
    const POLLING_INTERVAL: u64 = 100;

    // Setup cache syncer for testing
    async fn setup_cache_syncer() -> Arc<PendingOrdersCache> {
        let orderbook = Arc::new(OrderbookProvider::from_db_url(DB_URL).await.unwrap());
        let cache = Arc::new(PendingOrdersCache::builder().build());
        let watcher = CacheSyncer::new(orderbook, POLLING_INTERVAL, Arc::clone(&cache));
        tokio::spawn(async move {
            watcher.run().await;
        });
        Arc::clone(&cache)
    }

    // Setup database pool for testing
    pub async fn get_pool() -> sqlx::postgres::PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(DB_URL)
            .await
            .unwrap();
        pool
    }

    #[tokio::test]
    async fn test_cache_syncer() {
        // Initialize tracing for tests
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        tracing::info!("Setting up cache syncer");
        let cache = setup_cache_syncer().await;
        let pool = get_pool().await;
        let filler = "bcd6f4cfa96358c74dbc03fec5ba25da66bbc92a31b714ce339dd93db1a9ffac";

        let order_config = TestMatchedOrderConfig {
            destination_chain_initiator_address: filler.to_string(),
            ..Default::default()
        };

        tracing::info!("Creating test orders");
        let order_1 = create_test_matched_order(&pool, order_config.clone())
            .await
            .unwrap();
        tracing::info!(
            "Created order_1 with destination chain: {}",
            order_1.create_order.destination_chain
        );
        tracing::info!(
            "Created order_1 with source chain: {}",
            order_1.create_order.create_id
        );

        let order_2 = create_test_matched_order(&pool, order_config.clone())
            .await
            .unwrap();
        tracing::info!(
            "Created order_2 with destination chain: {}",
            order_2.create_order.destination_chain
        );

        tracing::info!("Simulating swap initiate for order_1");
        simulate_test_swap_initiate(&pool, &order_1.source_swap.swap_id, None)
            .await
            .unwrap();

        tracing::info!("Waiting for cache syncer to update (first interval)");
        tokio::time::sleep(Duration::from_millis(POLLING_INTERVAL * 2)).await;

        // Check if data exists in cache before unwrapping
        let cache_data = cache.get(&order_1.create_order.destination_chain).await;
        if let Some(data) = &cache_data {
            tracing::info!(
                "Cache has {} orders for chain {}",
                data.len(),
                order_1.create_order.destination_chain
            );
        } else {
            tracing::error!(
                "Cache has NO data for chain {}",
                order_1.create_order.destination_chain
            );
        }

        assert!(
            cache_data.is_some(),
            "Cache should contain data for destination chain"
        );
        assert_eq!(cache_data.unwrap().len(), 1);

        tracing::info!("Simulating swap initiate for order_2");
        simulate_test_swap_initiate(&pool, &order_2.source_swap.swap_id, None)
            .await
            .unwrap();

        tracing::info!("Waiting for cache syncer to update (second interval)");
        tokio::time::sleep(Duration::from_millis(POLLING_INTERVAL * 2)).await;

        // Check if data exists in cache before unwrapping
        let cache_data = cache.get(&order_2.create_order.destination_chain).await;
        if let Some(data) = &cache_data {
            tracing::info!(
                "Cache has {} orders for chain {}",
                data.len(),
                order_2.create_order.destination_chain
            );
        } else {
            tracing::error!(
                "Cache has NO data for chain {}",
                order_2.create_order.destination_chain
            );
        }

        assert!(
            cache_data.is_some(),
            "Cache should contain data for destination chain"
        );
        assert_eq!(cache_data.unwrap().len(), 2);

        tracing::info!("Cleaning up test data");
        // delete_matched_order(&pool, &order_1.create_order.create_id)
        //     .await
        //     .unwrap();
        // delete_matched_order(&pool, &order_2.create_order.create_id)
        //     .await
        //     .unwrap();
    }
}
