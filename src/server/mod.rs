use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use tars::orderbook::primitives::MatchedOrderVerbose;
use handlers::{HandlerState, get_all_pending_orders, get_health, get_pending_orders_by_chain};
use moka::future::Cache;
use reqwest::Method;
use std::collections::HashMap;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowHeaders, Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
pub mod handlers;
#[cfg(test)]
mod handlers_test;
use axum::{Router, routing::method_routing};
use tracing::info;

pub struct Server {
    pub port: i32,
    state: HandlerState,
}

impl Server {
    pub fn new(
        port: i32,
        orders_cache: Arc<Cache<String, HashMap<String, Vec<MatchedOrderVerbose>>>>,
        last_sync: Arc<AtomicI64>,
        polling_interval_ms: u64,
    ) -> Self {
        Self {
            port,
            state: HandlerState {
                orders_cache,
                last_sync,
                polling_interval_ms,
            },
        }
    }

    pub async fn run(&self) {
        // Setup CORS for maximum compatibility but minimal overhead
        let cors = CorsLayer::new()
            .allow_methods(vec![Method::GET, Method::POST])
            .allow_origin(Any)
            .allow_headers(AllowHeaders::any());

        // Create optimized router with middleware
        let app = Router::new()
            .route("/health", method_routing::get(get_health))
            .route("/", method_routing::get(get_all_pending_orders))
            .route(
                "/:chain_identifier",
                method_routing::get(get_pending_orders_by_chain),
            )
            .with_state(Arc::new(self.state.clone()))
            // Add aggressive compression for faster responses
            .layer(CompressionLayer::new().gzip(true).br(true))
            // Reduce request timeout to fail faster on slow requests
            .layer(TimeoutLayer::new(Duration::from_secs(5)))
            // Add concurrency limit to prevent resource exhaustion
            .layer(ConcurrencyLimitLayer::new(100))
            // Add CORS support
            .layer(cors)
            .layer(RequestBodyLimitLayer::new(4 * 1024 * 1024))
            // Add trace layer for performance monitoring
            .layer(TraceLayer::new_for_http());

        // Bind to all interfaces for better compatibility
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port as u16));

        // Configure TCP options for performance
        let socket = tokio::net::TcpSocket::new_v4().unwrap();

        // Configure socket for performance
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = socket.as_raw_fd();

            // Set TCP_NODELAY (disable Nagle's algorithm)
            unsafe {
                let optval: libc::c_int = 1;
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&optval) as libc::socklen_t,
                );
            }
        }

        // Bind and set reuse
        socket.set_reuseaddr(true).unwrap();
        socket.bind(addr).unwrap();

        let listener = socket.listen(1024).unwrap();

        info!("Listening on http://{}", addr);

        // Use Axum's optimized serve function
        axum::serve(listener, app).await.unwrap();
    }
}
