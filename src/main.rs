pub mod game;
pub mod player;
pub mod purse;
pub mod game_manager;
pub mod utils;
pub mod router;
pub mod ws;
pub mod evaluator;

use axum::Router;
use router::{create_router, AppState};
use std::net::SocketAddr;
use tracing::{info, error};
use tracing_subscriber;

use game_manager::GameManager;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting auction server...");

    // Create game manager
    let (mut game_manager, game_manager_tx) = GameManager::new();

    // Spawn game manager task
    tokio::spawn(async move {
        game_manager.run().await;
    });

    // Create application state
    let app_state = AppState {
        game_manager_tx,
    };

    // Create API router
    let api_router = create_router(app_state);

    // Create WebSocket router
    let ws_router = Router::new()
        .route("/ws/:room_id/:user_id", axum::routing::get(ws::ws_handler));

    // Combine routers
    let app = Router::new()
        .merge(api_router)
        .merge(ws_router);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| {
            error!("Server error: {}", e);
            std::process::exit(1);
        });
}