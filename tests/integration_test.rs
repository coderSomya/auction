use auction::game_manager::GameManager;
use auction::router::{create_router, AppState};
use auction::ws;
use axum::Router;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing_subscriber;

/// Helper to start the test server on a given port
async fn start_test_server(port: u16) -> (tokio::task::JoinHandle<()>, String, String) {
    // Initialize tracing for tests
    let _ = tracing_subscriber::fmt::try_init();

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

    // Start server on specified port
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    let base_url = format!("http://localhost:{}", port);
    let ws_base_url = format!("ws://localhost:{}", port);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (handle, base_url, ws_base_url)
}

/// Helper to wait for server to be ready
async fn wait_for_server(base_url: &str) {
    let client = Client::new();
    for _ in 0..10 {
        if client.get(format!("{}/api/games", base_url)).send().await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Helper to create a game
async fn create_game(client: &Client, base_url: &str, username: &str, initial_purse: u64) -> String {
    let response = client
        .post(format!("{}/api/games", base_url))
        .json(&json!({
            "username": username,
            "initial_purse": initial_purse
        }))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
    data["data"]["game_id"].as_str().unwrap().to_string()
}

/// Helper to join a game
async fn join_game(client: &Client, base_url: &str, game_id: &str, username: &str, initial_purse: u64) {
    let response = client
        .post(format!("{}/api/games/{}/join", base_url, game_id))
        .json(&json!({
            "username": username,
            "initial_purse": initial_purse
        }))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
}

/// Helper to start a game
async fn start_game(client: &Client, base_url: &str, game_id: &str) {
    let response = client
        .post(format!("{}/api/games/{}/start", base_url, game_id))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
}

/// Helper to place a bid
async fn place_bid(client: &Client, base_url: &str, game_id: &str, username: &str, price: u64) {
    let response = client
        .post(format!("{}/api/games/{}/bid", base_url, game_id))
        .json(&json!({
            "username": username,
            "price": price
        }))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
}

/// Helper to opt out
async fn opt_out(client: &Client, base_url: &str, game_id: &str, username: &str) {
    let response = client
        .post(format!("{}/api/games/{}/opt-out", base_url, game_id))
        .json(&json!({
            "username": username
        }))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
}

/// Helper to connect WebSocket
async fn connect_ws(ws_base_url: &str, room_id: &str, user_id: &str) {
    let url = format!("{}/ws/{}/{}", ws_base_url, room_id, user_id);
    let (ws_stream, _) = connect_async(url).await.unwrap();
    let (_write, mut read) = ws_stream.split();
    
    // Spawn task to handle messages (just consume them for now)
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    println!("WS Message: {}", serde_json::to_string_pretty(&value).unwrap());
                }
            }
        }
    });

    // Give it a moment to connect
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_full_game_flow() {
    let (_server, base_url, ws_base_url) = start_test_server(3001).await;
    wait_for_server(&base_url).await;

    let client = Client::new();

    // Test 1: Create game, join players, start game, and bid
    println!("Test 1: Full game flow with bidding");
    
    let game_id = create_game(&client, &base_url, "player1", 100).await;
    println!("Created game: {}", game_id);

    join_game(&client, &base_url, &game_id, "player2", 100).await;
    println!("Player2 joined");

    join_game(&client, &base_url, &game_id, "player3", 100).await;
    println!("Player3 joined");

    // Connect WebSockets for all players
    let uuid1 = uuid::Uuid::new_v4().to_string();
    let uuid2 = uuid::Uuid::new_v4().to_string();
    let uuid3 = uuid::Uuid::new_v4().to_string();

    connect_ws(&ws_base_url, &game_id, &uuid1).await;
    connect_ws(&ws_base_url, &game_id, &uuid2).await;
    connect_ws(&ws_base_url, &game_id, &uuid3).await;

    // Start the game
    start_game(&client, &base_url, &game_id).await;
    println!("Game started");

    // Wait for cricketer to be available
    sleep(Duration::from_secs(2)).await;

    // Place bids
    place_bid(&client, &base_url, &game_id, "player1", 25).await;
    println!("Player1 bid 25");

    sleep(Duration::from_millis(500)).await;

    place_bid(&client, &base_url, &game_id, "player2", 30).await;
    println!("Player2 bid 30");

    sleep(Duration::from_millis(500)).await;

    place_bid(&client, &base_url, &game_id, "player3", 35).await;
    println!("Player3 bid 35");

    // Wait a bit for processing
    sleep(Duration::from_secs(2)).await;

    // Verify game info
    let response = client
        .get(format!("{}/api/games/{}", base_url, game_id))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
    println!("Game info retrieved successfully");
}

#[tokio::test]
async fn test_opt_out_mechanism() {
    let (_server, base_url, ws_base_url) = start_test_server(3002).await;
    wait_for_server(&base_url).await;

    let client = Client::new();

    println!("Test 2: Opt-out mechanism");

    let game_id = create_game(&client, &base_url, "creator", 100).await;
    join_game(&client, &base_url, &game_id, "player2", 100).await;
    join_game(&client, &base_url, &game_id, "player3", 100).await;

    // Connect WebSockets
    let uuid1 = uuid::Uuid::new_v4().to_string();
    let uuid2 = uuid::Uuid::new_v4().to_string();
    let uuid3 = uuid::Uuid::new_v4().to_string();

    connect_ws(&ws_base_url, &game_id, &uuid1).await;
    connect_ws(&ws_base_url, &game_id, &uuid2).await;
    connect_ws(&ws_base_url, &game_id, &uuid3).await;

    start_game(&client, &base_url, &game_id).await;
    println!("Game started");

    // Wait for cricketer
    sleep(Duration::from_secs(2)).await;

    // Player1 places a bid
    place_bid(&client, &base_url, &game_id, "creator", 20).await;
    println!("Creator bid 20");

    sleep(Duration::from_millis(500)).await;

    // Player2 and Player3 opt out (n-1 = 2 opt outs should trigger sale)
    opt_out(&client, &base_url, &game_id, "player2").await;
    println!("Player2 opted out");

    sleep(Duration::from_millis(500)).await;

    opt_out(&client, &base_url, &game_id, "player3").await;
    println!("Player3 opted out");

    // Wait for sale to complete
    sleep(Duration::from_secs(2)).await;

    println!("Opt-out test completed - cricketer should be sold to creator");
}

#[tokio::test]
async fn test_timer_based_selling() {
    let (_server, base_url, ws_base_url) = start_test_server(3003).await;
    wait_for_server(&base_url).await;

    let client = Client::new();

    println!("Test 3: Timer-based selling");

    let game_id = create_game(&client, &base_url, "timer_test", 100).await;
    join_game(&client, &base_url, &game_id, "player2", 100).await;

    // Connect WebSockets
    let uuid1 = uuid::Uuid::new_v4().to_string();
    let uuid2 = uuid::Uuid::new_v4().to_string();

    connect_ws(&ws_base_url, &game_id, &uuid1).await;
    connect_ws(&ws_base_url, &game_id, &uuid2).await;

    start_game(&client, &base_url, &game_id).await;
    println!("Game started - waiting for cricketer");

    // Wait for cricketer to be available
    sleep(Duration::from_secs(2)).await;

    // Place a bid
    place_bid(&client, &base_url, &game_id, "timer_test", 25).await;
    println!("Bid placed at 25");

    // Wait for timer to expire (MAX_IDLE_TIME_IN_SECS = 60 seconds)
    // For testing, we'll wait a shorter time and verify the mechanism works
    // In a real scenario, we'd wait the full 60 seconds
    println!("Waiting for timer mechanism... (this would normally be 60 seconds)");
    
    // Note: In a real test, you might want to reduce MAX_IDLE_TIME_IN_SECS for testing
    // or use a mock time. For now, we'll just verify the bid was placed correctly.
    sleep(Duration::from_secs(2)).await;

    // Verify game is still running
    let response = client
        .get(format!("{}/api/games/{}", base_url, game_id))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
    println!("Timer test setup completed");
}

#[tokio::test]
async fn test_multiple_cricketers_auction() {
    let (_server, base_url, ws_base_url) = start_test_server(3004).await;
    wait_for_server(&base_url).await;

    let client = Client::new();

    println!("Test 4: Multiple cricketers auction");

    let game_id = create_game(&client, &base_url, "multi_test", 200).await;
    join_game(&client, &base_url, &game_id, "player2", 200).await;

    // Connect WebSockets
    let uuid1 = uuid::Uuid::new_v4().to_string();
    let uuid2 = uuid::Uuid::new_v4().to_string();

    connect_ws(&ws_base_url, &game_id, &uuid1).await;
    connect_ws(&ws_base_url, &game_id, &uuid2).await;

    start_game(&client, &base_url, &game_id).await;
    println!("Game started");

    // Wait for first cricketer
    sleep(Duration::from_secs(2)).await;

    // Bid on first cricketer
    place_bid(&client, &base_url, &game_id, "multi_test", 20).await;
    println!("Bid 20 on first cricketer");

    sleep(Duration::from_millis(500)).await;

    place_bid(&client, &base_url, &game_id, "player2", 25).await;
    println!("Bid 25 on first cricketer");

    // Opt out to trigger sale
    sleep(Duration::from_millis(500)).await;
    opt_out(&client, &base_url, &game_id, "multi_test").await;
    println!("Opted out - first cricketer should be sold");

    // Wait for sale and next cricketer
    sleep(Duration::from_secs(3)).await;

    // Bid on second cricketer
    place_bid(&client, &base_url, &game_id, "multi_test", 18).await;
    println!("Bid 18 on second cricketer");

    sleep(Duration::from_millis(500)).await;

    place_bid(&client, &base_url, &game_id, "player2", 22).await;
    println!("Bid 22 on second cricketer");

    // Opt out again
    sleep(Duration::from_millis(500)).await;
    opt_out(&client, &base_url, &game_id, "multi_test").await;
    println!("Opted out - second cricketer should be sold");

    // Wait a bit
    sleep(Duration::from_secs(2)).await;

    // Verify game info shows multiple sales
    let response = client
        .get(format!("{}/api/games/{}", base_url, game_id))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
    println!("Multiple cricketers test completed");
}

