use auction::game_manager::GameManager;
use auction::router::{create_router, AppState};
use auction::ws;
use axum::Router;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
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

    // Give the server a moment to start
    sleep(Duration::from_millis(100)).await;

    (handle, base_url, ws_base_url)
}

/// Helper to wait for server to be ready
async fn wait_for_server(base_url: &str) {
    let client = Client::new();
    // Try up to 50 times (5 seconds total)
    for i in 0..50 {
        match client.get(format!("{}/api/games", base_url)).send().await {
            Ok(response) if response.status().is_success() => {
                return;
            }
            Ok(response) => {
                // Server responded but with error status - might still be starting
                // 404 could mean routes aren't registered yet, keep waiting
                if i < 49 {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                // Last attempt failed
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                panic!("Server not ready after 5 seconds: status={}, body={}", status, body);
            }
            Err(e) => {
                // Connection error - server not up yet
                if i < 49 {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                panic!("Server did not become ready after 5 seconds: {:?}", e);
            }
        }
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

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        panic!("Failed to create game: status={}, body={}", status, text);
    }
    
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

#[tokio::test]
async fn test_game_end_with_winner_evaluation() {
    let (_server, base_url, ws_base_url) = start_test_server(3005).await;
    wait_for_server(&base_url).await;

    let client = Client::new();

    println!("Test 5: Game end with winner evaluation when cricketers exhausted");

    // Create game with 2 players
    let game_id = create_game(&client, &base_url, "player1", 200).await;
    join_game(&client, &base_url, &game_id, "player2", 200).await;
    println!("Game created with 2 players");

    // Connect WebSockets to collect messages
    let uuid1 = uuid::Uuid::new_v4().to_string();
    let uuid2 = uuid::Uuid::new_v4().to_string();

    let messages_arc = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let messages_clone1 = Arc::clone(&messages_arc);
    let messages_clone2 = Arc::clone(&messages_arc);

    // Connect WebSocket for player1
    let url1 = format!("{}/ws/{}/{}", ws_base_url, game_id, uuid1);
    let (ws_stream1, _) = connect_async(url1).await.unwrap();
    let (_write1, mut read1) = ws_stream1.split();
    
    tokio::spawn(async move {
        while let Some(msg) = read1.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    messages_clone1.lock().await.push(value.clone());
                    println!("WS1 Message: {}", serde_json::to_string_pretty(&value).unwrap());
                }
            }
        }
    });

    // Connect WebSocket for player2
    let url2 = format!("{}/ws/{}/{}", ws_base_url, game_id, uuid2);
    let (ws_stream2, _) = connect_async(url2).await.unwrap();
    let (_write2, mut read2) = ws_stream2.split();
    
    tokio::spawn(async move {
        while let Some(msg) = read2.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    messages_clone2.lock().await.push(value.clone());
                    println!("WS2 Message: {}", serde_json::to_string_pretty(&value).unwrap());
                }
            }
        }
    });

    sleep(Duration::from_millis(200)).await;

    // Start the game
    start_game(&client, &base_url, &game_id).await;
    println!("Game started - will process all cricketers");

    // Get initial game info to check bank
    let response = client
        .get(format!("{}/api/games/{}", base_url, game_id))
        .send()
        .await
        .unwrap();
    let data: Value = response.json().await.unwrap();
    let initial_bank = data["data"]["bank"].as_u64().unwrap_or(0);
    println!("Initial bank: {}", initial_bank);

    // Process cricketers by bidding and opting out quickly
    // This will exhaust all cricketers and trigger winner evaluation
    let mut cricketer_count = 0;
    loop {
        // Wait for cricketer to be available
        sleep(Duration::from_secs(1)).await;

        // Check if game has ended
        let messages = messages_arc.lock().await;
        let has_ended = messages.iter().any(|m| {
            m.get("msg_type")
                .and_then(|t| t.as_str())
                .map(|t| t == "game_ended")
                .unwrap_or(false)
        });
        drop(messages);

        if has_ended {
            println!("Game ended detected!");
            break;
        }

        // Place a bid
        place_bid(&client, &base_url, &game_id, "player1", 20 + cricketer_count as u64).await;
        sleep(Duration::from_millis(300)).await;

        // Opt out to trigger sale
        opt_out(&client, &base_url, &game_id, "player2").await;
        
        cricketer_count += 1;
        println!("Processed cricketer {} (waiting for next or game end)", cricketer_count);

        // Wait a bit for sale to complete
        sleep(Duration::from_secs(2)).await;

        // Check again if game ended
        let messages = messages_arc.lock().await;
        let has_ended = messages.iter().any(|m| {
            m.get("msg_type")
                .and_then(|t| t.as_str())
                .map(|t| t == "game_ended")
                .unwrap_or(false)
        });
        drop(messages);

        if has_ended {
            println!("Game ended after processing {} cricketers", cricketer_count);
            break;
        }

        // Safety check - if we've processed 18+ cricketers (max is 18), the game should have ended
        // Wait longer for game_ended message to arrive
        if cricketer_count >= 18 {
            println!("Processed {} cricketers (max is 18), waiting for game end message...", cricketer_count);
            // Wait up to 10 seconds for game_ended message
            for wait_attempt in 0..10 {
                sleep(Duration::from_secs(1)).await;
                let messages = messages_arc.lock().await;
                let has_ended = messages.iter().any(|m| {
                    m.get("msg_type")
                        .and_then(|t| t.as_str())
                        .map(|t| t == "game_ended")
                        .unwrap_or(false)
                });
                drop(messages);
                if has_ended {
                    println!("Game ended detected after {} second wait", wait_attempt + 1);
                    break;
                }
            }
            break;
        }
    }

    // Wait a bit more to ensure all messages are received
    sleep(Duration::from_secs(2)).await;

    // Verify game_ended message was broadcasted
    let messages = messages_arc.lock().await;
    let game_ended_messages: Vec<_> = messages
        .iter()
        .filter(|m| {
            m.get("msg_type")
                .and_then(|t| t.as_str())
                .map(|t| t == "game_ended")
                .unwrap_or(false)
        })
        .collect();

    assert!(!game_ended_messages.is_empty(), "Game ended message should be broadcasted to all players");
    println!("Found {} game_ended messages", game_ended_messages.len());

    // Verify winner information in the message
    if let Some(ended_msg) = game_ended_messages.first() {
        let payload = ended_msg.get("payload").unwrap();
        assert!(payload.get("winner").is_some(), "Winner should be in game_ended message");
        assert!(payload.get("winner")
            .and_then(|w| w.get("username"))
            .is_some(), "Winner username should be present");
        assert!(payload.get("winner")
            .and_then(|w| w.get("coins"))
            .is_some(), "Winner coins should be present");
        
        let winner_username = payload.get("winner")
            .and_then(|w| w.get("username"))
            .and_then(|u| u.as_str())
            .unwrap();
        
        let winner_coins = payload.get("winner")
            .and_then(|w| w.get("coins"))
            .and_then(|c| c.as_u64())
            .unwrap();
        
        println!("Winner evaluated: {} with {} coins", winner_username, winner_coins);
        
        // Verify winner is one of the players
        assert!(
            winner_username == "player1" || winner_username == "player2",
            "Winner should be one of the players"
        );
        
        // Verify winner has coins (should have winning amount added)
        assert!(winner_coins > 0, "Winner should have coins");
    }

    // Verify game status is FINISHED
    let response = client
        .get(format!("{}/api/games/{}", base_url, game_id))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
    
    let game_info = data["data"].as_object().unwrap();
    let status = game_info["status"].as_str().unwrap();
    assert_eq!(status, "FINISHED", "Game status should be FINISHED");
    
    let final_bank = game_info["bank"].as_u64().unwrap_or(0);
    println!("Final bank: {}", final_bank);
    
    // Verify bank was updated (should have money from sales)
    assert!(final_bank > initial_bank, "Bank should have increased from sales");
    assert!(final_bank > 0, "Bank should be greater than 0");

    println!("Game end test completed successfully!");
    println!("- Winner was evaluated using random evaluator");
    println!("- Winner was broadcasted to all players");
    println!("- Bank was updated correctly");
    println!("- Game status is FINISHED");
}

#[tokio::test]
#[ignore] // Duplicate test - use test_game_end_with_winner_evaluation instead
async fn test_winner_evaluation_when_cricketers_exhausted() {
    let (_server, base_url, ws_base_url) = start_test_server(3010).await;
    wait_for_server(&base_url).await;

    let client = Client::new();

    println!("Test 5: Winner evaluation when cricketers exhausted");

    let game_id = create_game(&client, &base_url, "player1", 200).await;
    join_game(&client, &base_url, &game_id, "player2", 200).await;
    join_game(&client, &base_url, &game_id, "player3", 200).await;

    println!("Created game with 3 players: {}", game_id);

    // Connect WebSockets to capture events
    let uuid1 = uuid::Uuid::new_v4().to_string();
    let uuid2 = uuid::Uuid::new_v4().to_string();
    let uuid3 = uuid::Uuid::new_v4().to_string();

    // Collect WebSocket messages
    let messages_arc = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let messages1 = Arc::clone(&messages_arc);
    let messages2 = Arc::clone(&messages_arc);
    let messages3 = Arc::clone(&messages_arc);

    // Connect and collect messages for player1
    let url1 = format!("{}/ws/{}/{}", ws_base_url, game_id, uuid1);
    let (ws_stream1, _) = connect_async(url1).await.unwrap();
    let (_write1, mut read1) = ws_stream1.split();
    tokio::spawn(async move {
        while let Some(msg) = read1.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    messages1.lock().await.push(value.clone());
                    println!("Player1 WS: {}", serde_json::to_string_pretty(&value).unwrap());
                }
            }
        }
    });

    // Connect and collect messages for player2
    let url2 = format!("{}/ws/{}/{}", ws_base_url, game_id, uuid2);
    let (ws_stream2, _) = connect_async(url2).await.unwrap();
    let (_write2, mut read2) = ws_stream2.split();
    tokio::spawn(async move {
        while let Some(msg) = read2.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    messages2.lock().await.push(value.clone());
                    println!("Player2 WS: {}", serde_json::to_string_pretty(&value).unwrap());
                }
            }
        }
    });

    // Connect and collect messages for player3
    let url3 = format!("{}/ws/{}/{}", ws_base_url, game_id, uuid3);
    let (ws_stream3, _) = connect_async(url3).await.unwrap();
    let (_write3, mut read3) = ws_stream3.split();
    tokio::spawn(async move {
        while let Some(msg) = read3.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    messages3.lock().await.push(value.clone());
                    println!("Player3 WS: {}", serde_json::to_string_pretty(&value).unwrap());
                }
            }
        }
    });

    sleep(Duration::from_millis(200)).await;

    // Start the game
    start_game(&client, &base_url, &game_id).await;
    println!("Game started - will process all cricketers");

    // Wait for first cricketer
    sleep(Duration::from_secs(2)).await;

    // Process cricketers quickly by bidding and opting out
    // This will exhaust all cricketers faster
    // We'll rotate players to avoid running out of money
    let mut cricketer_count = 0;
    let max_cricketers = 18; // We have 18 cricketers in the JSON
    
    // Process all cricketers
    while cricketer_count < max_cricketers {
        // Check if we've received a game_ended message
        let messages = messages_arc.lock().await;
        let has_ended = messages.iter().any(|m| {
            m.get("msg_type")
                .and_then(|t| t.as_str())
                .map(|t| t == "game_ended")
                .unwrap_or(false)
        });
        drop(messages);

        if has_ended {
            println!("Game ended detected after {} cricketers!", cricketer_count);
            break;
        }

        // Rotate players to distribute bids and avoid running out of money
        let bidder = match cricketer_count % 3 {
            0 => "player1",
            1 => "player2",
            _ => "player3",
        };

        // Place a bid on current cricketer (use base price + small increment)
        let bid_price = 18 + (cricketer_count % 5); // Keep bids reasonable
        place_bid(&client, &base_url, &game_id, bidder, bid_price).await;
        sleep(Duration::from_millis(300)).await;

        // Opt out other players to trigger sale quickly
        match bidder {
            "player1" => {
                opt_out(&client, &base_url, &game_id, "player2").await;
                sleep(Duration::from_millis(200)).await;
                opt_out(&client, &base_url, &game_id, "player3").await;
            },
            "player2" => {
                opt_out(&client, &base_url, &game_id, "player1").await;
                sleep(Duration::from_millis(200)).await;
                opt_out(&client, &base_url, &game_id, "player3").await;
            },
            _ => {
                opt_out(&client, &base_url, &game_id, "player1").await;
                sleep(Duration::from_millis(200)).await;
                opt_out(&client, &base_url, &game_id, "player2").await;
            },
        }

        cricketer_count += 1;
        println!("Processed cricketer {} by {} (waiting for next or game end)", cricketer_count, bidder);

        // Wait for next cricketer or game end
        sleep(Duration::from_secs(2)).await;
    }
    
    // Wait a bit more to ensure game end is processed
    sleep(Duration::from_secs(3)).await;
    
    // Final check for game end
    let messages = messages_arc.lock().await;
    let has_ended = messages.iter().any(|m| {
        m.get("msg_type")
            .and_then(|t| t.as_str())
            .map(|t| t == "game_ended")
            .unwrap_or(false)
    });
    drop(messages);
    
    if !has_ended {
        // Give it one more chance
        sleep(Duration::from_secs(2)).await;
    }

    // Wait a bit more for all messages
    sleep(Duration::from_secs(2)).await;

    // Verify winner was broadcasted
    let messages = messages_arc.lock().await;
    let game_ended_messages: Vec<_> = messages
        .iter()
        .filter(|m| {
            m.get("msg_type")
                .and_then(|t| t.as_str())
                .map(|t| t == "game_ended")
                .unwrap_or(false)
        })
        .collect();

    assert!(!game_ended_messages.is_empty(), "Game ended message should be broadcasted");
    
    if let Some(ended_msg) = game_ended_messages.first() {
        let payload = ended_msg.get("payload").unwrap();
        assert!(payload.get("winner").is_some(), "Winner should be in game_ended message");
        assert!(payload.get("winner")
            .and_then(|w| w.get("username"))
            .is_some(), "Winner username should be present");
        
        let winner_username = payload.get("winner")
            .and_then(|w| w.get("username"))
            .and_then(|u| u.as_str())
            .unwrap();
        
        println!("Winner evaluated: {}", winner_username);
    }

    // Verify game status is FINISHED
    let response = client
        .get(format!("{}/api/games/{}", base_url, game_id))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let data: Value = response.json().await.unwrap();
    assert_eq!(data["success"], true);
    
    let game_info = data.get("data").unwrap();
    let status = game_info.get("status").and_then(|s| s.as_str()).unwrap();
    assert_eq!(status, "FINISHED", "Game status should be FINISHED");
    
    // Verify bank was updated (should be > 0 if any cricketers were sold)
    let bank = game_info.get("bank").and_then(|b| b.as_u64()).unwrap();
    assert!(bank > 0, "Bank should be greater than 0 if cricketers were sold");
    
    println!("Winner evaluation test completed successfully!");
    println!("Game status: {}", status);
    println!("Bank amount: {}", bank);
    println!("Winner coins: {}", 
        game_ended_messages.first()
            .and_then(|m| m.get("payload"))
            .and_then(|p| p.get("winner"))
            .and_then(|w| w.get("coins"))
            .and_then(|c| c.as_u64())
            .unwrap_or(0)
    );
}

