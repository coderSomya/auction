use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::info;

use crate::game_manager::{GameManagerMessage, GameInfo};
use crate::player::Player;
use crate::ws::broadcast_to_room;

/// Application state containing the game manager sender
#[derive(Clone)]
pub struct AppState {
    pub game_manager_tx: mpsc::UnboundedSender<GameManagerMessage>,
}

/// Request to create a player
#[derive(Debug, Deserialize)]
pub struct CreatePlayerRequest {
    pub username: String,
    pub password: String,
    pub email: String,
}

/// Response for creating a player
#[derive(Debug, Serialize)]
pub struct CreatePlayerResponse {
    pub username: String,
    pub email: String,
}

/// Request to create a new game
#[derive(Debug, Deserialize)]
pub struct CreateGameRequest {
    pub username: String,
    pub initial_purse: u64,
}

/// Response for creating a game
#[derive(Debug, Serialize)]
pub struct CreateGameResponse {
    pub game_id: String,
}

/// Request to join a game
#[derive(Debug, Deserialize)]
pub struct JoinGameRequest {
    pub username: String,
    pub initial_purse: u64,
}

/// Request to opt out of current bidding
#[derive(Debug, Deserialize)]
pub struct OptOutRequest {
    pub username: String,
}

/// Request to place a bid
#[derive(Debug, Deserialize)]
pub struct PlaceBidRequest {
    pub username: String,
    pub price: u64,
}

/// Generic API response
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message),
        }
    }
}

/// Create a new game
/// POST /api/games
pub async fn create_game(
    State(state): State<AppState>,
    Json(request): Json<CreateGameRequest>,
) -> impl IntoResponse {
    info!("Creating game for user: {}", request.username);

    // Create a simple player (password/email not needed for game flow)
    let player = Player::new(
        request.username.clone(),
        "".to_string(), // Not used in game logic
        "".to_string(), // Not used in game logic
    );

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::CreateGame {
        creator: player,
        initial_purse: request.initial_purse,
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<CreateGameResponse>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(game_id) => {
            let response = CreateGameResponse { game_id: game_id.clone() };
            
            // Broadcast game created event
            broadcast_to_room(&game_id, "game_created", &serde_json::json!({
                "game_id": game_id,
                "creator": request.username
            }));
            
            (
                StatusCode::CREATED,
                Json(ApiResponse::success(response)),
            )
                .into_response()
        }
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<CreateGameResponse>::error(
                "Failed to create game".to_string(),
            )),
        )
            .into_response(),
    }
}

/// Join a game
/// POST /api/games/:game_id/join
pub async fn join_game(
    State(state): State<AppState>,
    Path(game_id): Path<String>,
    Json(request): Json<JoinGameRequest>,
) -> impl IntoResponse {
    info!("Player {} joining game: {}", request.username, game_id);

    let player = Player::new(
        request.username.clone(),
        "".to_string(),
        "".to_string(),
    );

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::AddPlayer {
        game_id: game_id.clone(),
        player,
        initial_purse: request.initial_purse,
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(Ok(_)) => {
            // Broadcast player joined event
            broadcast_to_room(&game_id, "player_joined", &serde_json::json!({
                "game_id": game_id,
                "player": {
                    "username": request.username
                }
            }));
            
            (
                StatusCode::OK,
                Json(ApiResponse::<()>::success(())),
            )
                .into_response()
        }
        Some(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<()>::error(e)),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Failed to join game".to_string(),
            )),
        )
            .into_response(),
    }
}

/// Opt out of current bidding
/// POST /api/games/:game_id/opt-out
pub async fn opt_out(
    State(state): State<AppState>,
    Path(game_id): Path<String>,
    Json(request): Json<OptOutRequest>,
) -> impl IntoResponse {
    info!("Player {} opting out of game: {}", request.username, game_id);

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::OptOut {
        game_id: game_id.clone(),
        player_username: request.username.clone(),
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(Ok(_)) => {
            // Broadcast opt-out event
            broadcast_to_room(&game_id, "player_opted_out", &serde_json::json!({
                "game_id": game_id,
                "player": {
                    "username": request.username
                }
            }));
            
            (
                StatusCode::OK,
                Json(ApiResponse::<()>::success(())),
            )
                .into_response()
        }
        Some(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<()>::error(e)),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Failed to opt out".to_string(),
            )),
        )
            .into_response(),
    }
}

/// Start a game
/// POST /api/games/:game_id/start
pub async fn start_game(
    State(state): State<AppState>,
    Path(game_id): Path<String>,
) -> impl IntoResponse {
    info!("Starting game: {}", game_id);

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::StartGame {
        game_id: game_id.clone(),
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(Ok(_)) => {
            // Broadcast game started event
            broadcast_to_room(&game_id, "game_started", &serde_json::json!({
                "game_id": game_id
            }));
            
            (
                StatusCode::OK,
                Json(ApiResponse::<()>::success(())),
            )
                .into_response()
        }
        Some(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<()>::error(e)),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Failed to start game".to_string(),
            )),
        )
            .into_response(),
    }
}

/// Place a bid
/// POST /api/games/:game_id/bid
pub async fn place_bid(
    State(state): State<AppState>,
    Path(game_id): Path<String>,
    Json(request): Json<PlaceBidRequest>,
) -> impl IntoResponse {
    info!(
        "Placing bid in game {}: {} bids {}",
        game_id, request.username, request.price
    );

    let player = Player::new(
        request.username.clone(),
        "".to_string(),
        "".to_string(),
    );

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::PlaceBid {
        game_id: game_id.clone(),
        player: player.clone(),
        price: request.price,
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(Ok(_)) => {
            // Broadcast bid placed event
            broadcast_to_room(&game_id, "bid_placed", &serde_json::json!({
                "game_id": game_id,
                "player": {
                    "username": request.username,
                },
                "price": request.price
            }));
            
            (
                StatusCode::OK,
                Json(ApiResponse::<()>::success(())),
            )
                .into_response()
        }
        Some(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<()>::error(e)),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Failed to place bid".to_string(),
            )),
        )
            .into_response(),
    }
}

/// End a game
/// POST /api/games/:game_id/end
pub async fn end_game(
    State(state): State<AppState>,
    Path(game_id): Path<String>,
) -> impl IntoResponse {
    info!("Ending game: {}", game_id);

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::EndGame {
        game_id: game_id.clone(),
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Player>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(Ok(winner)) => {
            // Broadcast game ended event
            broadcast_to_room(&game_id, "game_ended", &serde_json::json!({
                "game_id": game_id,
                "winner": {
                    "username": winner.username(),
                    "coins": winner.coins
                }
            }));
            
            (
                StatusCode::OK,
                Json(ApiResponse::success(winner)),
            )
                .into_response()
        }
        Some(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<Player>::error(e)),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Player>::error(
                "Failed to end game".to_string(),
            )),
        )
            .into_response(),
    }
}

/// Remove a player from a game
/// POST /api/games/:game_id/remove-player
pub async fn remove_player(
    State(state): State<AppState>,
    Path(game_id): Path<String>,
    Json(request): Json<JoinGameRequest>,
) -> impl IntoResponse {
    info!("Removing player {} from game: {}", request.username, game_id);

    let player = Player::new(
        request.username,
        "".to_string(),
        "".to_string(),
    );

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::RemovePlayer {
        game_id: game_id.clone(),
        player: player.clone(),
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(Ok(_)) => {
            // Broadcast player removed event
            broadcast_to_room(&game_id, "player_removed", &serde_json::json!({
                "game_id": game_id,
                "player": {
                    "username": player.username(),
                }
            }));
            
            (
                StatusCode::OK,
                Json(ApiResponse::<()>::success(())),
            )
                .into_response()
        }
        Some(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<()>::error(e)),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::error(
                "Failed to remove player".to_string(),
            )),
        )
            .into_response(),
    }
}

/// Get game information
/// GET /api/games/:game_id
pub async fn get_game_info(
    State(state): State<AppState>,
    Path(game_id): Path<String>,
) -> impl IntoResponse {
    info!("Getting game info: {}", game_id);

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::GetGameInfo {
        game_id,
        response_tx,
    };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<GameInfo>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(Some(info)) => (
            StatusCode::OK,
            Json(ApiResponse::success(info)),
        )
            .into_response(),
        Some(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::<GameInfo>::error(
                "Game not found".to_string(),
            )),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<GameInfo>::error(
                "Failed to get game info".to_string(),
            )),
        )
            .into_response(),
    }
}

/// List all games
/// GET /api/games
pub async fn list_games(
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("Listing all games");

    let (response_tx, mut response_rx) = mpsc::unbounded_channel();

    let message = GameManagerMessage::ListGames { response_tx };

    if state.game_manager_tx.send(message).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Vec<GameInfo>>::error(
                "Game manager unavailable".to_string(),
            )),
        )
            .into_response();
    }

    match response_rx.recv().await {
        Some(games) => (
            StatusCode::OK,
            Json(ApiResponse::success(games)),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Vec<GameInfo>>::error(
                "Failed to list games".to_string(),
            )),
        )
            .into_response(),
    }
}

/// Create the API router
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/api/games", post(create_game))
        .route("/api/games", get(list_games))
        .route("/api/games/:game_id", get(get_game_info))
        .route("/api/games/:game_id/join", post(join_game))
        .route("/api/games/:game_id/start", post(start_game))
        .route("/api/games/:game_id/bid", post(place_bid))
        .route("/api/games/:game_id/opt-out", post(opt_out))
        .route("/api/games/:game_id/end", post(end_game))
        .route("/api/games/:game_id/remove-player", post(remove_player))
        .with_state(state)
}

