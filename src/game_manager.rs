use crate::game::Game;
use crate::player::Player;
use crate::ws::broadcast_to_room;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use serde::{Serialize, Deserialize};

/// Messages that can be sent to the game manager
#[derive(Debug, Clone)]
pub enum GameManagerMessage {
    CreateGame {
        creator: Player,
        initial_purse: u64,
        response_tx: mpsc::UnboundedSender<String>, // game_id
    },
    AddPlayer {
        game_id: String,
        player: Player,
        initial_purse: u64,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    StartGame {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    PlaceBid {
        game_id: String,
        player: Player,
        price: u64,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    OptOut {
        game_id: String,
        player_username: String,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    SellCricketer {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    EndGame {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Result<Player, String>>,
    },
    RemovePlayer {
        game_id: String,
        player: Player,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    GetGameInfo {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Option<GameInfo>>,
    },
    ListGames {
        response_tx: mpsc::UnboundedSender<Vec<GameInfo>>,
    },
}

/// Information about a game (for querying)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameInfo {
    pub game_id: String,
    pub status: String,
    pub team_count: usize,
    pub bank: u64,
}

/// Manages all ongoing games
pub struct GameManager {
    games: Arc<Mutex<HashMap<String, Game>>>,
    message_rx: mpsc::UnboundedReceiver<GameManagerMessage>,
    message_tx: mpsc::UnboundedSender<GameManagerMessage>,
}

impl GameManager {
    /// Create a new game manager
    pub fn new() -> (Self, mpsc::UnboundedSender<GameManagerMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                games: Arc::new(Mutex::new(HashMap::new())),
                message_rx: rx,
                message_tx: tx.clone(),
            },
            tx,
        )
    }

    /// Start the game manager's message processing loop
    pub async fn run(&mut self) {
        while let Some(message) = self.message_rx.recv().await {
            self.handle_message(message).await;
        }
    }

    /// Handle incoming messages
    async fn handle_message(&self, message: GameManagerMessage) {
        match message {
            GameManagerMessage::CreateGame {
                creator,
                initial_purse,
                response_tx,
            } => {
                let game_id = self.create_game(creator, initial_purse).await;
                let _ = response_tx.send(game_id);
            }
            GameManagerMessage::AddPlayer {
                game_id,
                player,
                initial_purse,
                response_tx,
            } => {
                let result = self.add_player(&game_id, player, initial_purse).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::StartGame {
                game_id,
                response_tx,
            } => {
                let result = self.start_game(&game_id, self.message_tx.clone()).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::PlaceBid {
                game_id,
                player,
                price,
                response_tx,
            } => {
                let result = self.place_bid(&game_id, player, price).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::OptOut {
                game_id,
                player_username,
                response_tx,
            } => {
                let result = self.opt_out(&game_id, player_username).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::SellCricketer {
                game_id,
                response_tx,
            } => {
                let result = self.sell_cricketer(&game_id).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::EndGame {
                game_id,
                response_tx,
            } => {
                let result = self.end_game(&game_id).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::RemovePlayer {
                game_id,
                player,
                response_tx,
            } => {
                let result = self.remove_player(&game_id, player).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::GetGameInfo {
                game_id,
                response_tx,
            } => {
                let info = self.get_game_info(&game_id).await;
                let _ = response_tx.send(info);
            }
            GameManagerMessage::ListGames { response_tx } => {
                let games = self.list_games().await;
                let _ = response_tx.send(games);
            }
        }
    }

    /// Create a new game
    async fn create_game(&self, creator: Player, initial_purse: u64) -> String {
        let mut games = self.games.lock().await;
        let game = Game::new(creator, initial_purse);
        let game_id = game.game_id().clone();
        games.insert(game_id.clone(), game);
        game_id
    }

    /// Start a game
    async fn start_game(&self, game_id: &str, game_manager_tx: mpsc::UnboundedSender<GameManagerMessage>) -> Result<(), String> {
        let games = self.games.lock().await;
        if let Some(game) = games.get(game_id) {
            let game_id_clone = game_id.to_string();
            let cricketers = game.get_cricketers();
            let teams_count = game.teams_count();
            let state = game.get_state();
            drop(games);
            
            // Start the game
            let mut games = self.games.lock().await;
            if let Some(game) = games.get_mut(game_id) {
                game.start();
            }
            drop(games);

            // Start game loop in background
            tokio::spawn(async move {
                let mut cricketer_index = 0;
                loop {
                    if cricketer_index >= cricketers.len() {
                        break;
                    }

                    let cricketer = cricketers[cricketer_index].clone();
                    cricketer_index += 1;

                    // Set current cricketer and reset state
                    state.set_current_cricketer(Some(cricketer.clone())).await;
                    state.reset_opt_outs().await;
                    state.reset_bid_state().await;

                    // Broadcast new cricketer available
                    crate::ws::broadcast_to_room(&game_id_clone, "cricketer_available", &serde_json::json!({
                        "cricketer": cricketer.name,
                        "base_price": cricketer.price
                    }));

                    let start_time = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    // Timer loop
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                        let current_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        let opted_out_count = state.get_opted_out_count().await;
                        let last_bid_time = state.get_last_bid_timestamp().await;

                        // Check if n-1 players have opted out
                        if opted_out_count >= teams_count - 1 {
                            let current_bid = state.get_current_bid().await;
                            if let Some(_bid) = current_bid.as_ref() {
                                // Sell to current bid holder
                                let _ = game_manager_tx.send(crate::game_manager::GameManagerMessage::SellCricketer {
                                    game_id: game_id_clone.clone(),
                                    response_tx: mpsc::unbounded_channel().0,
                                });
                            } else {
                                // All opted out, cricketer goes unsold
                                crate::ws::broadcast_to_room(&game_id_clone, "cricketer_unsold", &serde_json::json!({
                                    "cricketer": cricketer.name
                                }));
                            }
                            break;
                        }

                        // Check if all players have opted out
                        if opted_out_count >= teams_count {
                            let current_bid = state.get_current_bid().await;
                            if let Some(_bid) = current_bid.as_ref() {
                                // Sell to current bid holder
                                let _ = game_manager_tx.send(crate::game_manager::GameManagerMessage::SellCricketer {
                                    game_id: game_id_clone.clone(),
                                    response_tx: mpsc::unbounded_channel().0,
                                });
                            } else {
                                // All opted out, cricketer goes unsold
                                crate::ws::broadcast_to_room(&game_id_clone, "cricketer_unsold", &serde_json::json!({
                                    "cricketer": cricketer.name
                                }));
                            }
                            break;
                        }

                        // Check timer
                        if let Some(last_bid) = last_bid_time {
                            if current_time - last_bid >= crate::game::MAX_IDLE_TIME_IN_SECS {
                                let current_bid = state.get_current_bid().await;
                                if let Some(_bid) = current_bid.as_ref() {
                                    // Sell to current bid holder
                                    let _ = game_manager_tx.send(crate::game_manager::GameManagerMessage::SellCricketer {
                                        game_id: game_id_clone.clone(),
                                        response_tx: mpsc::unbounded_channel().0,
                                    });
                                } else {
                                    // No bids, cricketer goes unsold
                                    crate::ws::broadcast_to_room(&game_id_clone, "cricketer_unsold", &serde_json::json!({
                                        "cricketer": cricketer.name
                                    }));
                                }
                                break;
                            }
                        } else {
                            // No bids yet, check if time since cricketer was announced has elapsed
                            if current_time - start_time >= crate::game::MAX_IDLE_TIME_IN_SECS {
                                // Cricketer goes unsold
                                crate::ws::broadcast_to_room(&game_id_clone, "cricketer_unsold", &serde_json::json!({
                                    "cricketer": cricketer.name
                                }));
                                break;
                            }
                        }
                    }

                    // Wait a bit before next cricketer
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            });

            Ok(())
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// Add a player to a game
    async fn add_player(
        &self,
        game_id: &str,
        player: Player,
        initial_purse: u64,
    ) -> Result<(), String> {
        let mut games = self.games.lock().await;
        if let Some(game) = games.get_mut(game_id) {
            game.add_team(player, initial_purse);
            Ok(())
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// Place a bid in a game
    async fn place_bid(
        &self,
        game_id: &str,
        player: Player,
        price: u64,
    ) -> Result<(), String> {
        let games = self.games.lock().await;
        if let Some(game) = games.get(game_id) {
            // Get current cricketer
            let state = game.get_state();
            if let Some(cricketer) = state.get_current_cricketer().await {
                game.send_bid_to_channel(player, cricketer.name, price)
                    .map_err(|e| format!("Failed to send bid: {:?}", e))?;
                Ok(())
            } else {
                Err("No cricketer currently being auctioned".to_string())
            }
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// Opt out of current bidding
    async fn opt_out(
        &self,
        game_id: &str,
        player_username: String,
    ) -> Result<(), String> {
        let games = self.games.lock().await;
        if let Some(game) = games.get(game_id) {
            game.opt_out(player_username).await;
            Ok(())
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// Sell the current cricketer (end current auction)
    async fn sell_cricketer(&self, game_id: &str) -> Result<(), String> {
        let mut games = self.games.lock().await;
        if let Some(game) = games.get_mut(game_id) {
            match game.sell().await {
                Ok((player_username, cricketer, price)) => {
                    // Broadcast cricketer sold event to all connected clients
                    broadcast_to_room(game_id, "cricketer_sold", &serde_json::json!({
                        "game_id": game_id,
                        "player": {
                            "username": player_username,
                        },
                        "cricketer": cricketer,
                        "price": price
                    }));
                    Ok(())
                }
                Err(e) => Err(e)
            }
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// End a game
    async fn end_game(&self, game_id: &str) -> Result<Player, String> {
        let mut games = self.games.lock().await;
        if let Some(game) = games.get_mut(game_id) {
            let winner = game.end();
            // Optionally remove the game from the map after ending
            // games.remove(game_id);
            Ok(winner)
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// Remove a player from a game
    async fn remove_player(&self, game_id: &str, player: Player) -> Result<(), String> {
        let mut games = self.games.lock().await;
        if let Some(game) = games.get_mut(game_id) {
            game.remove(&player).await;
            Ok(())
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// Get information about a game
    async fn get_game_info(&self, game_id: &str) -> Option<GameInfo> {
        let games = self.games.lock().await;
        games.get(game_id).map(|game| game.get_info())
    }

    /// List all games
    async fn list_games(&self) -> Vec<GameInfo> {
        let games = self.games.lock().await;
        games.values().map(|game| game.get_info()).collect()
    }
}

