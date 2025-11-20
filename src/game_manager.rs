use crate::game::{Game, Bid};
use crate::player::Player;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use serde::{Serialize, Deserialize};

/// Messages that can be sent to the game manager
#[derive(Debug, Clone)]
pub enum GameManagerMessage {
    /// Create a new game
    CreateGame {
        creator: Player,
        initial_purse: u64,
        response_tx: mpsc::UnboundedSender<String>, // game_id
    },
    /// Start a game
    StartGame {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    /// Place a bid in a game
    PlaceBid {
        game_id: String,
        player: Player,
        cricketer: String,
        price: u64,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    /// Sell the current cricketer (end current auction)
    SellCricketer {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    /// End a game
    EndGame {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Result<Player, String>>,
    },
    /// Remove a player from a game
    RemovePlayer {
        game_id: String,
        player: Player,
        response_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    /// Get game status/info
    GetGameInfo {
        game_id: String,
        response_tx: mpsc::UnboundedSender<Option<GameInfo>>,
    },
    /// List all games
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
}

impl GameManager {
    /// Create a new game manager
    pub fn new() -> (Self, mpsc::UnboundedSender<GameManagerMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                games: Arc::new(Mutex::new(HashMap::new())),
                message_rx: rx,
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
            GameManagerMessage::StartGame {
                game_id,
                response_tx,
            } => {
                let result = self.start_game(&game_id).await;
                let _ = response_tx.send(result);
            }
            GameManagerMessage::PlaceBid {
                game_id,
                player,
                cricketer,
                price,
                response_tx,
            } => {
                let result = self.place_bid(&game_id, player, cricketer, price).await;
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
    async fn start_game(&self, game_id: &str) -> Result<(), String> {
        let mut games = self.games.lock().await;
        if let Some(game) = games.get_mut(game_id) {
            game.start();
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
        cricketer: String,
        price: u64,
    ) -> Result<(), String> {
        let games = self.games.lock().await;
        if let Some(game) = games.get(game_id) {
            game.send_bid_to_channel(player, cricketer, price)
                .map_err(|e| format!("Failed to send bid: {:?}", e))?;
            Ok(())
        } else {
            Err(format!("Game {} not found", game_id))
        }
    }

    /// Sell the current cricketer (end current auction)
    async fn sell_cricketer(&self, game_id: &str) -> Result<(), String> {
        let mut games = self.games.lock().await;
        if let Some(game) = games.get_mut(game_id) {
            game.sell().await
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

    async fn get_all_games(&self) -> Vec<Game> {
        let games = self.games.lock().await;
        games.values().cloned().collect()
    }
}

