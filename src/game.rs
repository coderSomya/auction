use crate::player::Player;
use crate::purse::Purse;
use std::collections::HashMap;
use std::vec::Vec;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use serde::{Serialize, Deserialize};


const MAX_IDLE_TIME_IN_SECS: u64 = 1*60; // 1 min


#[derive(Debug, Clone, PartialEq)]
pub enum GameStatus{
    CREATED,
    STARTED,
    FINISHED
}

struct Buy{
    cricketer: String,
    price: u64
}

#[derive(Clone)]
pub struct Bid{
    // for whom the bid is going on
    pub cricketer: String,
    // against whom the bid is going on
    pub price: u64,
    // the player who has made the last bid
    pub player: Player,
    // the time when this bid was made
    pub timestamp: u64
}

struct GameState{
    // player -> {cricketer, price}
    teams: Mutex<HashMap<String, Vec<Buy>>>,
    // current bid for the cricketer being auctioned
    current_bid: Mutex<Option<Bid>>,
    // channel sender for incoming bids
    bid_tx: mpsc::UnboundedSender<Bid>
}

impl GameState{
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Bid>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self{
                teams: Mutex::new(HashMap::new()),
                current_bid: Mutex::new(None),
                bid_tx: tx
            },
            rx
        )
    }

    pub async fn add_cricketer_to_a_team(&self, player: String, cricketer: String, price: u64){
        let buy = Buy{
            cricketer,
            price
        };

        let mut teams = self.teams.lock().await;
        if let Some(buys_of_this_player) = teams.get_mut(&player){
            buys_of_this_player.push(buy);
        }else{
            teams.insert(player, vec![buy]);
        }
    }

}

#[derive(Serialize, Deserialize, Clone)]
pub struct Team{
    pub player: Player,
    pub purse: Purse
}

impl Team{
    pub fn new(player: Player, purse: Purse) -> Self{
        Self{
            player,
            purse
        }
    }
}

pub struct Game{
    game_id: String,
    teams: Vec<Team>,
    status: GameStatus,
    state: Arc<GameState>,
    bid_rx: Option<mpsc::UnboundedReceiver<Bid>>,
    bank: u64 // total money collected in this game
}

impl Game{
    pub fn new(creator: Player, initial_purse: u64) -> Self{
        let (state, bid_rx) = GameState::new();
        let creator_team = Team::new(creator, Purse::new(initial_purse));
        Self{
            game_id: uuid::Uuid::new_v4().to_string(),
            teams: vec![creator_team],
            status: GameStatus::CREATED,
            state: Arc::new(state),
            bid_rx: Some(bid_rx),
            bank: 0
        }
    }

    pub fn game_id(&self) -> &String {
        &self.game_id
    }

    pub fn status(&self) -> &GameStatus {
        &self.status
    }

    pub fn bank(&self) -> u64 {
        self.bank
    }

    pub fn get_info(&self) -> crate::game_manager::GameInfo {
        crate::game_manager::GameInfo {
            game_id: self.game_id.clone(),
            status: format!("{:?}", self.status),
            team_count: self.teams.len(),
            bank: self.bank,
        }
    }

    pub fn start_bid_consumer(&mut self) {
        let mut bid_rx = match self.bid_rx.take() {
            Some(rx) => rx,
            None => return, // Consumer already started
        };
        let state = Arc::clone(&self.state);
        
        tokio::spawn(async move {
            while let Some(bid) = bid_rx.recv().await {
                // Validate the bid
                let is_valid = {
                    let current_bid = state.current_bid.lock().await;
                    match current_bid.as_ref() {
                        Some(current_bid) => {
                            bid.price > current_bid.price
                        },
                        None => {
                            true // First bid is always valid
                        }
                    }
                };

                // Update bid if valid
                if is_valid {
                    let mut current_bid = state.current_bid.lock().await;
                    *current_bid = Some(bid);
                }
            }
        });
    }

    pub fn add_team(&mut self, player: Player, initial_purse: u64){
        let team = Team::new(player, Purse::new(initial_purse));
        self.teams.push(team);
    }

    pub fn teams(&self) -> &Vec<Team> {
        &self.teams
    }

    pub fn start(&mut self){
        self.status = GameStatus::STARTED;
        self.start_bid_consumer();
    }

    pub fn end(&mut self) -> Player{
        // TODO: Implement evaluator logic
        // For now, return the first player as winner
        let winner = if let Some(team) = self.teams.first() {
            team.player.clone()
        } else {
            // Return a dummy player if no teams
            Player::new("".to_string(), "".to_string(), "".to_string())
        };

        self.status = GameStatus::FINISHED;
        self.award_winner(winner.clone());
        winner
    }

    pub fn try_update_bid(&self, bid: Bid){
        // Send bid to channel for processing
        let _ = self.state.bid_tx.send(bid);
    }

    pub fn send_bid_to_channel(&self, player: Player, cricketer: String, price: u64) -> Result<(), mpsc::error::SendError<Bid>> {
        let bid = Bid {
            cricketer,
            price,
            player,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        };
        self.state.bid_tx.send(bid)
    }

    pub async fn sell(&mut self) -> Result<(), String>{
        let mut current_bid = self.state.current_bid.lock().await;
        match current_bid.take(){
            Some(bid) => {
                let player_username = bid.player.username().clone();
                let bid_price = bid.price;
                drop(current_bid); // Release the lock before calling another async function
                self.state.add_cricketer_to_a_team(player_username.clone(), bid.cricketer.clone(), bid_price).await;

                // Reduce the purse of this player
                if let Some(team) = self.teams.iter_mut().find(|t| t.player.username() == &player_username) {
                    team.purse.spend(bid_price);
                    // Add to bank
                    self.bank += bid_price;
                }

                Ok(())
            },
            None => {
                Err("cannot sell with no current bids".to_string())
            }
        }
    }
    
    pub async fn remove(&mut self, player: &Player) {
        let username = player.username().clone();
        
        // Remove from teams vector
        self.teams.retain(|team| team.player.username() != &username);
        
        // Remove from state.teams HashMap
        let mut state_teams = self.state.teams.lock().await;
        state_teams.remove(&username);
    }

    pub fn award_winner(&self, winner: Player){
        let winning_amount = get_winning_amount(self);
        winner.coins += winning_amount;
    }

    pub async fn is_bid_valid(&self, bid: &Bid) -> bool{
        let current_bid = self.state.current_bid.lock().await;
        match current_bid.as_ref() {
            Some(current_bid) => {
                bid.price > current_bid.price && bid.cricketer == current_bid.cricketer
            },
            None => {
                true // First bid is always valid
            }
        }
    }
}