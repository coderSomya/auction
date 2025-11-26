use crate::player::Player;
use crate::purse::Purse;
use crate::utils::{get_winning_amount, Cricketer, load_cricketers};
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use serde::{Serialize, Deserialize};
use rand::seq::SliceRandom;
use rand::thread_rng;

pub const MAX_IDLE_TIME_IN_SECS: u64 = 60; // 1 min


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

impl Bid{
    pub fn new(cricketer: String, price: u64, player: Player, timestamp: u64) -> Self{
        Self{
            cricketer,
            price,
            player,
            timestamp
        }
    }
}

pub struct GameState{
    // player -> {cricketer, price}
    teams: Mutex<HashMap<String, Vec<Buy>>>,
    // current bid for the cricketer being auctioned
    current_bid: Mutex<Option<Bid>>,
    // channel sender for incoming bids
    bid_tx: mpsc::UnboundedSender<Bid>,
    // players who have opted out of current bidding
    opted_out: Mutex<HashSet<String>>,
    // timestamp of last bid (for timer)
    last_bid_timestamp: Mutex<Option<u64>>,
    // current cricketer being auctioned
    current_cricketer: Mutex<Option<Cricketer>>,
}

impl GameState{
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Bid>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self{
                teams: Mutex::new(HashMap::new()),
                current_bid: Mutex::new(None),
                bid_tx: tx,
                opted_out: Mutex::new(HashSet::new()),
                last_bid_timestamp: Mutex::new(None),
                current_cricketer: Mutex::new(None),
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

    pub async fn opt_out(&self, player_username: String) -> bool {
        let mut opted_out = self.opted_out.lock().await;
        opted_out.insert(player_username)
    }

    pub async fn reset_opt_outs(&self) {
        let mut opted_out = self.opted_out.lock().await;
        opted_out.clear();
    }

    pub async fn get_opted_out_count(&self) -> usize {
        let opted_out = self.opted_out.lock().await;
        opted_out.len()
    }

    pub async fn set_current_cricketer(&self, cricketer: Option<Cricketer>) {
        let mut current = self.current_cricketer.lock().await;
        *current = cricketer;
    }

    pub async fn get_current_cricketer(&self) -> Option<Cricketer> {
        let current = self.current_cricketer.lock().await;
        current.clone()
    }

    pub async fn update_last_bid_timestamp(&self) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut last_bid = self.last_bid_timestamp.lock().await;
        *last_bid = Some(timestamp);
    }

    pub async fn get_last_bid_timestamp(&self) -> Option<u64> {
        let last_bid = self.last_bid_timestamp.lock().await;
        *last_bid
    }

    pub async fn reset_bid_state(&self) {
        let mut current_bid = self.current_bid.lock().await;
        *current_bid = None;
        let mut last_bid = self.last_bid_timestamp.lock().await;
        *last_bid = None;
    }

    pub async fn get_current_bid(&self) -> Option<Bid> {
        let current_bid = self.current_bid.lock().await;
        current_bid.clone()
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
    bank: u64, // total money collected in this game
    cricketers: Vec<Cricketer>, // list of cricketers to auction
    cricketer_index: usize, // current index in cricketers list
}

impl Game{
    pub fn new(creator: Player, initial_purse: u64) -> Self{
        let (state, bid_rx) = GameState::new();
        let creator_team = Team::new(creator, Purse::new(initial_purse));
        let mut cricketers = load_cricketers();
        cricketers.shuffle(&mut thread_rng());
        Self{
            game_id: uuid::Uuid::new_v4().to_string(),
            teams: vec![creator_team],
            status: GameStatus::CREATED,
            state: Arc::new(state),
            bid_rx: Some(bid_rx),
            bank: 0,
            cricketers,
            cricketer_index: 0,
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
                            bid.price > current_bid.price && bid.cricketer == current_bid.cricketer
                        },
                        None => {
                            // Check if bid is for current cricketer and meets base price
                            if let Some(cricketer) = state.get_current_cricketer().await {
                                bid.cricketer == cricketer.name && bid.price >= cricketer.price
                            } else {
                                false
                            }
                        }
                    }
                };

                // Update bid if valid
                if is_valid {
                    let mut current_bid = state.current_bid.lock().await;
                    *current_bid = Some(bid.clone());
                    drop(current_bid);
                    state.update_last_bid_timestamp().await;
                    // Reset opt-outs when a new bid comes in
                    state.reset_opt_outs().await;
                }
            }
        });
    }

    pub async fn opt_out(&self, player_username: String) -> bool {
        self.state.opt_out(player_username).await
    }

    pub async fn get_next_cricketer(&mut self) -> Option<Cricketer> {
        if self.cricketer_index < self.cricketers.len() {
            let cricketer = self.cricketers[self.cricketer_index].clone();
            self.cricketer_index += 1;
            Some(cricketer)
        } else {
            None
        }
    }

    pub fn teams_count(&self) -> usize {
        self.teams.len()
    }

    pub fn get_state(&self) -> Arc<GameState> {
        Arc::clone(&self.state)
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

    pub fn get_cricketers(&self) -> Vec<Cricketer> {
        self.cricketers.clone()
    }

    pub fn get_cricketer_index(&self) -> usize {
        self.cricketer_index
    }

    pub fn set_cricketer_index(&mut self, index: usize) {
        self.cricketer_index = index;
    }


    pub async fn end(&mut self) -> Player {
        use crate::evaluator::Evaluator;
        
        // Use default evaluator (random)
        let evaluator = Evaluator::new();
        let winner_result = evaluator.get_winner(self).await;
        let winner = winner_result.player;

        self.status = GameStatus::FINISHED;
        let winner_username = winner.username().clone();
        self.award_winner(&winner_username);
        // Get the updated winner from teams
        if let Some(team) = self.teams.iter().find(|t| t.player.username() == &winner_username) {
            team.player.clone()
        } else {
            winner
        }
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

    pub async fn sell(&mut self) -> Result<(String, String, u64), String>{
        let mut current_bid = self.state.current_bid.lock().await;
        match current_bid.take(){
            Some(bid) => {
                let player_username = bid.player.username().clone();
                let bid_price = bid.price;
                let cricketer = bid.cricketer.clone();
                drop(current_bid); // Release the lock before calling another async function
                self.state.add_cricketer_to_a_team(player_username.clone(), bid.cricketer.clone(), bid_price).await;

                // Reduce the purse of this player
                if let Some(team) = self.teams.iter_mut().find(|t| t.player.username() == &player_username) {
                    if team.purse.cash() >= bid_price {
                        team.purse.spend(bid_price);
                        // Add to bank
                        self.bank += bid_price;
                    } else {
                        // Player doesn't have enough money, skip this sale
                        return Err("Player doesn't have enough money in purse".to_string());
                    }
                }

                Ok((player_username, cricketer, bid_price))
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

    pub fn award_winner(&mut self, winner_username: &str){
        let winning_amount = get_winning_amount(&self);
        // Update the winner's coins in the teams
        if let Some(team) = self.teams.iter_mut().find(|t| t.player.username() == winner_username) {
            team.player.coins += winning_amount;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_game_data(){
        let mut player1 = Player::new("test".to_string(), "test".to_string(), "test".to_string());
        player1.add_coins(100);

        let mut game = Game::new(player1.clone(), 50);

        assert_eq!(game.bank(), 0);

        assert_eq!(game.teams().len(), 1);
        assert_eq!(game.teams()[0].player.username(), "test");
        assert_eq!(game.teams()[0].purse.cash(), 50);

        assert_eq!(*game.status(), GameStatus::CREATED);

        let mut player2 = Player::new("test2".to_string(), "test2".to_string(), "test2".to_string());
        player2.add_coins(100);
        
        player2.join_game(&mut game, 40);
        assert_eq!(game.teams().len(), 2);
        assert_eq!(game.teams()[1].player.username(), "test2");
        assert_eq!(game.teams()[1].purse.cash(), 40);

        game.start();
        game.start_bid_consumer();
        
        assert_eq!(*game.status(), GameStatus::STARTED);

        let bid = Bid::new("msd".to_string(), 20, player1.clone(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
        game.try_update_bid(bid);
        
        // Give the async task time to process the bid
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}