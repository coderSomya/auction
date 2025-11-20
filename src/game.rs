use crate::player::Player;
use std::collections::HashMap;
use std::vec::Vec;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};


const MAX_IDLE_TIME_IN_SECS: u64 = 1*60; // 1 min


enum GameStatus{
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

pub struct Game{
    game_id: String,
    players: Vec<Player>,
    status: GameStatus,
    state: Arc<GameState>,
    bid_rx: Option<mpsc::UnboundedReceiver<Bid>>
}

impl Game{
    pub fn new(&self, creator: Player) -> Self{
        let (state, bid_rx) = GameState::new();
        Self{
            game_id: generate_random_id(),
            players: vec![creator],
            status: GameStatus::CREATED,
            state: Arc::new(state),
            bid_rx: Some(bid_rx)
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

    pub fn add_player(&mut self, player: Player){
        self.players.push(player);
    }

    pub fn start(&mut self){
        self.status = GameStatus::STARTED;
        self.start_bid_consumer();
    }

    pub fn end(&mut self) -> Player{
        let evaluator = Evaluator::default();
        let winner = evaluator.get_winner(self.state);

        self.status = GameStatus::FINISHED;

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

    pub async fn sell(&self) -> Result<(), String>{
        let mut current_bid = self.state.current_bid.lock().await;
        match current_bid.take(){
            Some(bid) => {
                drop(current_bid); // Release the lock before calling another async function
                self.state.add_cricketer_to_a_team(bid.player.username().clone(), bid.cricketer, bid.price).await;
                Ok(())
            },
            None => {
                Err("cannot sell with no current bids".to_string())
            }
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