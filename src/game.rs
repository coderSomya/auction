use crate::player::Player;
use std::collections::HashMap;
use std::vec::Vec;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;


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
struct Bid{
    // for whom the bid is going on
    cricketer: String,
    // against whom the bid is going on
    price: u64,
    // the player who has made the last bid
    player: Player,
    // the time when this bid was made
    timestamp: u64
}

struct GameState{
    // player -> {cricketer, price}
    teams: Mutex<HashMap<String, Vec<Buy>>>,
    // current bid for the cricketer being auctioned
    current_bid: Mutex<Option<Bid>>
}

impl GameState{
    pub fn new() -> Self{
        Self{
            teams: Mutex::new(HashMap::new()),
            current_bid: Mutex::new(None)
        }
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

struct Game{
    game_id: String,
    players: Vec<Player>,
    status: GameStatus,
    state: GameState
}

impl Game{
    pub fn new(&self, creator: Player) -> Self{
        Self{
            game_id: generate_random_id(),
            players: vec![creator],
            status: GameStatus::CREATED,
            state: GameState::new()
        }
    }

    pub fn add_player(&mut self, player: Player){
        self.players.push(player);
    }

    pub fn start(&mut self){
        self.status = GameStatus::STARTED;
    }

    pub fn end(&mut self) -> Player{
        let evaluator = Evaluator::default();
        let winner = evaluator.get_winner(self.state);

        self.status = GameStatus::FINISHED;

        winner
    }

    async fn update_bid(&self, bid: Bid){
        let mut current_bid = self.state.current_bid.lock().await;
        *current_bid = Some(bid);
    }

    pub async fn try_update_bid(&self, bid: Bid){
        if self.is_bid_valid(bid.clone()).await {
            self.update_bid(bid).await;
        }
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

    pub async fn is_bid_valid(&self, bid: Bid) -> bool{
        let current_bid = self.state.current_bid.lock().await;
        match current_bid.as_ref() {
            Some(current_bid) => {
                bid.price > current_bid.price
            },
            None => {
                true // First bid is always valid
            }
        }
    }
}