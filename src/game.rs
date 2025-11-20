use crate::player::Player;
use std::collections::HashMap;
use std::vec::Vec;
use std::time::{SystemTime, UNIX_EPOCH};


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
    teams: HashMap<String, Vec<Buy>>,
    // cricketer, price, option<player>
    current_bid: Option<(String, u64, Option<Player>)>
}

impl GameState{
    pub fn new() -> Self{
        Self{
            teams: HashMap::new(),
            current_bid: None
        }
    }

    pub fn add_cricketer_to_a_team(&mut self, player: String, cricketer: String, price: u64){
        let buy = Buy{
            cricketer,
            price
        };

        let mut buys_of_this_player = self.teams.get(&player);
        if let Some(buys_of_this_player) = buys_of_this_player{
            buys_of_this_player.push(buy);
        }else{
            buys_of_this_player = Vec::new();
        }
        self.teams.insert(player, buys_of_this_player);
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

    fn update_bid(&mut self, bid: Bid){
        self.state.current_bid = bid;
    }

    pub fn try_update_bid(&mut self, bid: Bid){
        if self.is_bid_valid(bid) {
            update_bid(bid);
        }
    }

    pub fn sell(&mut self) -> Result<(), String>{
        match self.state.current_bid{
            Some(bid) => {
                self.state.add_cricketer_to_a_team(bid.player, bid.cricketer, bid.price);
            },
            None => {
                Err("cannot sell with no current bids".to_string());
            }
        }

        self.state.current_bid = None;

        Ok(())
    }

    pub fn is_bid_valid(&self, bid: Bid) -> bool{
        unimplemented!();
    }
}