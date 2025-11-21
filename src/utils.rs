use crate::game::Game;
use std::fs::File;
use std::io::BufReader;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Cricketer{
    name: String,
    price: u64
}

pub fn get_random_num() -> u64{
    42
}

pub fn get_winning_amount(game: &Game) -> u64{
    game.bank()/2
}

pub fn load_cricketers() -> Vec<Cricketer>{
    let file = File::open("cricketers.json").unwrap();
    let readers = BufReader::new(file);
    // return a list of cricketers with their name and price from the json
    Vec::new()
}