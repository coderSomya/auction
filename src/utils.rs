use crate::game::Game;
use std::fs::File;
use std::io::BufReader;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Cricketer{
    pub name: String,
    pub price: u64
}

#[derive(Deserialize)]
struct CricketersData {
    data: HashMap<String, u64>
}

pub fn get_random_num() -> u64{
    42
}

pub fn get_winning_amount(game: &Game) -> u64{
    game.bank()/2
}

pub fn load_cricketers() -> Vec<Cricketer>{
    let file = File::open("src/cricketers.json").unwrap();
    let reader = BufReader::new(file);
    let cricketers_data: CricketersData = serde_json::from_reader(reader).unwrap();
    
    cricketers_data.data
        .into_iter()
        .map(|(name, price)| Cricketer { name, price })
        .collect()
}