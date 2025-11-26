use crate::game::Game;
use crate::player::Player;
use serde::{Serialize, Deserialize};

pub mod random;
pub mod ai;
pub mod audience;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Winner{
    pub player: Player,
    pub reason: String
}

#[async_trait::async_trait]
pub trait Evaluate: Send + Sync {
    async fn get_winner(&self, game: &Game) -> Winner;
}

pub struct Evaluator {
    evaluator: Box<dyn Evaluate>,
}

impl Evaluator {
    pub fn new() -> Self {
        Self {
            evaluator: Box::new(random::Random),
        }
    }

    pub async fn get_winner(&self, game: &Game) -> Winner {
        self.evaluator.get_winner(game).await
    }
}