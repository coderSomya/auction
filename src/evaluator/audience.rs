use crate::evaluator::{Evaluate, Winner};
use crate::game::Game;

pub struct Audience;

#[async_trait::async_trait]
impl Evaluate for Audience {
    async fn get_winner(&self, _game: &Game) -> Winner {
        // TODO: Implement audience-based evaluation
        // For now, return first player
        let teams = _game.teams();
        let player = if teams.is_empty() {
            crate::player::Player::new("".to_string(), "".to_string(), "".to_string())
        } else {
            teams[0].player.clone()
        };
        
        Winner {
            player,
            reason: "Audience vote".to_string(),
        }
    }
}