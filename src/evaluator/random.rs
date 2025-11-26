use crate::evaluator::{Evaluate, Winner};
use crate::game::Game;
use crate::utils::get_random_num;

pub struct Random;

#[async_trait::async_trait]
impl Evaluate for Random {
    async fn get_winner(&self, game: &Game) -> Winner {
        let teams = game.teams();
        
        if teams.is_empty() {
            // Return a dummy winner if no teams
            return Winner {
                player: crate::player::Player::new("".to_string(), "".to_string(), "".to_string()),
                reason: "No players in game".to_string(),
            };
        }

        let random_index = (get_random_num() as usize) % teams.len();
        let random_player = teams[random_index].player.clone();

        Winner {
            player: random_player,
            reason: "Random Selection".to_string(),
        }
    }
}