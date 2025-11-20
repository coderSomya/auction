#[derive(Serialize, Deserialize)]
pub struct Winner{
    player: Player,
    reason: String
}

pub trait Evaluate{
    pub async fn get_winner(game: GameState) -> Winner;
}

pub struct Evaluator;

impl Evaluator{
    pub fn new() -> Self{
        Self{}
    }
}