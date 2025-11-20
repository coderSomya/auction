pub struct Ai;


impl Evaluate for Ai{
    pub async fn get_winner(&self, game: GameState) -> Winner{
        let prompt = "take this game state and return the result in the following format.
        {
        'winner': 'name of the player with the strongest team',
        'reason': 'a description backing the choice'
        }
        ";

        let respose = self.query(prompt).await;
        let Ok(winner) = serde_json::from_str::<Winner>(response) else{
            panic!("wtf ai");
        }

        winner
    }
}