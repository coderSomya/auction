pub struct Random;


impl Evaluate for Random{
    pub async fn get_winner(&self, game: GameState) -> Winner{

        let players = game.players;
        let random_index = get_random_num()%players.len();
        let random_player = players[random_index];

        let reason = "Random Selection";

        let winner = Winner{
            player: random_player,
            reason: reason
        }
        winner
    }
}