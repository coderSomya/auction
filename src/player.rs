#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Player{
    username: String,
    password: String,
    email: String,
    coins: u64
}

impl Player{
    pub fn new(username: String, password: String, email: String) -> Self{
        Self{
            username,
            password, 
            email,
            coins: 0
        }
    }

    pub fn username(&self) -> &String {
        &self.username
    }

    pub fn join_game(&self, game: &mut crate::game::Game, initial_purse: u64){
        game.add_team(self.clone(), initial_purse);
    }

    pub fn place_bid(&self, game: &crate::game::Game, cricketer: String, price: u64) -> Result<(), tokio::sync::mpsc::error::SendError<crate::game::Bid>> {
        // Send bid to the game's bid channel
        game.send_bid_to_channel(self.clone(), cricketer, price)
    }
}
