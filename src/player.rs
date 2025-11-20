#[derive(Clone)]
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

    pub fn join_game(mut game: Game){
        game.add_player(self);
    }

    pub fn bid(&self, game: &crate::game::Game, cricketer: String, price: u64) -> Result<(), tokio::sync::mpsc::error::SendError<crate::game::Bid>> {
        // Send bid to the game's bid channel
        game.send_bid_to_channel(self.clone(), cricketer, price)
    }
}
