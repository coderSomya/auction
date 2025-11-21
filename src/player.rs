#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub struct Player{
    username: String,
    password: String,
    email: String,
    pub coins: u64
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

    pub fn with_coins(username: String, password: String, email: String, coins: u64) -> Self{
        Self{
            username,
            password,
            email,
            coins
        }
    }

    pub fn add_coins(&mut self, coins: u64){
        self.coins += coins;
    }

    pub fn username(&self) -> &String {
        &self.username
    }

    pub fn join_game(&mut self, game: &mut crate::game::Game, initial_purse: u64){
        self.coins -= initial_purse;
        game.add_team(self.clone(), initial_purse);
    }

    pub async fn exit_game(&mut self, game: &mut crate::game::Game){
        // Get money left from the team's purse
        let money_left = if let Some(team) = game.teams().iter().find(|t| t.player.username() == self.username()) {
            team.purse.cash()
        } else {
            0
        };
        game.remove(self).await;
        self.coins += money_left;
    }

    pub fn place_bid(&self, game: &crate::game::Game, cricketer: String, price: u64) -> Result<(), tokio::sync::mpsc::error::SendError<crate::game::Bid>> {
        // Send bid to the game's bid channel
        game.send_bid_to_channel(self.clone(), cricketer, price)
    }
}
