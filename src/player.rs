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

    pub fn bid(&self, game: Game, cricketer: String, price: u64){
        let bid = Bid{
            cricketer: cricketer,
            price: price,
            player: self,
            timestamp: get_current_timestamp()
        };

        game.try_update_bid(bid);
    }
}
