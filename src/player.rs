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

    pub fn join_game(game_id: String){

    }
}
