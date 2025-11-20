pub fn Lot{
    cricketers: Vec<Cricketer>,
}

impl Lot{
    pub fn new(cricketers: Vec<Cricketer>) -> Self{
        Self{
            cricketers
        }
    }

    pub fn cricketers(&self) -> &Vec<Cricketer> {
        &self.cricketers
    }

    pub fn load_cricketers(&mut self){
        self.cricketers = load_cricketers();
    }

    pub fn bring_next_cricketer(&mut self) -> Cricketer{
        self.cricketers.remove(0)
    }

    pub fn randomize(&mut self){
        self.cricketers.shuffle(&mut thread_rng());
    }
}