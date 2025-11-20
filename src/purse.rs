#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Purse{
    cash: u64
}

impl Purse{
    pub fn new(cash: u64) -> Self {
        Self{
            cash
        }
    }

    pub fn spend(&mut self, cash: u64){
        self.cash -= cash;
    }

    pub fn cash(&self) -> u64 {
        self.cash
    }
}
