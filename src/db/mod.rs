pub mod scylla;
pub mod cockroach;

use async_trait::async_trait;

#[async_trait]
pub trait DB<T, K> {
    async fn insert(&self, value: &T) -> anyhow::Result<()>;
    async fn get(&self, key: &K) -> anyhow::Result<Option<T>>;
    async fn delete(&self, key: &K) -> anyhow::Result<()>;
}