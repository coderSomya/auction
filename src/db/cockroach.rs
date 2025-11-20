use sqlx::{Pool, Postgres, postgres::PgPoolOptions, FromRow};
use std::sync::Arc;
use async_trait::async_trait;
use crate::DB;

// T -> type (User, Player, Game, etc)
// K -> key tuple (Uuid,), (String,), etc.
pub struct CockroachDB<T, K> {
    pool: Arc<Pool<Postgres>>,
    table: &'static str,
    insert_query: &'static str,
    select_query: &'static str,
    delete_query: &'static str,
    _phantom: std::marker::PhantomData<(T, K)>,
}

impl<T, K> CockroachDB<T, K> {
    pub async fn new(
        uri: &str,
        table: &'static str,
        insert: &'static str,
        select: &'static str,
        delete: &'static str,
    ) -> anyhow::Result<Self> {

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(uri)
            .await?;

        Ok(Self {
            pool: Arc::new(pool),
            table,
            insert_query: insert,
            select_query: select,
            delete_query: delete,
            _phantom: Default::default(),
        })
    }
}


#[async_trait]
impl<T, K> DB<T, K> for CockroachDB<T, K>
where
    T: for<'r> FromRow<'r, sqlx::postgres::PgRow> + Send + Sync + Unpin,
    K: sqlx::Encode<'static, Postgres> + sqlx::Type<Postgres> + Send + Sync + Clone,
{
    async fn insert(&self, value: &T) -> anyhow::Result<()> {
        sqlx::query(self.insert_query)
            .bind(value)
            .execute(&*self.pool)
            .await?;
        Ok(())
    }

    async fn get(&self, key: &K) -> anyhow::Result<Option<T>> {
        let row = sqlx::query_as::<_, T>(self.select_query)
            .bind(key.clone())
            .fetch_optional(&*self.pool)
            .await?;

        Ok(row)
    }

    async fn delete(&self, key: &K) -> anyhow::Result<()> {
        sqlx::query(self.delete_query)
            .bind(key.clone())
            .execute(&*self.pool)
            .await?;
        Ok(())
    }
}


#[tokio::test]
async fn test_cockroach() -> anyhow::Result<()> {
    use serde::{Serialize, Deserialize};
    use sqlx::FromRow;
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
    pub struct User {
        pub id: Uuid,
        pub name: String,
        pub age: i32,
    }

    let repo = CockroachDB::<User, Uuid>::new(
        "postgres://root@localhost:26257/defaultdb?sslmode=disable",
        "users",
        // inserts bind individually (SQLx limitation)
        "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
        "SELECT id, name, age FROM users WHERE id = $1",
        "DELETE FROM users WHERE id = $1"
    ).await?;

    let user = User {
        id: Uuid::new_v4(),
        name: "Alice".into(),
        age: 30,
    };

    // Insert (explicit binds)
    sqlx::query(repo.insert_query)
        .bind(user.id)
        .bind(&user.name)
        .bind(user.age)
        .execute(&*repo.pool)
        .await?;

    // Get
    let fetched = sqlx::query_as::<_, User>(repo.select_query)
        .bind(user.id)
        .fetch_one(&*repo.pool)
        .await?;

    println!("Fetched: {:?}", fetched);

    // Delete
    sqlx::query(repo.delete_query)
        .bind(user.id)
        .execute(&*repo.pool)
        .await?;

    Ok(())
}
