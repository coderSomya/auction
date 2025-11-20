use scylla::{Session, SessionBuilder, query::Query};
use std::sync::Arc;
use scylla::frame::value::ValueList;
use scylla::FromRow;
use crate::DB;

// T -> type of data (Player, Game, etc.)
// K -> key type (String, UUID, etc)
pub struct ScyllaDB<T, K> {
    session: Arc<Session>,
    table: &'static str,
    insert_query: Query,
    select_query: Query,
    delete_query: Query,
    _phantom: std::marker::PhantomData<(T, K)>,
}

impl<T, K> ScyllaDB<T, K> {

    pub async fn new(
        uri: &str,
        table: &'static str,
        insert: &str,
        select: &str,
        delete: &str,
    ) -> anyhow::Result<Self> {
        let session = SessionBuilder::new()
            .known_node(uri)
            .build()
            .await?;
        
        Ok(Self {
            session: Arc::new(session),
            table,
            insert_query: Query::new(insert.to_string()),
            select_query: Query::new(select.to_string()),
            delete_query: Query::new(delete.to_string()),
            _phantom: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl<T, K> DB<T, K> for ScyallDB<T, K>
where
    T: FromRow + Sync + Send,
    K: ValueList + Sync + Send,
    T: ValueList,
{
    async fn insert(&self, value: &T) -> anyhow::Result<()> {
        self.session
            .execute(&self.insert_query, value)
            .await?;
        Ok(())
    }

    async fn get(&self, key: &K) -> anyhow::Result<Option<T>> {
        let rows = self.session
            .execute(&self.select_query, key)
            .await?
            .rows;

        let Some(rows) = rows else { return Ok(None); };
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows[0].clone();
        let parsed = T::from_row(row)?;
        Ok(Some(parsed))
    }

    async fn delete(&self, key: &K) -> anyhow::Result<()> {
        self.session
            .execute(&self.delete_query, key)
            .await?;
        Ok(())
    }
}


#[tokio::test]
pub async fn test_scylla(){
    use scylla::{FromRow};
    use uuid::Uuid;
    use serde::{Serialize, Deserialize};

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct User {
        pub id: Uuid,
        pub name: String,
        pub age: i32,
    }

    let repo = ScyllaDB::<User, (Uuid,)>::new(
        "127.0.0.1:9042",
        "users",
        "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
        "SELECT id, name, age FROM users WHERE id = ?",
        "DELETE FROM users WHERE id = ?"
    ).await?;

    let user = User {
        id: Uuid::new_v4(),
        name: "Alice".into(),
        age: 30,
    };

    // insert
    repo.insert(&user).await?;

    // get
    let fetched = repo.get(&(user.id,)).await?;
    println!("Fetched: {:?}", fetched);

    // delete
    repo.delete(&(user.id,)).await?;

}