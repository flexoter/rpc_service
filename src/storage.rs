use std::{error::Error, io};
use Result::{Ok, Err};

use sqlx::{Any, AnyPool, Executor, Pool, Row};

const STORE_DATA_QUERY_IMPL: &str = r"insert into key_data values ($1, $2)";
const GET_DATA_QUERY_IMPL: &str = r"select data from key_data values where key = $1";

/// Storage type with pool of any connections
///
/// Utilizes Pool<Any> to achieve handling arbitrary db storage.
/// Uses db url to determine which driver to use.
///
/// It's main purpose to serve as a db handler in executor.rs.
///
/// Supports two kind of requests:
///     - storing data with key and data, both text types;
///     - retreiving data with a specific key.
pub struct Storage {
    conn: Pool<Any>
}

impl Storage {
    /// Creates new storage.
    ///
    /// Runs sqlx::any::install_default_drivers() at first to initialize
    /// all the available db drivers.
    pub async fn new(url: &str) -> Result<Self, Box<dyn Error>> {
        sqlx::any::install_default_drivers();

        let conn_res = AnyPool::connect(url).await;
        match conn_res {
            Ok(conn) => {
                let storage = Storage{ conn };

                let r = storage.init_scheme().await;
                if let Err(err) = r {
                    println!("init failed with err {}", err);

                    return Err(Box::new(
                        io::Error::new(io::ErrorKind::Other, "storage init failed")
                    ));
                }

                Ok(storage)
            }
            Err(err) => {
                println!("{}", err);

                Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                "couldn't establish connection with database"
            )))
        }
        }
    }

    /// Method for storing data by key in db.
    pub async fn store_data_by_key(&self, key: &str, data: &str) -> Result<(), Box<dyn Error>> {
        let r = sqlx::query(STORE_DATA_QUERY_IMPL)
        .bind(key)
        .bind(data)
        .execute(&self.conn)
        .await;

        match r {
            Ok(_) => { Ok(()) }
            Err(err) => {
                println!("store data by key {}", err);

                Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                "store data query execution failed"
            ))) }
        }
    }

    /// Method for retreiving data by key in db.
    pub async fn get_data_by_key(&self, key: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let r = sqlx::query(GET_DATA_QUERY_IMPL)
        .bind(key)
        .fetch_all(&self.conn)
        .await;

        if r.is_err() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                "get data query execution failed")
            ));
        }

        let rows = r.unwrap();
        let mut result = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            let r = row.try_get("data");
            match r {
                Ok(data) => { result.push(data) }
                Err(_) => { return Err(
                    Box::new(io::Error::new(
                            io::ErrorKind::Other,
                            "there is no data column in the result",
                        )
                    ))
                }
            }
        }

        Result::Ok(result)
    }

    /// Method for closing underlying db connections.
    ///
    /// It's intended to use in Drop implementation.
    pub async fn close(&self) -> Result<(), Box<dyn Error>> {
        let r = self.drop_scheme().await;
        if let Err(err) = r {
            println!("drop sheme error {}", err);

            return Err(Box::new(io::Error::new(
                io::ErrorKind::Other, "drop scheme failed"
            )));
        }

        self.conn.close().await;

        Ok(())
    }

    /// Init some table to store data.
    pub async fn init_scheme(&self) -> Result<(), Box<dyn Error>> {
        let r = self.conn.execute(
            r#"DROP TABLE IF EXISTS key_data;"#).await;
        if let Err(err) = r {
            return Err(Box::new(
                io::Error::new(io::ErrorKind::Other, format!("drop table, err={}", err))
            ));
        }

        let r = self.conn.execute(
            r#"CREATE TABLE IF NOT EXISTS key_data
            (
                key     TEXT NOT NULL,
                data    TEXT NOT NULL
            );"#).await;
        if let Err(err) = r {
            println!("create table err {}", err);

            return Err(Box::new(
                io::Error::new(io::ErrorKind::Other, format!("create table, err={}", err))
            ));
        }

        let r = self.conn.execute(
            r#"ALTER TABLE key_data OWNER TO postgres;"#).await;
        if let Err(err) = r {
            return Err(Box::new(
                io::Error::new(io::ErrorKind::Other, format!("drop table, err={}", err))
            ));
        }

        Ok(())
    }

    /// Drop table in case we don't need it anymore.
    pub async fn drop_scheme(&self) -> Result<(), Box<dyn Error>> {
        let r = self.conn.execute(
            r#"DROP TABLE IF EXISTS key_data;"#).await;
        if let Err(err) = r {
            return Err(Box::new(
                io::Error::new(io::ErrorKind::Other, format!("drop table failed, err={}", err))
            ));
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_storage_smoke() {
    let db_url = "postgres://postgres:postgrespw@localhost:55000";
    let db = Storage::new(db_url)
        .await
        .expect("failed to create new storage");

    db.close().await.expect("close db failed");
}

#[tokio::test]
async fn test_storate_insert() {
    let db_url = "postgres://postgres:postgrespw@localhost:55000";
    let db = Storage::new(db_url)
        .await
        .expect("failed to create new storage");

    db.store_data_by_key("favorite movie", "Back to the future")
        .await
        .expect("failed to insert data in db");

    let data = db.get_data_by_key("favorite movie")
        .await
        .expect("failed to get data from db");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0], "Back to the future");

    db.store_data_by_key("favorite movie", "007: Skyfall")
        .await
        .expect("failed to insert data in db");

    let data = db.get_data_by_key("favorite movie")
        .await
        .expect("failed to get data from db");
    assert_eq!(data.len(), 2);
    assert_eq!(data[0], "Back to the future");
    assert_eq!(data[1], "007: Skyfall");

    db.close()
        .await
        .expect("close db failed");
}