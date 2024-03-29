// #[path = "storage.rs"] mod storage;
use crate::storage::Storage;

use std::io;
use std::{collections::HashMap, error::Error};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Executor implements internal logic of rpc requests
///
/// Supports two kind of requests:
///     - storing data with key and data, both text types;
///     - retreiving data with a specific key.
///
/// Internally utilyzes underlying db storage and hashmap as
/// key-value storage for inserting/retreiving data.
pub struct Executor {
    storage: Arc<Mutex<Storage>>,
    cache: Arc<Mutex<HashMap<String, String>>>,
}

impl Executor {
    pub fn new(storage: Storage) -> Self {
        Executor{
            storage: Arc::new(Mutex::new(storage)),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn store_data_by_key(&self, key: String, data: String) -> Result<(), Box<dyn Error>> {
        let c = self.cache.clone();
        let st = self.storage.clone();

        let mut cache = c.lock().await;

        cache.insert(key.clone(), data.clone());

        let storage = st.lock().await;

        storage.store_data_by_key(&key, &data).await
    }

    pub async fn get_cached_data_by_key(&self, key: String) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let c = Arc::clone(&self.cache);
        let cache = c.lock().await;

        match cache.get(&key) {
            Some(data) => {
                return Ok(Vec::from([data.clone()]));
            }
            None => {
                return Err(Box::new(io::Error::new(io::ErrorKind::Other, "Executor: no such key in cache")));
            }
        }
    }

    pub async fn get_data_by_key(&self, key: String) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let st = Arc::clone(&self.storage);

        let storage = st.lock().await;

        storage.get_data_by_key(&key).await
    }
}