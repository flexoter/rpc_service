// #[path = "storage.rs"] mod storage;
use crate::storage::Storage;

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

    pub async fn get_data_by_key(&self, key: String) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let c = Arc::clone(&self.cache);
        let st = Arc::clone(&self.storage);

        let cache = c.lock().await;

        // Let's check if we have the key in the cache
        if let Some(data) = cache.get(&key) {
            return Ok(Vec::from([data.clone()]));
        }

        let storage = st.lock().await;

        // If we don't we grab it from the storage
        storage.get_data_by_key(&key).await
    }
}