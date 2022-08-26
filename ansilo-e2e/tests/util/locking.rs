use std::{
    fs,
    marker::PhantomData,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

const CACHE_DIR: &'static str = "/tmp/ansilo-e2e-cache/";

/// Booting certain external systems for integration testing can be slow and painful.
/// Often we reduce this pain by keeping them alive with an idle timeout while they are still in use.
///
/// This struct allows functions to check if they have been run recently, and if so cache their outputs
/// and return then for future invocations
pub struct FunctionCache<T> {
    key: String,
    duration: Duration,
    _d: PhantomData<T>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Cached<T> {
    expires: u64,
    val: T,
}

impl<T> FunctionCache<T>
where
    T: DeserializeOwned,
    T: Serialize,
    T: Clone,
{
    pub fn new(key: impl Into<String>, duration: Duration) -> Self {
        Self {
            key: key.into(),
            duration,
            _d: PhantomData,
        }
    }

    pub fn valid(&mut self) -> Option<T> {
        let path = self.path();

        let data = fs::read_to_string(path).ok()?;
        let item: Cached<T> = serde_json::from_str(&data).ok()?;

        let timestamp = timestamp();

        if item.expires > timestamp {
            Some(item.val)
        } else {
            None
        }
    }

    pub fn extend(&mut self) {
        let val = self.valid().expect("Cache item is not valid");
        self.save(&val);
    }

    pub fn save(&mut self, val: &T) {
        let path = self.path();

        let expires = timestamp() + self.duration.as_secs();
        let item = Cached { val, expires };

        let json = serde_json::to_string(&item).unwrap();
        
        let _ = fs::create_dir_all(path.parent().unwrap());
        fs::write(path, json).unwrap();
    }

    fn path(&self) -> PathBuf {
        return Path::new(CACHE_DIR).join(&self.key);
    }
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
