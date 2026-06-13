//! MVCC versioned store with time-travel. Each key holds an ascending vector of
//! `(seq, Option<Value>)` versions (`None` is a tombstone). Reads at HEAD take the
//! last version; `as_of` reads take the newest version with `seq <= as_of`.

use std::collections::HashMap;

use serde_json::Value;

#[derive(Default)]
pub struct MvccStore {
    v: HashMap<String, Vec<(u64, Option<Value>)>>,
}

impl MvccStore {
    pub fn put(&mut self, key: &str, val: Value, seq: u64) {
        self.v.entry(key.to_string()).or_default().push((seq, Some(val)));
    }

    pub fn delete(&mut self, key: &str, seq: u64) {
        self.v.entry(key.to_string()).or_default().push((seq, None));
    }

    pub fn get(&self, key: &str, as_of: Option<u64>) -> Option<&Value> {
        let chain = self.v.get(key)?;
        let idx = match as_of {
            None => chain.len() - 1,
            Some(s) => {
                // versions are appended in ascending seq order → binary search
                let mut lo = 0usize;
                let mut hi = chain.len();
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    if chain[mid].0 <= s {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                if lo == 0 {
                    return None;
                }
                lo - 1
            }
        };
        chain[idx].1.as_ref()
    }

    pub fn keys(&self, prefix: &str) -> Vec<&String> {
        self.v
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| self.get(k, None).is_some())
            .collect()
    }
}
