//! DAG edge store — typed directed edges between node hashes.
//!
//! Layout: `graph/{from_hash}/{edge_type}/{to_hash}`
//! Existence of the file = the edge exists. No file content needed.
//!
//! This makes TRACE queries pure filesystem traversal:
//!   FROM nodes TRACE caused_by → list dir graph/{hash}/caused_by/
//!   Each entry is the hash of a causal predecessor node.
//!   Follow recursively until limit reached or no more edges.
//!
//! Write is atomic (create file). Read is readdir. Both are O(degree).
//! No global lock. Multiple threads can add edges concurrently.

use std::fs;
use std::path::{Path, PathBuf};
use anyhow::Result;

pub struct GraphStore {
    root: PathBuf,
}

impl GraphStore {
    pub fn new(db_root: &Path) -> Result<Self> {
        let root = db_root.join("graph");
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn edge_path(&self, from: &str, edge_type: &str, to: &str) -> PathBuf {
        self.root.join(from).join(edge_type).join(to)
    }

    /// Add a directed edge: from → to with the given type label.
    pub fn add_edge(&self, from: &str, edge_type: &str, to: &str) -> Result<()> {
        let path = self.edge_path(from, edge_type, to);
        fs::create_dir_all(path.parent().unwrap())?;
        // Atomic: O_CREAT | O_EXCL is atomic on POSIX for a new file
        if !path.exists() {
            fs::write(&path, b"")?;
        }
        Ok(())
    }

    /// Get all outgoing edges of a given type from a node.
    pub fn outgoing(&self, from: &str, edge_type: &str) -> Vec<String> {
        let dir = self.root.join(from).join(edge_type);
        fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect()
    }

    /// Get all incoming edges of a given type to a node (reverse lookup).
    /// This requires scanning the `{reverse_edge_type}` edges stored when writing.
    pub fn incoming(&self, to: &str, reverse_edge_type: &str) -> Vec<String> {
        self.outgoing(to, reverse_edge_type)
    }

    /// TRACE: walk the DAG from `start` following `edge_type` edges.
    /// Returns hashes in BFS order, up to `limit`.
    pub fn trace(
        &self,
        start: &str,
        edge_type: &str,
        reverse: bool,
        limit: usize,
    ) -> Vec<String> {
        let mut result = Vec::new();
        let mut queue  = vec![start.to_string()];
        let mut seen   = std::collections::HashSet::new();
        seen.insert(start.to_string());

        while !queue.is_empty() && result.len() < limit {
            let current = queue.remove(0);
            result.push(current.clone());

            let next_hashes = if reverse {
                // "reverse" means traverse the reverse-edge (e.g. "caused" instead of "caused_by")
                let rev_type = format!("{}_rev", edge_type);
                self.outgoing(&current, &rev_type)
            } else {
                self.outgoing(&current, edge_type)
            };

            for next in next_hashes {
                if !seen.contains(&next) {
                    seen.insert(next.clone());
                    queue.push(next);
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn add_and_traverse_edge() {
        let dir = tempdir().unwrap();
        let g = GraphStore::new(dir.path()).unwrap();

        g.add_edge("hash_c", "caused_by", "hash_b").unwrap();
        g.add_edge("hash_b", "caused_by", "hash_a").unwrap();

        let trace = g.trace("hash_c", "caused_by", false, 10);
        assert_eq!(trace, vec!["hash_c", "hash_b", "hash_a"]);
    }

    #[test]
    fn idempotent_edge() {
        let dir = tempdir().unwrap();
        let g = GraphStore::new(dir.path()).unwrap();
        g.add_edge("a", "caused_by", "b").unwrap();
        g.add_edge("a", "caused_by", "b").unwrap();   // second call is no-op
        assert_eq!(g.outgoing("a", "caused_by").len(), 1);
    }
}
