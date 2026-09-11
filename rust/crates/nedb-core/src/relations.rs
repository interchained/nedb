// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

// nedb-core — first-class, time-travel-aware relations (the graph layer).
//
// Relations are stored as adjacency lists for O(1) traversal. Each edge records
// the seq at which it was added and optionally removed, so traversal can also be
// asked "AS OF" any past sequence — the graph time-travels just like the records do.

use std::collections::HashMap;

/// A single directed edge: target node key, seq added, seq removed (None = live).
#[derive(Clone, Debug)]
pub struct Edge {
    pub to:      String,
    pub added:   u64,
    pub removed: Option<u64>,
}

/// (frm_key, relation) → list of edges
type Adj = HashMap<(String, String), Vec<Edge>>;

#[derive(Default)]
pub struct Relations {
    adj:  Adj,   // forward edges
    radj: Adj,   // reverse edges (to, rel) → [frm, ...]  stored as Edge{to: frm, ...}
}

fn live(edges: &[Edge], as_of: Option<u64>) -> Vec<String> {
    edges
        .iter()
        .filter(|e| match as_of {
            None => e.removed.is_none(),
            Some(s) => e.added <= s && e.removed.map_or(true, |r| r > s),
        })
        .map(|e| e.to.clone())
        .collect()
}

impl Relations {
    pub fn link(&mut self, frm: &str, rel: &str, to: &str, seq: u64) {
        // idempotent: skip if edge already live
        let fwd = self.adj.entry((frm.to_string(), rel.to_string())).or_default();
        if fwd.iter().any(|e| e.to == to && e.removed.is_none()) {
            return;
        }
        fwd.push(Edge { to: to.to_string(), added: seq, removed: None });
        self.radj
            .entry((to.to_string(), rel.to_string()))
            .or_default()
            .push(Edge { to: frm.to_string(), added: seq, removed: None });
    }

    pub fn unlink(&mut self, frm: &str, rel: &str, to: &str, seq: u64) {
        if let Some(fwd) = self.adj.get_mut(&(frm.to_string(), rel.to_string())) {
            for e in fwd.iter_mut() {
                if e.to == to && e.removed.is_none() {
                    e.removed = Some(seq);
                }
            }
        }
        if let Some(rev) = self.radj.get_mut(&(to.to_string(), rel.to_string())) {
            for e in rev.iter_mut() {
                if e.to == frm && e.removed.is_none() {
                    e.removed = Some(seq);
                }
            }
        }
    }

    pub fn neighbors(&self, frm: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        self.adj
            .get(&(frm.to_string(), rel.to_string()))
            .map(|edges| live(edges, as_of))
            .unwrap_or_default()
    }

    pub fn inbound(&self, to: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        self.radj
            .get(&(to.to_string(), rel.to_string()))
            .map(|edges| live(edges, as_of))
            .unwrap_or_default()
    }

    /// All (frm_key, rel, to_key) triples that are currently live. Used by nedbd to
    /// populate the schema graph without a full Studio deploy.
    pub fn all_live_edges(&self) -> Vec<(String, String, String)> {
        let mut out = Vec::new();
        for ((frm, rel), edges) in &self.adj {
            for e in edges {
                if e.removed.is_none() {
                    out.push((frm.clone(), rel.clone(), e.to.clone()));
                }
            }
        }
        out
    }
}
