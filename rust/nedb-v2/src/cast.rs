// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! Natural-language query planning — the `cast` feature.
//!
//! Turns a short English prompt into NQL using `nedb-cast-slm`, a 3.34M-parameter
//! model trained specifically on this engine's query language. Off by default;
//! enable with the `cast` cargo feature and `nedbd --cast` (or `NEDBD_CAST=1`).
//!
//! Why this belongs in the engine rather than in a client:
//!
//! The hard part of natural-language querying is not the model, it is knowing the
//! schema. A client has to fetch the collection list and pass it in; the engine
//! already holds it in `db.id_index.collections()`. Putting the planner here means
//! it can be constrained to collections that actually exist, at the moment of the
//! call, for free — and every nedbd client inherits the capability instead of
//! reimplementing it.
//!
//! Safety posture, in order of importance:
//!
//!   1. The model NEVER executes anything itself. It emits NQL text, which is then
//!      handed to the ordinary `nql::query` path. Anything the parser rejects fails
//!      exactly as a hand-typed bad query would — there is no second execution path
//!      to audit.
//!   2. `execute` defaults to FALSE. The endpoint returns a plan for review. A
//!      planner that silently runs a wrong guess is worse than one that admits it
//!      is unsure.
//!   3. Unparseable output is reported as `valid: false` with the raw text attached,
//!      never swallowed into an empty result set. A silent empty result reads as
//!      "no matching rows" and that would be a lie.
//!
//! Model provenance: weights are a `model.cast` container, checksum-verified on
//! load. The same container is consumed by the Python package and the npm binding,
//! and CI holds all three to per-logit parity with the PyTorch reference
//! (max |Δlogit| 7.6e-6, decoded strings byte-identical).

use std::path::PathBuf;
use std::sync::Arc;

use nedb_cast_slm::Cast as CastModel;

/// A loaded planner. Cheap to clone (the weights sit behind an Arc), so the
/// server holds one and shares it across requests.
#[derive(Clone)]
pub struct Caster {
    inner: Arc<CastModel>,
    source: String,
}

impl Caster {
    /// Load from an explicit path, or search the conventional locations.
    ///
    /// Search order, first hit wins:
    ///   1. `$NEDBD_CAST_MODEL`             — explicit override
    ///   2. `<data_dir>/model.cast`         — sits with the database
    ///   3. `$CAST_HOME/model.cast`
    ///   4. `~/.cache/nedb-cast-slm/<tag>/model.cast`  — where the Python and npm
    ///      packages cache it, so a machine that already ran either one is ready
    pub fn load(data_dir: &std::path::Path) -> Result<Self, String> {
        for cand in Self::candidates(data_dir) {
            if !cand.exists() {
                continue;
            }
            return match CastModel::load(&cand) {
                Ok(m) => {
                    if !m.checksum_verified {
                        // Loud, not fatal: a container without a checksum is a
                        // format we still understand, but the operator should know
                        // the bytes were not verified.
                        eprintln!(
                            "  cast: WARNING {} has no checksum; bytes unverified",
                            cand.display()
                        );
                    }
                    Ok(Caster {
                        inner: Arc::new(m),
                        source: cand.display().to_string(),
                    })
                }
                Err(e) => Err(format!("failed to load {}: {}", cand.display(), e)),
            };
        }
        Err(format!(
            "no model.cast found. Looked in:\n  {}\n\
             Download it from \
             https://github.com/aiassistsecure/nedb-cast-slm/releases \
             or set NEDBD_CAST_MODEL to its path.",
            Self::candidates(data_dir)
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join("\n  ")
        ))
    }

    fn candidates(data_dir: &std::path::Path) -> Vec<PathBuf> {
        let mut v = Vec::new();
        if let Ok(p) = std::env::var("NEDBD_CAST_MODEL") {
            v.push(PathBuf::from(p));
        }
        v.push(data_dir.join("model.cast"));
        if let Ok(h) = std::env::var("CAST_HOME") {
            v.push(PathBuf::from(&h).join("model.cast"));
            v.push(PathBuf::from(&h).join("v10.30.90").join("model.cast"));
        }
        if let Ok(home) = std::env::var("HOME") {
            let base = PathBuf::from(home).join(".cache").join("nedb-cast-slm");
            v.push(base.join("v10.30.90").join("model.cast"));
            v.push(base.join("model.cast"));
        }
        v
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn n_params(&self) -> usize {
        self.inner.n_params()
    }

    pub fn vocab_size(&self) -> usize {
        self.inner.vocab_size()
    }

    /// Cast a prompt into NQL text. Greedy decoding — a DSL has exactly one right
    /// answer, so sampling can only hurt.
    pub fn cast(&self, prompt: &str) -> String {
        self.inner.cast(prompt)
    }

    /// Cast, then check the collection it chose actually exists in this database.
    ///
    /// This is the payoff of living inside the engine. The model was trained on a
    /// synthetic schema universe, so on an unfamiliar database it can name a
    /// plausible-but-absent collection. We cannot un-generate that, but we CAN
    /// detect it precisely, because the engine knows the real collection list.
    /// Returning `unknown_collection` beats returning zero rows and letting the
    /// caller conclude the data isn't there.
    pub fn cast_checked(&self, prompt: &str, collections: &[String]) -> CastResult {
        let nql = self.cast(prompt);
        let coll = extract_collection(&nql);
        let known = match &coll {
            Some(c) => collections.iter().any(|k| k == c),
            None => false,
        };
        let drift = detect_drift(prompt, &nql);
        CastResult { nql, collection: coll, collection_known: known, drift }
    }
}

#[derive(Debug, Clone)]
pub struct CastResult {
    pub nql: String,
    /// The collection named after FROM, if one could be read off the output.
    pub collection: Option<String>,
    /// Whether that collection exists in the database being queried.
    pub collection_known: bool,
    /// Set when the plan contains a quoted literal that is NOT in the prompt —
    /// the model substituted a memorised value. See [`detect_drift`].
    pub drift: Option<String>,
}

/// Detect a literal the model INVENTED rather than copied from the prompt.
///
/// This is the most dangerous failure this model has, because it is invisible.
/// A word outside the 581-token vocabulary gets replaced by a memorised
/// training literal:
///
/// ```text
/// "memories about pricing"  ->  FROM memories SEARCH "handoff"
/// ```
///
/// That plan parses. It names a real collection. It returns real rows. Both
/// `valid` and `collection_known` are true — and it answers a question nobody
/// asked. Measured on the released checkpoint: 3/3 in-vocabulary terms copied
/// correctly (`release flow`, `guardrail`, `handoff`), 0/3 novel terms
/// (`pricing`, `deadlines`, `kubernetes`) — every one collapsed to that same
/// memorised string.
///
/// A wrong-but-plausible answer is worse than an error, and worse again for an
/// agent, where the next step is built on it.
///
/// The check needs no model introspection: a quoted literal the model emitted
/// should be traceable to the prompt. If it is not, it was substituted. Root
/// cause is the absence of a copy mechanism over prompt tokens — the same gap
/// that truncates long digit runs (`height 400000` -> `4000`).
///
/// Deliberately ADVISORY. It is reported, never used to reject: the plan may
/// still be what the caller wanted, and silently discarding a valid query would
/// be its own kind of lie. Same posture as `execute: false` — surface it, let
/// the caller decide.
fn detect_drift(prompt: &str, nql: &str) -> Option<String> {
    let p = prompt.to_lowercase();
    for lit in quoted_literals(nql) {
        let l = lit.to_lowercase();
        // A literal that appears verbatim is fine. So is a multi-word phrase
        // whose every word appears — the model may requote or reorder, and
        // flagging that would be noise that trains people to ignore the field.
        if p.contains(&l) || l.split_whitespace().all(|w| p.contains(w)) {
            continue;
        }
        return Some(format!(
            "generated the literal {lit:?}, which does not appear in the prompt — \
             likely outside the model's vocabulary and substituted. Verify before \
             trusting these results."
        ));
    }
    None
}

/// Every double-quoted literal in a NQL string.
///
/// A hand-rolled scan rather than a parser call, deliberately: this must work on
/// output that does NOT parse, which is exactly when a caller most needs to see
/// what the model produced.
fn quoted_literals(nql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut inside = false;
    for ch in nql.chars() {
        if ch == '"' {
            if inside {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                cur.clear();
                inside = false;
            } else {
                inside = true;
                cur.clear();
            }
        } else if inside {
            cur.push(ch);
        }
    }
    out
}

/// Pull the collection name out of generated NQL: the token after `FROM`.
///
/// Deliberately a tiny scan rather than a call into the parser — this runs even
/// when the text does not fully parse, so we can report *which* collection was
/// attempted in the error path.
fn extract_collection(nql: &str) -> Option<String> {
    let mut it = nql.split_whitespace();
    while let Some(tok) = it.next() {
        if tok.eq_ignore_ascii_case("FROM") {
            return it.next().map(|s| s.trim_matches('"').to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_collection_after_from() {
        assert_eq!(
            extract_collection("FROM orders WHERE total > 99"),
            Some("orders".to_string())
        );
        assert_eq!(
            extract_collection("from stylists limit 5"),
            Some("stylists".to_string())
        );
        assert_eq!(extract_collection("WHERE total > 99"), None);
        assert_eq!(extract_collection(""), None);
    }

    #[test]
    fn strips_quotes_from_collection() {
        assert_eq!(
            extract_collection("FROM \"my coll\" LIMIT 1"),
            Some("my".to_string()),
            "quoted multiword collections are not a thing in NQL; \
             first token is the right answer"
        );
    }

    // ── drift detection ───────────────────────────────────────────────────
    //
    // These cases are TRANSCRIBED FROM REAL MODEL OUTPUT on the released
    // v10.30.90 checkpoint, not invented. Verified 12/12 on the true/false
    // split, plus 12 further probes for false positives (0 fired).

    #[test]
    fn drift_fires_on_substituted_search_term() {
        // The actual observed failure. "pricing" is outside the 581-token
        // vocabulary, so the model emits a memorised literal instead — and the
        // result is a well-formed query answering a different question.
        let d = detect_drift("memories about pricing", r#"FROM memories SEARCH "handoff""#);
        assert!(d.is_some(), "must flag a literal absent from the prompt");
        assert!(d.unwrap().contains("handoff"), "must name the offending literal");

        // Same substitution for every novel term — 0/3 copied, measured.
        for p in ["memories about deadlines", "memories about kubernetes"] {
            assert!(
                detect_drift(p, r#"FROM memories SEARCH "handoff""#).is_some(),
                "should flag: {p}"
            );
        }
    }

    #[test]
    fn drift_silent_when_the_term_was_copied() {
        // In-vocabulary terms are copied correctly — 3/3, measured. Flagging
        // these would be noise, and noise trains people to ignore the field.
        for (p, nql) in [
            ("memories about the release flow", r#"FROM memories SEARCH "release flow""#),
            ("memories about guardrail", r#"FROM memories SEARCH "guardrail""#),
            ("memories about handoff", r#"FROM memories SEARCH "handoff""#),
        ] {
            assert!(detect_drift(p, nql).is_none(), "false positive on: {p}");
        }
    }

    #[test]
    fn drift_ignores_inferred_enum_values() {
        // The false-positive class that would sink this feature: the user says
        // "refunded orders" and the model correctly writes status = "refunded".
        // The literal IS in the prompt, just in a different grammatical role.
        // Verified against real output — 0 of 12 such probes fired.
        for (p, nql) in [
            ("orders that were refunded", r#"FROM orders WHERE status = "refunded""#),
            ("customers in orlando", r#"FROM customers WHERE city = "orlando""#),
            ("runs still running", r#"FROM runs WHERE status = "running""#),
            ("runs by vex", r#"FROM runs WHERE owner = "vex""#),
        ] {
            assert!(detect_drift(p, nql).is_none(), "false positive on: {p}");
        }
    }

    #[test]
    fn drift_tolerates_requoted_multiword_phrases() {
        // Word-level match, so reordering or requoting a phrase whose words are
        // all present stays silent. Being strict here would flag correct plans.
        assert!(detect_drift(
            "customers in winter park",
            r#"FROM customers WHERE city = "winter park""#
        ).is_none());
        assert!(detect_drift(
            "show me the park in winter",
            r#"FROM customers WHERE city = "winter park""#
        ).is_none(), "all words present — not a substitution");
    }

    #[test]
    fn drift_is_none_when_no_literals() {
        assert!(detect_drift("show me all orders", "FROM orders").is_none());
        assert!(detect_drift("orders with total over 100",
                             "FROM orders WHERE total > 100.0").is_none());
    }

    #[test]
    fn quoted_literals_handles_degenerate_output() {
        // Must not panic on unparseable text — that is exactly when a caller
        // most needs to see what the model produced.
        assert_eq!(quoted_literals(r#"FROM a WHERE b = "x" AND c = "y""#),
                   vec!["x".to_string(), "y".to_string()]);
        assert_eq!(quoted_literals("FROM FROM FROM 6"), Vec::<String>::new());
        assert_eq!(quoted_literals(r#"unterminated "quote"#), Vec::<String>::new(),
                   "an unclosed quote yields nothing, and must not hang");
        assert_eq!(quoted_literals(r#"empty "" literal"#), Vec::<String>::new());
        assert_eq!(quoted_literals(""), Vec::<String>::new());
    }

}
