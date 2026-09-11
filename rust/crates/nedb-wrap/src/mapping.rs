// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! Key/table pattern → NEDB collection mapping (glob), shared by all adapters.

/// Maps a host key pattern (glob, e.g. `driver:*`) to a NEDB collection.
#[derive(Debug, Clone)]
pub struct CollectionMapping {
    pub pattern: String,
    pub collection: String,
    regex: regex_lite::Glob,
}

impl CollectionMapping {
    pub fn new(pattern: impl Into<String>, collection: impl Into<String>) -> Self {
        let pattern = pattern.into();
        let regex = regex_lite::Glob::new(&pattern);
        Self { pattern, collection: collection.into(), regex }
    }

    pub fn matches(&self, key: &str) -> bool {
        self.regex.is_match(key)
    }

    /// Default id extraction: the segment after the last `:` (Python/JS parity).
    pub fn extract_id<'a>(&self, key: &'a str) -> &'a str {
        key.rsplit(':').next().unwrap_or(key)
    }
}

/// Tiny zero-dependency glob (`*` and `?`) — enough for key patterns without
/// pulling the `regex` crate into every build.
mod regex_lite {
    /// A parsed glob: Any (run of `*`), Any1 (`?`), or a literal char.
    #[derive(Debug, Clone, PartialEq)]
    enum Tok {
        Any,
        Any1,
        Lit(char),
    }

    #[derive(Debug, Clone)]
    pub struct Glob {
        toks: Vec<Tok>,
    }
    impl Glob {
        pub fn new(pattern: &str) -> Self {
            let mut toks = Vec::with_capacity(pattern.len());
            for c in pattern.chars() {
                match c {
                    '*' => {
                        // collapse consecutive stars
                        if toks.last() != Some(&Tok::Any) {
                            toks.push(Tok::Any);
                        }
                    }
                    '?' => toks.push(Tok::Any1),
                    c => toks.push(Tok::Lit(c)),
                }
            }
            Self { toks }
        }
        pub fn is_match(&self, s: &str) -> bool {
            let txt: Vec<char> = s.chars().collect();
            glob_rec(&self.toks, 0, &txt, 0)
        }
    }

    /// Backtracking matcher over glob tokens.
    fn glob_rec(toks: &[Tok], mut ti: usize, txt: &[char], mut xi: usize) -> bool {
        while ti < toks.len() {
            match toks[ti] {
                Tok::Any => {
                    if ti + 1 == toks.len() {
                        return true; // trailing star eats the rest
                    }
                    for k in xi..=txt.len() {
                        if glob_rec(toks, ti + 1, txt, k) {
                            return true;
                        }
                    }
                    return false;
                }
                Tok::Any1 => {
                    if xi >= txt.len() {
                        return false;
                    }
                    ti += 1;
                    xi += 1;
                }
                Tok::Lit(c) => {
                    if xi >= txt.len() || txt[xi] != c {
                        return false;
                    }
                    ti += 1;
                    xi += 1;
                }
            }
        }
        xi == txt.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_basics() {
        assert!(CollectionMapping::new("driver:*", "d").matches("driver:d1"));
        assert!(!CollectionMapping::new("driver:*", "d").matches("trip:t1"));
        // '?' matches exactly ONE char: "z?d" fits "z5d", not "z55d"
        assert!(CollectionMapping::new("trip:z?d", "t").matches("trip:z5d"));
        assert!(!CollectionMapping::new("trip:z?d", "t").matches("trip:z55d"));
        let m = CollectionMapping::new("driver:*", "d");
        assert_eq!(m.extract_id("driver:d1"), "d1");
        assert_eq!(m.extract_id("trip:zone:t1"), "t1");
    }
}
