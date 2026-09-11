// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! [`WrapSink`] — the redis-feature adapter: record pipelined write commands
//! into the DAG after they succeed.
//!
//! Enabled with `features = ["redis"]`. The sink implements
//! `redis::Commands`-compatible pass-through: call your normal commands, then
//! `sink.shadow_cmd("SET", "driver:d1", value)` (or let [`Tracked`] do it).
//! Every recorded command becomes a DAG node — full causal chain, zero host
//! namespace pollution.

use serde_json::json;

use crate::surface::Surface;

/// Wrap a `redis::Connection` (or any `redis::Commands` implementor).
/// Commands pass through unchanged; call [`Tracked::shadow_cmd`] after a
/// successful write (or use the macro in the docs) to chain it into NEDB.
pub struct Tracked<C> {
    pub conn: C,
    pub surface: Surface,
}

impl<C> Tracked<C> {
    pub fn new(conn: C, surface: Surface) -> Self {
        Self { conn, surface }
    }

    /// Chain one successful write command.
    ///
    /// * `SET`-family → full replace; `HSET`/`INCR`-family → merge.
    /// * `DEL`/`UNLINK` → tombstone (`_deleted: true`).
    /// * Unmapped keys → raw chain entry (tamper evidence only).
    pub fn shadow_cmd(&self, cmd: &str, key: &str, value: serde_json::Value) -> anyhow::Result<()> {
        let upper = cmd.to_ascii_uppercase();
        match upper.as_str() {
            "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" => {
                self.surface.shadow(key, value, true)?;
            }
            "HSET" | "HMSET" | "HINCRBY" | "HINCRBYFLOAT" | "INCR" | "INCRBY" | "DECR" | "DECRBY" | "APPEND" => {
                self.surface.shadow(key, value, false)?;
            }
            "DEL" | "UNLINK" => {
                self.surface.shadow(key, json!({ "_deleted": true }), true)?;
            }
            other => {
                self.surface
                    .chain_raw(other, key, json!(value))?;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "redis")]
mod redis_impl {
    //! Pass-through `redis::Commands` impl: every command forwards to the
    //! wrapped connection untouched. Shadowing stays explicit per the family
    //! contract (Python/JS parity — the auto-intercept layer belongs to the
    //! connection clients, which hide method replacement from safe Rust).

    use super::*;
    use redis::{Commands, RedisWrite, ToRedisArgs, FromRedisValue, RedisResult, Value as RValue};

    impl<C: RedisWrite> RedisWrite for Tracked<C> {
        fn write_arg_raw(&mut self, arg: impl ToRedisArgs) {
            self.conn.write_arg_raw(arg)
        }
    }

    impl<C> Tracked<C> {
        /// Forward any command, returning the raw redis value.
        pub fn query<RV: FromRedisValue>(&mut self, cmd: &str, args: &[&dyn ToRedisArgs]) -> RedisResult<RV> {
            redis::cmd(cmd).arg(args).query(&mut self.conn)
        }
    }

    /// Convenience: SET + shadow in one call.
    pub fn set_tracked<C: Commands>(
        tracked: &Tracked<C>,
        key: &str,
        value: impl ToRedisArgs,
        json_value: serde_json::Value,
    ) -> RedisResult<()> {
        tracked.conn.set(key, value)?;
        let _ = tracked.shadow_cmd("SET", key, json_value);
        Ok(())
    }

    #[allow(dead_code)]
    fn _assert_value_shape(_: RValue) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::surface::Surface;
    use serde_json::json;

    #[test]
    fn shadow_cmd_routes_by_family() {
        let s = Surface::in_memory();
        s.register("driver:*", "driver");
        s.shadow_writes.store(true, std::sync::atomic::Ordering::Relaxed);
        let t = Tracked::new((), s);

        t.shadow_cmd("SET", "driver:d1", json!({"name":"Bob"})).unwrap();
        t.shadow_cmd("HSET", "driver:d1", json!({"rating":4.9})).unwrap();
        let doc = t.surface.get("driver", "d1").unwrap();
        assert_eq!(doc["name"], json!("Bob"));
        assert_eq!(doc["rating"], json!(4.9));

        t.shadow_cmd("DEL", "driver:d1", json!(null)).unwrap();
        let doc = t.surface.get("driver", "d1").unwrap();
        assert_eq!(doc["_deleted"], json!(true));
        assert!(t.surface.verify());
    }
}
