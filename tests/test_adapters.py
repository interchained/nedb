# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""Tests for the SQL adapter, Redis compat layer, and auto-indexer."""
from __future__ import annotations
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

from nedb import NEDB, AutoIndexDB
from nedb.sql import sql_exec, sql_to_nql, SQLUnsupportedError
from nedb.redis_compat import RedisCompat, RedisUnsupportedError
from nedb.mongo import MongoCompat, MongoUnsupportedError, ObjectId

PASS = FAIL = 0
def check(name, cond):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ok  {name}")
    else:    FAIL += 1; print(f"  FAIL {name}")

def fresh():
    db = NEDB()
    db.create_index("users", "status", "eq")
    db.create_index("users", "age",    "ordered")
    db.create_index("users", "bio",    "search")
    db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active",   "bio": "rust db"})
    db.put("users", "bob",   {"name": "Bob",   "age": 24, "status": "active",   "bio": "python data"})
    db.put("users", "carol", {"name": "Carol", "age": 41, "status": "inactive", "bio": "rust systems"})
    return db

# ─── SQL adapter ──────────────────────────────────────────────────────────────
print("\n── SQL adapter ──")

def test_sql_to_nql_simple():
    nql = sql_to_nql("SELECT * FROM users WHERE status = 'active' ORDER BY age DESC LIMIT 5")
    check("sql_to_nql basic", "FROM users" in nql and 'status = "active"' in nql and "ORDER BY age DESC" in nql and "LIMIT 5" in nql)

def test_sql_select():
    db = fresh()
    rows = sql_exec(db, "SELECT * FROM users WHERE status = 'active' ORDER BY age ASC")
    check("SELECT WHERE ORDER BY", len(rows) == 2 and rows[0]["name"] == "Bob")

def test_sql_select_like():
    db = fresh()
    rows = sql_exec(db, "SELECT * FROM users WHERE bio LIKE '%rust%'")
    names = sorted(r["name"] for r in rows)
    check("SELECT LIKE → SEARCH", names == ["Alice", "Carol"])

def test_sql_select_limit():
    db = fresh()
    rows = sql_exec(db, "SELECT * FROM users LIMIT 1")
    check("SELECT LIMIT", len(rows) == 1)

def test_sql_insert():
    db = fresh()
    sql_exec(db, "INSERT INTO users (id, name, age, status, bio) VALUES ('dave', 'Dave', 28, 'active', 'go dev')")
    check("INSERT", db.get("users", "dave")["name"] == "Dave")

def test_sql_update():
    db = fresh()
    sql_exec(db, "UPDATE users SET age = 99 WHERE id = 'alice'")
    check("UPDATE", db.get("users", "alice")["age"] == 99)
    check("UPDATE preserves other fields", db.get("users", "alice")["name"] == "Alice")

def test_sql_delete():
    db = fresh()
    sql_exec(db, "DELETE FROM users WHERE id = 'alice'")
    check("DELETE", db.get("users", "alice") is None)

def test_sql_or_raises():
    from nedb.sql import SQLError as _SE
    try:
        sql_to_nql("SELECT * FROM users WHERE status = 'active' OR age > 30")
        check("OR raises error", False)
    except (SQLUnsupportedError, _SE):
        check("OR raises error", True)

def test_sql_as_of():
    db = fresh()
    snap = db.seq
    db.put("users", "alice", {"name": "Alice", "age": 55, "status": "active"})
    rows = sql_exec(db, f"SELECT * FROM users AS OF {snap} WHERE status = 'active'")
    check("SELECT AS OF (time-travel)", all(r["age"] != 55 for r in rows if r["_id"] == "alice"))

for fn in [test_sql_to_nql_simple, test_sql_select, test_sql_select_like,
           test_sql_select_limit, test_sql_insert, test_sql_update,
           test_sql_delete, test_sql_or_raises, test_sql_as_of]:
    fn()

# ─── Redis compat ─────────────────────────────────────────────────────────────
print("\n── Redis adapter ──")

def test_redis_ping():
    r = RedisCompat(fresh()); check("PING", r.execute("PING") == "PONG")

def test_redis_set_get():
    r = RedisCompat(fresh())
    r.execute("SET", "k", "hello")
    check("SET/GET", r.execute("GET", "k") == "hello")

def test_redis_del():
    r = RedisCompat(fresh())
    r.execute("SET", "k", "v")
    r.execute("DEL", "k")
    check("DEL", r.execute("GET", "k") is None)

def test_redis_exists():
    r = RedisCompat(fresh())
    r.execute("SET", "k", "v")
    check("EXISTS yes", r.execute("EXISTS", "k") == 1)
    check("EXISTS no",  r.execute("EXISTS", "missing") == 0)

def test_redis_incr():
    r = RedisCompat(fresh())
    r.execute("SET", "counter", "0")
    check("INCR", r.execute("INCR", "counter") == 1)
    check("INCRBY", r.execute("INCRBY", "counter", 9) == 10)
    check("DECR",  r.execute("DECR", "counter") == 9)

def test_redis_setnx():
    r = RedisCompat(fresh())
    check("SETNX new", r.execute("SETNX", "k", "v") == 1)
    check("SETNX dup", r.execute("SETNX", "k", "v2") == 0)
    check("SETNX kept", r.execute("GET", "k") == "v")

def test_redis_mset_mget():
    r = RedisCompat(fresh())
    r.execute("MSET", "a", "1", "b", "2")
    check("MGET", r.execute("MGET", "a", "b", "missing") == ["1", "2", None])

def test_redis_hash():
    r = RedisCompat(fresh())
    r.execute("HSET", "user:1", "name", "Ada", "age", "31")
    check("HGET", r.execute("HGET", "user:1", "name") == "Ada")
    check("HGETALL", r.execute("HGETALL", "user:1") == {"name": "Ada", "age": "31"})
    check("HLEN", r.execute("HLEN", "user:1") == 2)
    check("HEXISTS", r.execute("HEXISTS", "user:1", "name") == 1)
    r.execute("HDEL", "user:1", "age")
    check("HDEL", r.execute("HGET", "user:1", "age") is None)
    check("HINCRBY", r.execute("HINCRBY", "user:1", "score", 5) == 5)

def test_redis_set():
    r = RedisCompat(fresh())
    r.execute("SADD", "tags", "python", "rust", "go")
    check("SMEMBERS", r.execute("SMEMBERS", "tags") == {"python", "rust", "go"})
    check("SISMEMBER yes", r.execute("SISMEMBER", "tags", "rust") == 1)
    check("SISMEMBER no",  r.execute("SISMEMBER", "tags", "java") == 0)
    check("SCARD", r.execute("SCARD", "tags") == 3)
    r.execute("SREM", "tags", "go")
    check("SREM", r.execute("SCARD", "tags") == 2)

def test_redis_list():
    r = RedisCompat(fresh())
    r.execute("RPUSH", "q", "a", "b", "c")
    check("LRANGE", r.execute("LRANGE", "q", 0, -1) == ["a", "b", "c"])
    check("LLEN", r.execute("LLEN", "q") == 3)
    check("LINDEX", r.execute("LINDEX", "q", 1) == "b")
    check("LPOP", r.execute("LPOP", "q") == "a")
    check("RPOP", r.execute("RPOP", "q") == "c")

def test_redis_unsupported():
    r = RedisCompat(fresh())
    for cmd in ("EXPIRE", "TTL", "SUBSCRIBE", "PUBLISH", "MULTI", "EXEC"):
        try:
            r.execute(cmd, "k", "60")
            check(f"{cmd} raises", False)
        except RedisUnsupportedError:
            check(f"{cmd} → UNSUPPORTED", True)

for fn in [test_redis_ping, test_redis_set_get, test_redis_del, test_redis_exists,
           test_redis_incr, test_redis_setnx, test_redis_mset_mget, test_redis_hash,
           test_redis_set, test_redis_list, test_redis_unsupported]:
    fn()

# ─── MongoDB compat ───────────────────────────────────────────────────────────
print("\n── MongoDB adapter ──")

def mongo_seeded():
    m = MongoCompat(NEDB())
    users = m["users"]
    users.insert_many([
        {"_id": "alice", "name": "Alice", "age": 31, "status": "active",   "tags": ["rust", "db"]},
        {"_id": "bob",   "name": "Bob",   "age": 24, "status": "active",   "tags": ["python"]},
        {"_id": "carol", "name": "Carol", "age": 41, "status": "inactive", "tags": ["rust", "ops"]},
    ])
    return m, users

def test_mongo_insert_objectid():
    m = MongoCompat(NEDB())
    res = m["t"].insert_one({"x": 1})
    check("insert_one returns ObjectId", isinstance(res.inserted_id, str) and len(res.inserted_id) == 24)
    check("ObjectId() shape", len(ObjectId()) == 24)

def test_mongo_insert_many_find():
    _, users = mongo_seeded()
    check("insert_many + count", users.count_documents({}) == 3)
    check("find_one by _id", users.find_one({"_id": "alice"})["name"] == "Alice")

def test_mongo_find_operators():
    _, users = mongo_seeded()
    over_30 = list(users.find({"age": {"$gt": 30}}))
    check("$gt", sorted(u["name"] for u in over_30) == ["Alice", "Carol"])
    active = list(users.find({"status": "active"}))
    check("implicit eq", len(active) == 2)
    inlist = list(users.find({"age": {"$in": [24, 41]}}))
    check("$in", sorted(u["name"] for u in inlist) == ["Bob", "Carol"])
    ne = list(users.find({"status": {"$ne": "active"}}))
    check("$ne", len(ne) == 1 and ne[0]["name"] == "Carol")

def test_mongo_find_logical():
    _, users = mongo_seeded()
    res = list(users.find({"$or": [{"age": {"$lt": 25}}, {"status": "inactive"}]}))
    check("$or", sorted(u["name"] for u in res) == ["Bob", "Carol"])
    res2 = list(users.find({"$and": [{"status": "active"}, {"age": {"$gt": 30}}]}))
    check("$and", len(res2) == 1 and res2[0]["name"] == "Alice")

def test_mongo_array_match():
    _, users = mongo_seeded()
    rusty = list(users.find({"tags": "rust"}))
    check("array membership eq", sorted(u["name"] for u in rusty) == ["Alice", "Carol"])

def test_mongo_regex_exists():
    _, users = mongo_seeded()
    res = list(users.find({"name": {"$regex": "^A"}}))
    check("$regex", len(res) == 1 and res[0]["name"] == "Alice")
    res2 = list(users.find({"tags": {"$exists": True}}))
    check("$exists", len(res2) == 3)

def test_mongo_sort_skip_limit():
    _, users = mongo_seeded()
    names = [u["name"] for u in users.find({}).sort("age", -1)]
    check("sort desc", names == ["Carol", "Alice", "Bob"])
    page = [u["name"] for u in users.find({}).sort("age", 1).skip(1).limit(1)]
    check("skip+limit", page == ["Alice"])

def test_mongo_projection():
    _, users = mongo_seeded()
    doc = users.find_one({"_id": "alice"}, {"name": 1})
    check("projection include", set(doc.keys()) == {"_id", "name"})
    doc2 = users.find_one({"_id": "alice"}, {"tags": 0, "_id": 0})
    check("projection exclude", "tags" not in doc2 and "_id" not in doc2 and "name" in doc2)

def test_mongo_update_operators():
    _, users = mongo_seeded()
    r = users.update_one({"_id": "alice"}, {"$set": {"age": 32}, "$inc": {"logins": 1}})
    check("update_one matched", r.matched_count == 1 and r.modified_count == 1)
    a = users.find_one({"_id": "alice"})
    check("$set + $inc", a["age"] == 32 and a["logins"] == 1)
    users.update_one({"_id": "alice"}, {"$push": {"tags": "systems"}})
    check("$push", "systems" in users.find_one({"_id": "alice"})["tags"])
    users.update_one({"_id": "alice"}, {"$addToSet": {"tags": "rust"}})
    check("$addToSet no dup", users.find_one({"_id": "alice"})["tags"].count("rust") == 1)
    users.update_one({"_id": "alice"}, {"$unset": {"logins": ""}})
    check("$unset", "logins" not in users.find_one({"_id": "alice"}))

def test_mongo_update_many():
    _, users = mongo_seeded()
    r = users.update_many({"status": "active"}, {"$set": {"seen": True}})
    check("update_many", r.modified_count == 2)
    check("update_many applied", users.count_documents({"seen": True}) == 2)

def test_mongo_upsert():
    _, users = mongo_seeded()
    r = users.update_one({"_id": "dave"}, {"$set": {"name": "Dave", "age": 50}}, upsert=True)
    check("upsert id", r.upserted_id == "dave")
    check("upsert wrote doc", users.find_one({"_id": "dave"})["name"] == "Dave")

def test_mongo_replace():
    _, users = mongo_seeded()
    users.replace_one({"_id": "bob"}, {"name": "Bobby", "age": 25})
    b = users.find_one({"_id": "bob"})
    check("replace_one", b["name"] == "Bobby" and "status" not in b and b["_id"] == "bob")

def test_mongo_delete():
    _, users = mongo_seeded()
    check("delete_one", users.delete_one({"_id": "alice"}).deleted_count == 1)
    check("delete_one gone", users.find_one({"_id": "alice"}) is None)
    check("delete_many", users.delete_many({"status": "active"}).deleted_count == 1)

def test_mongo_distinct():
    _, users = mongo_seeded()
    check("distinct", sorted(users.distinct("status")) == ["active", "inactive"])
    check("distinct over arrays", sorted(users.distinct("tags")) == ["db", "ops", "python", "rust"])

def test_mongo_aggregate():
    _, users = mongo_seeded()
    grouped = users.aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}, "avg_age": {"$avg": "$age"}}}])
    by = {g["_id"]: g for g in grouped}
    check("$group $sum", by["active"]["n"] == 2)
    check("$group $avg", abs(by["active"]["avg_age"] - 27.5) < 1e-9)
    pipe = users.aggregate([
        {"$match": {"status": "active"}},
        {"$sort": {"age": -1}},
        {"$limit": 1},
    ])
    check("aggregate $match/$sort/$limit", len(pipe) == 1 and pipe[0]["name"] == "Alice")
    cnt = users.aggregate([{"$match": {"status": "active"}}, {"$count": "total"}])
    check("$count", cnt == [{"total": 2}])

def test_mongo_index_acceleration():
    db = NEDB()
    db.create_index("users", "status", "eq")
    m = MongoCompat(db)["users"]
    m.insert_many([{"_id": str(i), "status": "active" if i % 2 else "idle", "n": i} for i in range(50)])
    res = list(m.find({"status": "active"}))
    check("indexed find correct", len(res) == 25 and all(r["status"] == "active" for r in res))

def test_mongo_persistence_replay():
    import tempfile, shutil
    tmp = tempfile.mkdtemp()
    try:
        db = NEDB(tmp)
        MongoCompat(db)["items"].insert_one({"_id": "x1", "name": "Widget"})
        db.close()
        db2 = NEDB(tmp)
        check("mongo write survives reopen", MongoCompat(db2)["items"].find_one({"_id": "x1"})["name"] == "Widget")
        check("reopened db verifies", db2.verify())
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

def test_mongo_unsupported():
    _, users = mongo_seeded()
    try:
        list(users.find({"x": {"$where": "this.x > 1"}}))
        check("$where raises", False)
    except MongoUnsupportedError:
        check("$where → UNSUPPORTED", True)
    try:
        users.aggregate([{"$lookup": {}}])
        check("$lookup raises", False)
    except MongoUnsupportedError:
        check("$lookup → UNSUPPORTED", True)

for fn in [test_mongo_insert_objectid, test_mongo_insert_many_find, test_mongo_find_operators,
           test_mongo_find_logical, test_mongo_array_match, test_mongo_regex_exists,
           test_mongo_sort_skip_limit, test_mongo_projection, test_mongo_update_operators,
           test_mongo_update_many, test_mongo_upsert, test_mongo_replace, test_mongo_delete,
           test_mongo_distinct, test_mongo_aggregate, test_mongo_index_acceleration,
           test_mongo_persistence_replay, test_mongo_unsupported]:
    fn()

# ─── AutoIndexDB ──────────────────────────────────────────────────────────────
print("\n── AutoIndexDB ──")

def test_autoindex():
    db = AutoIndexDB(NEDB(), threshold=3, verbose=False)
    db.put("items", "1", {"name": "A", "status": "active"})
    db.put("items", "2", {"name": "B", "status": "archived"})
    # Query 3 times — threshold reached on the 3rd
    for _ in range(3):
        db.query('FROM items WHERE status = "active"')
    check("autoindex created eq index", ("items", "status", "eq") in db._created)
    rows = db.query('FROM items WHERE status = "active"')
    check("autoindex query still correct", len(rows) == 1 and rows[0]["name"] == "A")
    s = db.suggest()
    check("suggest returns string list", isinstance(s, list))
    a = db.analyze()
    check("analyze returns tallies", "tallies" in a and "indexes_created" in a)

test_autoindex()

# ─── Summary ──────────────────────────────────────────────────────────────────
print(f"\nAdapters: {PASS} passed, {FAIL} failed {'✅' if not FAIL else '❌'}")
sys.exit(1 if FAIL else 0)
