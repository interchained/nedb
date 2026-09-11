// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

/**
 * The PUBLISHED npm client — `nedb-engine-client` — against a real nedbd.
 *
 * This suite did not exist. `client/node` is published to npm by the release
 * workflow on every `v*` tag, and its only verification was `npx tsc` — that
 * it COMPILES. Not one request was ever made against a server before the
 * package shipped, which is the same publish-without-verification gap the test
 * workflow closed for the engine in 3.2.x.
 *
 * What shipped unverified, found by writing this:
 *
 *   - `get()` built `FROM coll WHERE _id = "<id>"` by interpolation, so any id
 *     containing a double quote was UNREACHABLE — the call returned null,
 *     meaning "no such document", for a document `put()` had stored and
 *     `query("FROM coll")` returned.
 *   - `delete()` interpolated the id into the URL path unencoded, so an id
 *     containing a slash matched a different route and the call returned false
 *     — "did not exist" — for a document that did.
 *
 * Both now go through `GET/DELETE /v1/databases/<db>/rows/<coll>/<id>` with the
 * id percent-encoded into the path.
 *
 * Run from client/node:  npx tsc && node test/client.test.mjs
 * The compiled dist/index.js is what gets published, so that is what is
 * imported here — testing the source would not prove the artifact works.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import { createServer } from "node:net";
import test from "node:test";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const CLIENT_ROOT = path.resolve(HERE, "..");
const REPO_ROOT = path.resolve(CLIENT_ROOT, "..", "..");

const { NedbClient } = await import(path.join(CLIENT_ROOT, "dist", "index.js"));

function freePort() {
  return new Promise((resolve, reject) => {
    const s = createServer();
    s.once("error", reject);
    s.listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

async function waitReady(port, tries = 60) {
  for (let i = 0; i < tries; i++) {
    await new Promise((r) => setTimeout(r, 250));
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      if (res.ok) return true;
    } catch {
      /* not up yet */
    }
  }
  return false;
}

// Ids that are perfectly legal to store but historically broke the client.
const TRICKY = [
  "plain",
  'has"quote',
  'x" LIMIT 99 OR _id = "y',
  'a"b"c',
  "a/slash",
  "sp ace",
  "100%",
  "uni✓code",
  "back\\slash",
  "ends\\",
];

const dataDir = mkdtempSync(path.join(tmpdir(), "nedb-nodeclient-"));
const port = await freePort();

// The Python AOF server — always available, needs no platform wheel.
const daemon = spawn(
  process.env.PYTHON ?? "python3",
  ["-m", "nedb.server", "--host", "127.0.0.1", "--port", String(port),
   "--data", path.join(dataDir, "data")],
  {
    stdio: "ignore",
    env: { ...process.env, NEDBD_SWEEP_S: "0",
           PYTHONPATH: path.join(REPO_ROOT, "python") },
  },
);

// unref so the child never holds Node's event loop open. Without this the
// suite passes every assertion and then hangs forever with a live ProcessWrap.
daemon.unref();

const up = await waitReady(port);
if (!up) {
  daemon.kill();
  rmSync(dataDir, { recursive: true, force: true });
  throw new Error("nedbd never came up");
}

function teardown() {
  try { daemon.kill(); } catch { /* already gone */ }
  rmSync(dataDir, { recursive: true, force: true });
}
// The net, for an early abort or a thrown assertion.
process.on("exit", teardown);

const db = new NedbClient({ url: `http://127.0.0.1:${port}`, db: "nodepkg" });


test("transport: health and database lifecycle", async () => {
  const h = await db.health();
  assert.equal(h.ok, true);
  assert.ok(h.version, "health reports a version");
  await db.createDatabase();
  const names = await db.listDatabases();
  assert.ok(JSON.stringify(names).includes("nodepkg"), "the database is listed");
});

test("put / get / query round-trip", async () => {
  const res = await db.put("t", "u1", { name: "Alice", n: 1 });
  assert.equal(res.ok, true);
  const got = await db.get("t", "u1");
  assert.ok(got, "get returned a document");
  assert.equal(got.name, "Alice");
  assert.equal(got._id, "u1");
  assert.ok("_seq" in got, "the document carries _seq");
  assert.equal(await db.get("t", "nope"), null, "an absent id is null");
  const rows = await db.query("FROM t");
  assert.equal(rows.length, 1);
});

// THE BUG. Every one of these ids stores fine and comes back from
// `FROM ids`, but get() could not fetch the ones containing a quote.
test("every legal id is reachable by get()", async () => {
  for (const id of TRICKY) await db.put("ids", id, { marker: id });

  const stored = new Set((await db.query("FROM ids")).map((r) => r._id));
  for (const id of TRICKY) {
    assert.ok(stored.has(id), `id ${JSON.stringify(id)} was stored`);
  }

  for (const id of TRICKY) {
    const got = await db.get("ids", id);
    assert.ok(got !== null,
      `get(${JSON.stringify(id)}) returned null for a document that exists`);
    assert.equal(got.marker, id, `get(${JSON.stringify(id)}) round-trips`);
  }
});

test("a crafted id cannot widen the result set", async () => {
  const crafted = await db.get("ids", 'zzz" OR _id = "u1');
  assert.equal(crafted, null, "a crafted id names a document or nothing");
});

test("delete reaches ids needing URL encoding", async () => {
  for (const id of ["a/slash", "sp ace", "100%", 'has"quote']) {
    const existed = await db.delete("ids", id);
    assert.equal(existed, true,
      `delete(${JSON.stringify(id)}) returned false for a document that exists`);
    assert.equal(await db.get("ids", id), null, `${JSON.stringify(id)} is gone`);
  }
  assert.equal(await db.delete("ids", "nope"), false, "an absent id is false");
});

test("the 3.3.0 query surface rides through unchanged", async () => {
  const seed = [
    ["1", { status: "open", fee: 10, miner: "Acme" }],
    ["2", { status: "pending", fee: 20, miner: "acme solo" }],
    ["3", { status: "closed", fee: 30, miner: "Zenith" }],
    ["4", { status: "open", fee: 40 }],
  ];
  for (const [id, doc] of seed) await db.put("jobs", id, doc);

  const ids = async (nql) =>
    (await db.query(nql)).map((r) => r._id).sort();

  const cases = [
    ['FROM jobs WHERE status IN ("open","closed")', ["1", "3", "4"]],
    ['FROM jobs WHERE status NOT IN ("open")', ["2", "3"]],
    ["FROM jobs WHERE fee BETWEEN 20 AND 40", ["2", "3", "4"]],
    ['FROM jobs WHERE miner LIKE "Acme%"', ["1"]],
    ['FROM jobs WHERE miner ILIKE "acme%"', ["1", "2"]],
    ["FROM jobs WHERE miner IS NULL", ["4"]],
    ["FROM jobs WHERE fee = 10 OR fee = 40", ["1", "4"]],
    ['FROM jobs WHERE (fee = 10 OR fee = 40) AND status = "open"', ["1", "4"]],
    ["FROM jobs WHERE NOT (fee > 20)", ["1", "2"]],
    ["FROM jobs ORDER BY fee OFFSET 2", ["3", "4"]],
  ];
  for (const [nql, want] of cases) {
    assert.deepEqual(await ids(nql), want, nql);
  }

  const agg = await db.query(
    "FROM jobs GROUP BY status SUM fee HAVING sum_fee > 20");
  assert.deepEqual(agg.map((r) => r.status).sort(), ["closed", "open"]);

  const bare = await db.query("FROM jobs COUNT");
  assert.equal(bare.length, 1, "a bare aggregate is one row");
  assert.equal(bare[0].count, 4);
});

test("a misspelled clause does not silently answer", async () => {
  let rejected = false;
  try {
    const rows = await db.query("FROM jobs ORDRE BY fee");
    rejected = rows.length === 0;
  } catch {
    rejected = true;
  }
  assert.ok(rejected, "an unhonourable query must not return rows");
});

test("AS OF on a single document", async () => {
  await db.put("tt", "d", { v: 1 });
  const first = await db.get("tt", "d");
  await db.put("tt", "d", { v: 2 });
  assert.equal((await db.get("tt", "d")).v, 2, "current version");
  const old = await db.get("tt", "d", first._seq);
  assert.ok(old, "as_of returned a row");
  assert.equal(old.v, 1, "historical version");
});

test("integrity surfaces", async () => {
  const v = await db.verify();
  assert.ok(v === true || v.ok === true, `verify: ${JSON.stringify(v)}`);
  assert.ok(await db.head(), "head is a hash");
  assert.equal(typeof (await db.seq()), "number");
});

test("batch applies every op", async () => {
  await db.batch([
    { op: "put", coll: "b", id: "x", doc: { n: 1 } },
    { op: "put", coll: "b", id: "y", doc: { n: 2 } },
  ]);
  const full = await db.queryFull("FROM b");
  assert.equal(full.count, 2);
});

// Teardown as the LAST test rather than an after-hook: the ordering is
// explicit, it shows up in the report, and it does not depend on hook
// semantics. `process.on("exit")` remains as the net for an early abort.
test("teardown: the daemon stops and the temp dir is removed", () => {
  teardown();
  assert.ok(true);
});
