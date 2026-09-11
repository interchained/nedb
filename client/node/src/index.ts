// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

/**
 * nedb-client — TypeScript/JavaScript client for the nedbd HTTP API.
 *
 * Works in Node.js (18+) and modern browsers.
 *
 * @example
 * ```ts
 * import { NedbClient } from "nedb-engine-client";
 *
 * const db = new NedbClient({ url: "http://127.0.0.1:7070", db: "mydb" });
 *
 * await db.put("blocks", "618000", { height: 618000, hash: "000abc" });
 * const rows = await db.query("FROM blocks ORDER BY height DESC LIMIT 10");
 * const head = await db.head();
 * ```
 */

// ── Types ──────────────────────────────────────────────────────────────────

export interface NedbClientOptions {
  /** Base URL of the nedbd server. Default: "http://127.0.0.1:7070" */
  url?: string;
  /** Database name. All operations target this database. */
  db: string;
  /** Bearer token (matches NEDBD_TOKEN on the server). */
  token?: string;
  /**
   * Auto-create the database on first write if it doesn't exist.
   * Default: true
   */
  autoCreate?: boolean;
  /**
   * Read timeout in milliseconds (for queries).
   * Default: 3000
   */
  readTimeoutMs?: number;
  /**
   * Write timeout in milliseconds (for puts, deletes, batch).
   * Default: 30000
   */
  writeTimeoutMs?: number;
}

export interface PutOptions {
  /** Object hashes that causally led to this write (DAG provenance). */
  causedBy?: string[];
  /** Bi-temporal valid-from date (ISO 8601). */
  validFrom?: string;
  /** Bi-temporal valid-to date (ISO 8601). */
  validTo?: string;
  /** Human-readable provenance note. */
  evidence?: string;
  /** Confidence score 0–1. */
  confidence?: number;
  /** Idempotency key — duplicate puts with the same key are no-ops. */
  idem?: string;
  /** Replay-protection nonce (monotonically increasing per clientId). */
  nonce?: number;
  /** Client identifier for replay protection. */
  clientId?: string;
}

export interface PutResult {
  ok: boolean;
  doc: Record<string, unknown>;
  seq: number;
  head: string;
}

export interface QueryResult {
  rows: Record<string, unknown>[];
  count: number;
  seq: number;
  head: string;
}

/**
 * Result of a natural-language cast. The interesting part is the PLAN, not the
 * rows — `rows`/`count` appear only when `execute: true` was requested.
 */
export interface CastResult {
  prompt: string;
  /** The generated NQL. Present even on a 422, so you can see what it got wrong. */
  nql: string;
  /** Whether the generated NQL parses — checked by the same parser that executes it. */
  valid: boolean;
  /** The collection named after FROM, if one could be read off the output. */
  collection: string | null;
  /** Whether that collection exists in this database. */
  collection_known: boolean;
  /** Every collection this database actually has — what the plan was checked against. */
  collections: string[];
  executed: boolean;
  seq: number;
  head: string;
  rows?: Record<string, unknown>[];
  count?: number;
  error?: string;
  /**
   * Present when the plan contains a quoted literal that is NOT in the prompt —
   * the model substituted a memorised value instead of copying yours.
   *
   * ```
   * "memories about pricing"  ->  FROM memories SEARCH "handoff"
   * ```
   *
   * The query is valid, the collection exists, rows come back — and it answers
   * a different question. `valid` and `collection_known` cannot catch this, so
   * **check `drift` before acting on results unattended.** Advisory only: the
   * plan may still be what you wanted.
   */
  drift?: string;
}

export interface VerifyResult {
  ok: boolean;
  seq: number;
  head: string;
  tamper_evident: boolean;
  objects_checked: number;
  tampered: string[];
}

export interface HealthResult {
  ok: boolean;
  service: string;
  version: string;
  databases: string[];
  encrypted: boolean;
}

export interface BatchOp {
  op: "put" | "del";
  coll: string;
  id: string;
  doc?: Record<string, unknown>;
  caused_by?: string[];
}

export interface BatchResult {
  results: Array<{ op: string; id: string; seq?: number; error?: string }>;
  count: number;
  seq: number;
  head: string;
}

/** Thrown when nedbd returns a non-2xx response (except auto-handled cases). */
export class NedbError extends Error {
  constructor(
    public readonly status: number,
    public readonly message: string,
  ) {
    super(`NedbError ${status}: ${message}`);
    this.name = "NedbError";
  }
}

// ── Client ────────────────────────────────────────────────────────────────

export class NedbClient {
  private readonly base: string;
  private readonly db: string;
  private readonly headers: Record<string, string>;
  private readonly autoCreate: boolean;
  private readonly readMs: number;
  private readonly writeMs: number;

  constructor(opts: NedbClientOptions) {
    this.base       = (opts.url ?? "http://127.0.0.1:7070").replace(/\/$/, "");
    this.db         = opts.db;
    this.autoCreate = opts.autoCreate ?? true;
    this.readMs     = opts.readTimeoutMs  ?? 3_000;
    this.writeMs    = opts.writeTimeoutMs ?? 30_000;
    this.headers = { "Content-Type": "application/json" };
    if (opts.token) this.headers["Authorization"] = `Bearer ${opts.token}`;
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  private async fetch(
    method: string,
    path: string,
    body?: unknown,
    timeoutMs?: number,
  ): Promise<Response> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs ?? this.readMs);
    try {
      return await fetch(`${this.base}${path}`, {
        method,
        headers: this.headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }
  }

  private async raise(resp: Response): Promise<never> {
    let msg = resp.statusText;
    try {
      const body = await resp.json() as { error?: string };
      if (body.error) msg = body.error;
    } catch { /* ignore */ }
    throw new NedbError(resp.status, msg);
  }

  private async ensureDb(): Promise<void> {
    await this.fetch("POST", "/v1/databases", { name: this.db }, this.writeMs);
    // 201 = created, 409 = already exists — both fine
  }

  // ── Core CRUD ─────────────────────────────────────────────────────────────

  /**
   * Write a document.
   *
   * @example
   * ```ts
   * await db.put("blocks", "618000", { height: 618000 });
   * await db.put("claims", "c1", { fact: "..." }, { causedBy: ["abc123"] });
   * ```
   */
  async put(
    coll: string,
    id: string,
    doc: Record<string, unknown>,
    opts: PutOptions = {},
  ): Promise<PutResult> {
    const payload: Record<string, unknown> = { coll, id, doc };
    if (opts.causedBy)   payload.caused_by  = opts.causedBy;
    if (opts.validFrom)  payload.valid_from  = opts.validFrom;
    if (opts.validTo)    payload.valid_to    = opts.validTo;
    if (opts.evidence)   payload.evidence    = opts.evidence;
    if (opts.confidence !== undefined) payload.confidence = opts.confidence;
    if (opts.idem)       payload.idem        = opts.idem;
    if (opts.nonce !== undefined)      payload.nonce      = opts.nonce;
    if (opts.clientId)   payload.client      = opts.clientId;

    let resp = await this.fetch("POST", `/v1/databases/${this.db}/put`, payload, this.writeMs);
    if (resp.status === 404 && this.autoCreate) {
      await NedbClient.drain(resp);
      await this.ensureDb();
      resp = await this.fetch("POST", `/v1/databases/${this.db}/put`, payload, this.writeMs);
    }
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<PutResult>;
  }

  /**
   * Discard a response body we are not going to read.
   *
   * Node's `fetch` (undici) keeps the socket checked out until the body is
   * consumed or cancelled. Every early return that skipped this leaked a
   * connection: `queryFull()` returning `[]` for a missing database is a
   * DOCUMENTED resilient path, so a long-running service leaked one socket per
   * such call until the pool starved. It also kept the event loop alive, which
   * is how this was found — the test suite passed every assertion and then
   * hung forever instead of exiting.
   */
  private static async drain(resp: Response): Promise<void> {
    try {
      if (resp.body && !resp.bodyUsed) await resp.body.cancel();
    } catch {
      /* best effort — the socket is being discarded either way */
    }
  }

  /**
   * Percent-encode one URL path segment.
   *
   * `delete("t", "a/slash")` used to interpolate the id straight into the
   * path, so the `/` split it and the route matched a different id — the call
   * returned false ("no such document") for a document that existed.
   * `encodeURIComponent` encodes `/`, which is exactly what is needed here.
   */
  private static seg(value: string): string {
    return encodeURIComponent(String(value));
  }

  /**
   * Escape a value for use inside a double-quoted NQL string literal.
   *
   * The engine's lexer collapses `\"` to a literal quote and leaves every
   * OTHER backslash alone, so only the quote needs escaping. One case is
   * genuinely unrepresentable: a value ENDING in a backslash produces `...\"`,
   * which the lexer reads as an escaped quote, and the string never
   * terminates. That is why {@link get} prefers the `rows/:coll/:id` route,
   * which takes the id from the URL path and has no quoting to get wrong.
   */
  private static nqlStr(value: string): string {
    return String(value).replace(/"/g, '\\"');
  }

  /**
   * Fetch the current version of a document. Returns null if not found.
   *
   * With `asOf`, returns the version at or before that sequence number — the
   * single-document form of time travel.
   *
   * Uses `GET /v1/databases/<db>/rows/<coll>/<id>`, which takes the id from
   * the URL path. This used to build `FROM coll WHERE _id = "..."` and
   * interpolate the id into it, which made every id containing a double quote
   * unreachable: the call returned null — meaning "no such document" — for a
   * document `put()` had stored and `FROM coll` returned. An id ending in a
   * backslash could not be escaped at all.
   *
   * A missing document is `200 {"row": null}` on this route, not 404 —
   * deliberately, so that a 404/405 unambiguously means "the server does not
   * have this route" and the client can fall back to the query path.
   * Requires nedb-engine >= 3.3.0; older servers take the fallback.
   */
  async get(
    coll: string,
    id: string,
    asOf?: number,
  ): Promise<Record<string, unknown> | null> {
    const qs = asOf !== undefined ? `?as_of=${encodeURIComponent(String(asOf))}` : "";
    const path =
      `/v1/databases/${this.db}/rows/${NedbClient.seg(coll)}/${NedbClient.seg(id)}${qs}`;
    const resp = await this.fetch("GET", path);

    // The route answers 200 with `row: null` for a missing document,
    // precisely so this cannot be confused with "no such route". A 404/405
    // therefore means the server predates the route.
    if (resp.status === 404 || resp.status === 405) {
      await NedbClient.drain(resp);
      return this.getViaQuery(coll, id, asOf);
    }
    if (!resp.ok) await this.raise(resp);
    const body = await resp.json() as { row?: Record<string, unknown> | null };
    return body.row ?? null;
  }

  /** Pre-3.3.0 fallback for {@link get} — a point lookup built as NQL. */
  private async getViaQuery(
    coll: string,
    id: string,
    asOf?: number,
  ): Promise<Record<string, unknown> | null> {
    const asOfClause = asOf !== undefined ? ` AS OF ${Math.trunc(asOf)}` : "";
    const rows = await this.query(
      `FROM ${coll}${asOfClause} WHERE _id = "${NedbClient.nqlStr(id)}" LIMIT 1`,
    );
    return rows[0] ?? null;
  }

  /**
   * Tombstone-delete a document.
   * History is preserved in the DAG; returns true if the document existed.
   */
  async delete(coll: string, id: string): Promise<boolean> {
    const resp = await this.fetch(
      "DELETE",
      `/v1/databases/${this.db}/rows/${NedbClient.seg(coll)}/${NedbClient.seg(id)}`,
      undefined,
      this.writeMs,
    );
    if (resp.status === 404) {
      await NedbClient.drain(resp);
      return false;
    }
    if (!resp.ok) await this.raise(resp);
    const body = await resp.json() as { ok: boolean };
    return body.ok;
  }

  /**
   * Run a NQL query. Returns an array of document objects.
   *
   * ```
   * NQL: FROM <coll>
   *        [AS OF <seq>]
   *        [VALID AS OF "<date>"]
   *        [WHERE <predicate>]
   *        [SEARCH "text"]
   *        [TRAVERSE <relation>]
   *        [TRACE caused_by [REVERSE]]
   *        [GROUP BY field [COUNT|SUM f|AVG f|MIN f|MAX f]]
   *        [COUNT | SUM f | AVG f | MIN f | MAX f]
   *        [HAVING <predicate>]
   *        [ORDER BY field [ASC|DESC] (, field [ASC|DESC])*]
   *        [LIMIT n] [OFFSET n]
   * ```
   *
   * Clauses are evaluated in SQL's order regardless of how they are written:
   * FROM -> WHERE -> GROUP BY -> HAVING -> ORDER BY -> OFFSET -> LIMIT.
   *
   * `<predicate>` is a full boolean expression (AND binds tighter than OR;
   * parentheses nest to any depth):
   *
   * ```
   * field = != < <= > >= value
   * field [NOT] IN (v1, v2, ...)
   * field [NOT] BETWEEN low AND high     // inclusive, as in SQL
   * field [NOT] LIKE|ILIKE "pat"         // % any run, _ any one char
   * field IS [NOT] NULL                  // absent OR explicitly null
   * NOT (...) / (... OR ...) / ...
   * ```
   *
   * An ordering comparison against a missing or null field is never true, so
   * `WHERE fee < 5` will not return a row that has no `fee`. Use `IS NULL` to
   * select those rows.
   *
   * A clause the server does not implement is REJECTED rather than silently
   * ignored, so a typo throws instead of quietly answering a different
   * question.
   *
   * Requires nedb-engine >= 3.3.0 for IN / BETWEEN / LIKE / IS NULL / OR /
   * NOT / OFFSET / HAVING / multi-key ORDER BY and bare aggregates.
   */
  async query(nql: string): Promise<Record<string, unknown>[]> {
    const result = await this.queryFull(nql);
    return result.rows;
  }

  /**
   * Like {@link query} but returns the full response including `seq` and `head`.
   */
  async queryFull(nql: string): Promise<QueryResult> {
    const resp = await this.fetch("POST", `/v1/databases/${this.db}/query`, { nql });
    if (resp.status === 400 || resp.status === 404) {
      await NedbClient.drain(resp);
      return { rows: [], count: 0, seq: 0, head: "" };
    }
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<QueryResult>;
  }

  // ── Cast — natural language into NQL ──────────────────────────────────────

  /**
   * Turn a short English prompt into NQL, server-side.
   *
   * Requires a daemon built with `--features cast` and started with `--cast`.
   * Returns the full {@link CastResult} rather than rows, because the plan is
   * the point.
   *
   * `execute` defaults to `false` deliberately: the endpoint hands back a plan
   * for review. A planner that silently runs a wrong guess is worse than one
   * that admits uncertainty. Pass `{ execute: true }` to get `rows` and `count`
   * as well.
   *
   * Throws {@link NedbError} when the daemon lacks the feature (501), when the
   * model emits unparseable NQL (422), or when it names a collection this
   * database does not have (422). That last case is the model's known failure
   * mode on an unfamiliar schema, and it is reported explicitly rather than as
   * an empty result set — which would read as "no matching rows" and be a lie.
   *
   * @example
   * ```ts
   * const plan = await db.cast("orders over 100");
   * // { nql: 'FROM orders WHERE total > 100', valid: true,
   * //   collection_known: true, executed: false, ... }
   *
   * if (plan.valid && plan.collection_known) {
   *   const rows = await db.query(plan.nql);   // run it yourself, after looking
   * }
   * ```
   */
  async cast(prompt: string, opts: { execute?: boolean } = {}): Promise<CastResult> {
    const resp = await this.fetch("POST", `/v1/databases/${this.db}/cast`, {
      prompt,
      execute: opts.execute ?? false,
    });
    if (!resp.ok) {
      // Append the offending NQL to the message. Debugging a cast failure
      // without seeing what the model generated is guesswork.
      let msg = resp.statusText;
      try {
        const body = await resp.json() as { error?: string; nql?: string };
        if (body.error) msg = body.error;
        if (body.nql) msg = `${msg} (generated: ${JSON.stringify(body.nql)})`;
      } catch { /* ignore */ }
      throw new NedbError(resp.status, msg);
    }
    return resp.json() as Promise<CastResult>;
  }

  /** Just the NQL string. Convenience wrapper over {@link cast}. */
  async castNql(prompt: string): Promise<string> {
    return (await this.cast(prompt)).nql;
  }

  // ── Batch ─────────────────────────────────────────────────────────────────

  /**
   * Run a batch of put/del operations in a single HTTP round-trip.
   *
   * @example
   * ```ts
   * await db.batch([
   *   { op: "put", coll: "blocks", id: "1", doc: { height: 1 } },
   *   { op: "del", coll: "blocks", id: "0" },
   * ]);
   * ```
   */
  async batch(ops: BatchOp[]): Promise<BatchResult> {
    let resp = await this.fetch(
      "POST",
      `/v1/databases/${this.db}/batch`,
      { ops },
      this.writeMs,
    );
    if (resp.status === 404 && this.autoCreate) {
      await NedbClient.drain(resp);
      await this.ensureDb();
      resp = await this.fetch(
        "POST",
        `/v1/databases/${this.db}/batch`,
        { ops },
        this.writeMs,
      );
    }
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<BatchResult>;
  }

  // ── Indexes ───────────────────────────────────────────────────────────────

  /** Create a sorted index on (coll, field) for fast ORDER BY queries. */
  async createIndex(
    coll: string,
    field: string,
    kind: "sorted" | "eq" = "sorted",
  ): Promise<{ ok: boolean }> {
    const resp = await this.fetch(
      "POST",
      `/v1/databases/${this.db}/index`,
      { coll, field, kind },
      this.writeMs,
    );
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<{ ok: boolean }>;
  }

  // ── Integrity ─────────────────────────────────────────────────────────────

  /** Run a full BLAKE2b tamper-evidence check over all objects. */
  async verify(): Promise<VerifyResult> {
    const resp = await this.fetch("GET", `/v1/databases/${this.db}/verify`);
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<VerifyResult>;
  }

  /** Return the current BLAKE2b Merkle head of the database. */
  async head(): Promise<string> {
    const resp = await this.fetch("GET", `/v1/databases/${this.db}`);
    if (!resp.ok) await this.raise(resp);
    const body = await resp.json() as { head: string };
    return body.head;
  }

  /** Return the current global sequence number. */
  async seq(): Promise<number> {
    const resp = await this.fetch("GET", `/v1/databases/${this.db}`);
    if (!resp.ok) await this.raise(resp);
    const body = await resp.json() as { seq: number };
    return body.seq;
  }

  /** Trigger an explicit checkpoint (no-op on v2 DAG — always snapshotted). */
  async checkpoint(): Promise<{ ok: boolean; head: string; seq: number }> {
    const resp = await this.fetch(
      "POST",
      `/v1/databases/${this.db}/checkpoint`,
      {},
      this.writeMs,
    );
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<{ ok: boolean; head: string; seq: number }>;
  }

  /** Return the last `limit` write operations. */
  async log(limit = 50): Promise<Record<string, unknown>[]> {
    const resp = await this.fetch(
      "GET",
      `/v1/databases/${this.db}/log?limit=${limit}`,
    );
    if (!resp.ok) await this.raise(resp);
    const body = await resp.json() as { log: Record<string, unknown>[] };
    return body.log;
  }

  // ── Server ────────────────────────────────────────────────────────────────

  /** Ping the server. Returns full health object. */
  async health(): Promise<HealthResult> {
    const resp = await this.fetch("GET", "/health");
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<HealthResult>;
  }

  /** Returns true if the server is reachable and healthy. */
  async ping(): Promise<boolean> {
    try {
      const h = await this.health();
      return h.ok;
    } catch {
      return false;
    }
  }

  /** List all database names on this server. */
  async listDatabases(): Promise<string[]> {
    const resp = await this.fetch("GET", "/v1/databases");
    if (!resp.ok) await this.raise(resp);
    const body = await resp.json() as { databases: Array<{ name: string }> };
    return body.databases.map((d) => d.name);
  }

  /** Explicitly create this database. Idempotent. */
  async createDatabase(): Promise<void> {
    const resp = await this.fetch(
      "POST",
      "/v1/databases",
      { name: this.db },
      this.writeMs,
    );
    if (!resp.ok && resp.status !== 409) await this.raise(resp);
  }

  /** Drop this database and all its data. Irreversible. */
  async dropDatabase(): Promise<boolean> {
    const resp = await this.fetch(
      "DELETE",
      `/v1/databases/${this.db}`,
      undefined,
      this.writeMs,
    );
    if (!resp.ok) await this.raise(resp);
    const body = await resp.json() as { dropped: boolean };
    return body.dropped;
  }
}

export default NedbClient;
