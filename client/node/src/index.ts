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
      await this.ensureDb();
      resp = await this.fetch("POST", `/v1/databases/${this.db}/put`, payload, this.writeMs);
    }
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<PutResult>;
  }

  /**
   * Fetch the current version of a document. Returns null if not found.
   */
  async get(
    coll: string,
    id: string,
  ): Promise<Record<string, unknown> | null> {
    const rows = await this.query(`FROM ${coll} WHERE _id = "${id}" LIMIT 1`);
    return rows[0] ?? null;
  }

  /**
   * Tombstone-delete a document.
   * History is preserved in the DAG; returns true if the document existed.
   */
  async delete(coll: string, id: string): Promise<boolean> {
    const resp = await this.fetch(
      "DELETE",
      `/v1/databases/${this.db}/rows/${coll}/${id}`,
      undefined,
      this.writeMs,
    );
    if (resp.status === 404) return false;
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
   *        [WHERE field = value [AND ...]]
   *        [ORDER BY field [DESC]]
   *        [LIMIT n]
   *        [GROUP BY field COUNT|SUM|AVG|MIN|MAX]
   *        [TRACE caused_by [REVERSE]]
   *        [SEARCH "text"]
   * ```
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
      return { rows: [], count: 0, seq: 0, head: "" };
    }
    if (!resp.ok) await this.raise(resp);
    return resp.json() as Promise<QueryResult>;
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
