#!/usr/bin/env python3
"""
seed_studio_schema.py — Write _studio/schema for NEDB Studio.

Run on the production server against a running nedbd instance:
    python3 seed_studio_schema.py --db vision --url http://localhost:7070

This introspects the live database and writes the _studio/schema document
that the Studio query console requires.

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import argparse, json, sys, urllib.request, urllib.error

def req(url, method="GET", body=None, token=None):
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(r, timeout=30) as resp:
        return json.loads(resp.read())

def infer_type(value) -> str:
    if isinstance(value, bool):   return "boolean"
    if isinstance(value, int):    return "number"
    if isinstance(value, float):  return "number"
    if isinstance(value, dict):   return "json"
    if isinstance(value, list):   return "json"
    return "string"

def sample_fields(base_url, db_name, coll, token, limit=5):
    """Sample up to `limit` docs from a collection and infer field types."""
    try:
        result = req(f"{base_url}/v1/databases/{db_name}/query", "POST",
                     {"nql": f"FROM {coll} LIMIT {limit}"}, token)
        rows = result.get("rows", [])
    except Exception:
        return [{"name": "_id", "type": "string"}]

    field_types: dict[str, str] = {}
    for row in rows:
        for k, v in row.items():
            if k.startswith("_"):
                continue  # skip NEDB internal fields
            field_types[k] = infer_type(v)

    if not field_types:
        return [{"name": "_id", "type": "string"}]
    return [{"name": k, "type": t} for k, t in sorted(field_types.items())]

def main():
    ap = argparse.ArgumentParser(description="Seed _studio/schema for NEDB Studio.")
    ap.add_argument("--db",       required=True, help="Database name (e.g. vision)")
    ap.add_argument("--url",      default="http://localhost:7070", help="nedbd URL")
    ap.add_argument("--token",    default=None,  help="Bearer token (NEDBD_TOKEN)")
    ap.add_argument("--name",     default=None,  help="App name (defaults to db name)")
    ap.add_argument("--desc",     default=None,  help="Description")
    ap.add_argument("--overwrite",action="store_true", help="Overwrite existing schema")
    args = ap.parse_args()

    base = args.url.rstrip("/")
    db   = args.db
    tok  = args.token

    # Check existing schema
    if not args.overwrite:
        try:
            r = req(f"{base}/v1/databases/{db}/query", "POST",
                    {"nql": 'FROM _studio WHERE _id = "schema" LIMIT 1'}, tok)
            if r.get("rows"):
                print(f"  _studio/schema already exists in '{db}'. Use --overwrite to replace.")
                sys.exit(0)
        except Exception:
            pass

    # Get database detail
    print(f"  Introspecting '{db}' at {base}…")
    try:
        detail = req(f"{base}/v1/databases/{db}", token=tok)
    except Exception as e:
        sys.exit(f"  ERROR: cannot reach {base}/v1/databases/{db} — {e}")

    collections_raw = detail.get("collections", {})
    if not collections_raw:
        sys.exit(f"  ERROR: database '{db}' has no collections yet (empty?)")

    # Skip internal collections
    user_colls = [c for c in collections_raw.keys()
                  if not c.startswith("_") and c not in ("__links__",)]

    if not user_colls:
        print(f"  No user collections found. Writing minimal schema.")
        user_colls = list(collections_raw.keys())[:5]

    print(f"  Found {len(user_colls)} collections: {', '.join(user_colls[:8])}{'…' if len(user_colls) > 8 else ''}")

    # Build collections with sampled fields
    coll_defs = []
    for coll in user_colls:
        print(f"    sampling {coll}…", end=" ", flush=True)
        fields = sample_fields(base, db, coll, tok, limit=3)
        print(f"{len(fields)} fields")
        coll_defs.append({"name": coll, "fields": fields})

    # Extract relations from __links__
    relations = []
    seen_rels = set()
    try:
        link_result = req(f"{base}/v1/databases/{db}/query", "POST",
                          {"nql": "FROM __links__ LIMIT 100"}, tok)
        for row in link_result.get("rows", []):
            frm = str(row.get("_from", ""))
            rel = str(row.get("_rel", ""))
            to  = str(row.get("_to", ""))
            if frm and rel and to and ":" in frm and ":" in to:
                frm_coll = frm.split(":")[0]
                to_coll  = to.split(":")[0]
                key = (frm_coll, rel, to_coll)
                if key not in seen_rels:
                    seen_rels.add(key)
                    relations.append({
                        "from": frm_coll, "relation": rel,
                        "to": to_coll,    "cardinality": "one_to_many"
                    })
    except Exception:
        pass
    print(f"  Found {len(relations)} relation types")

    # Build NQL examples
    nql_examples = []
    for c in user_colls[:3]:
        nql_examples.append(f"FROM {c} LIMIT 10")
    if relations:
        r0 = relations[0]
        nql_examples.append(
            f'FROM {r0["from"]} LIMIT 5 TRAVERSE {r0["relation"]}'
        )

    # Build the scaffold doc
    app_name = args.name or db
    description = args.desc or f"{db} — auto-generated by NEDB Studio scaffold tool"

    schema_doc = {
        "_id":         "schema",
        "appName":     app_name,
        "description": description,
        "collections": coll_defs,
        "relations":   relations,
        "indexes":     [],          # Studio rebuilds from DB detail
        "nqlExamples": nql_examples,
    }

    # Write to _studio/schema
    print(f"\n  Writing _studio/schema to '{db}'…")
    result = req(f"{base}/v1/databases/{db}/put", "POST", {
        "coll": "_studio",
        "id":   "schema",
        "doc":  schema_doc,
    }, tok)

    print(f"  ✅  Done!  seq={result.get('seq')}  head={str(result.get('head',''))[:16]}…")
    print(f"\n  Reload the Studio — the query console should now load immediately.")

if __name__ == "__main__":
    main()
