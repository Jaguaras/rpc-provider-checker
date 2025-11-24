#!/usr/bin/env python3
import os
import json
import requests
import argparse
from typing import Optional, Tuple, Iterable, List

import psycopg2
from psycopg2.extras import RealDictCursor

# --- .env loader (no external deps) ---
def load_env_file(path: str = ".env") -> None:
    """Load simple KEY=VALUE pairs from a .env file into os.environ (if not already set)."""
    if not os.path.exists(path):
        return
    with open(path, "r") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

# Load .env BEFORE reading any env-based config
load_env_file(os.getenv("ENV_FILE", ".env"))

# --- Configuration ---
PROVIDER = os.getenv("TEST_PROVIDER", "https://gnosis-rpc.publicnode.com")
CONTRACT = os.getenv("CONTRACT", "0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d")
TOPIC    = os.getenv("TOPIC",    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

#DB_URL      = os.getenv("DB_URL", "postgresql://rpc_writer:z5eKm563OcvxhH44c6q01PjvhG7j@78.60.147.136:5432/rpc_checks")
#DB_SCHEMA   = os.getenv("DB_SCHEMA")  # optional, e.g. "public"
#DB_TABLE    = os.getenv("DB_TABLE")   # optional explicit table name override

# NEW: where to store discrepancies (optional override via env)
#DB_DISC_TABLE = os.getenv("DB_DISC_TABLE", "rpc_discrepancies")

# --- DB configuration from split env vars (no hard-coded defaults) ---
def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

DB_HOST = require_env("DB_HOST")
DB_PORT = require_env("DB_PORT")
DB_NAME = require_env("DB_NAME")
DB_USER = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")

# Build DB_URL only from the split pieces (no DB_URL env, no inline defaults)
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_SCHEMA   = os.getenv("DB_SCHEMA")          # optional, e.g. "public"
DB_TABLE    = os.getenv("DB_TABLE")           # optional explicit table name override
DB_DISC_TABLE = os.getenv("DB_DISC_TABLE", "rpc_discrepancies")


# --- Argument + env handling ---
parser = argparse.ArgumentParser(description="Check logcount discrepancies between RPC and Postgres ranges.")
parser.add_argument("--from_block", type=int, help="Optional starting block (will snap to nearest if not exact)")
parser.add_argument("--to_block", type=int, help="Optional ending block (will snap to nearest if not exact)")
parser.add_argument("--test_provider", type=str, help="Optional RPC endpoint to override default/provider env variable")
parser.add_argument("--delete_rpc_data", action="store_true", help="Delete all discrepancy rows for this provider and exit")
args, _ = parser.parse_known_args()

# Environment variable fallback
ENV_FROM_BLOCK = os.getenv("FROM_BLOCK")
ENV_TO_BLOCK   = os.getenv("TO_BLOCK")
ENV_PROVIDER = os.getenv("TEST_PROVIDER", "https://gnosis-rpc.publicnode.com")

# Final unified values
REQ_FROM_BLOCK = args.from_block or (int(ENV_FROM_BLOCK) if ENV_FROM_BLOCK else None)
REQ_TO_BLOCK   = args.to_block or (int(ENV_TO_BLOCK)   if ENV_TO_BLOCK   else None)
PROVIDER       = args.test_provider or ENV_PROVIDER

SOURCE_NAME = DB_TABLE or (DB_SCHEMA + "." if DB_SCHEMA else "") + "(auto-detected)"

# --- RPC fetch ---
def get_log_count(provider: str, contract: str, topic: str, start_block: int, end_block: int) -> int:
    """Fetch number of logs for given block range."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": contract,
            "topics": [topic]
        }]
    }
    try:
        resp = requests.post(provider, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        # if RPC returns an error, treat as failure
        if "error" in data:
            raise RuntimeError(json.dumps(data["error"]))
        return len(data.get("result", []))
    except Exception as e:
        print(f"⚠️ Error fetching logs {start_block}-{end_block}: {e}")
        return -1

# --- Postgres helpers (reading ranges) ---
def _find_table_with_columns(conn, schema: Optional[str]) -> Optional[Tuple[str, str]]:
    """
    Locate a table that has columns: (from_block, to_block, cnt).
    Returns (schema, table) if found, else None.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.columns
            WHERE (%s IS NULL OR table_schema = %s)
              AND column_name IN ('from_block','to_block','cnt')
            GROUP BY table_schema, table_name
            HAVING COUNT(DISTINCT column_name) = 3
            ORDER BY table_schema, table_name
            """,
            (schema, schema)
        )
        row = cur.fetchone()
        if row:
            return row["table_schema"], row["table_name"]
    return None

def read_ranges_from_pg(db_url: str,
                        explicit_table: Optional[str] = None,
                        schema: Optional[str] = None) -> Iterable[Tuple[int, int, int]]:
    """
    Yields tuples (from_block, to_block, cnt) from PostgreSQL.
    Orders by from_block ascending.
    """
    conn = psycopg2.connect(db_url)
    try:
        if explicit_table:
            if "." in explicit_table:
                tbl_schema, tbl_name = explicit_table.split(".", 1)
            else:
                tbl_schema, tbl_name = (schema or "public"), explicit_table
        else:
            found = _find_table_with_columns(conn, schema)
            if not found:
                raise RuntimeError("No table with required columns (from_block, to_block, cnt) was found.")
            tbl_schema, tbl_name = found

        with conn.cursor() as cur:
            q = f'SELECT "from_block","to_block","cnt" FROM "{tbl_schema}"."{tbl_name}" ORDER BY "from_block" ASC'
            cur.execute(q)
            for b, e, n_old in cur.fetchall():
                yield int(b), int(e), int(n_old)
    finally:
        conn.close()

# --- NEW: discrepancy table helpers (write-only, no logic change to comparisons) ---
def ensure_discrepancy_table(conn, schema: Optional[str], table: str):
    sch = schema or "public"
    with conn.cursor() as cur:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS "{sch}"."{table}" (
                id BIGSERIAL PRIMARY KEY,
                from_block BIGINT NOT NULL,
                to_block   BIGINT NOT NULL,
                discrepancy_count INTEGER NOT NULL,
                provider TEXT NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        ''')
        conn.commit()

def insert_discrepancy(conn, schema: Optional[str], table: str,
                       b: int, e: int, diff: int, provider: str):
    sch = schema or "public"
    with conn.cursor() as cur:
        cur.execute(
            f'INSERT INTO "{sch}"."{table}" '
            f'(from_block, to_block, discrepancy_count, provider) '
            f'VALUES (%s, %s, %s, %s);',
            (b, e, diff, provider)
        )
    conn.commit()

def delete_rpc_data(conn, schema: Optional[str], table: str, provider: str):
    """
    Delete all discrepancy rows for the given provider from the discrepancy table.
    """
    sch = schema or "public"
    with conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{sch}"."{table}" WHERE provider = %s;',
            (provider,),
        )
        deleted = cur.rowcount
    conn.commit()
    print(f"🗑️ Deleted {deleted} rows for provider {provider!r} from {sch}.{table}")

# --- NEW: nearest-window selection (unchanged comparison logic) ---
def choose_window(ranges: List[Tuple[int,int,int]],
                  req_from: Optional[int],
                  req_to: Optional[int]) -> List[Tuple[int,int,int]]:
    """
    Given full list of (b,e,cnt), pick a window bounded by the closest existing
    from_block to req_from (if provided) and closest existing to_block to req_to (if provided).
    Returns sublist preserving order and original comparison logic.
    """
    if not ranges:
        return []

    from_values = [b for (b, _, _) in ranges]
    to_values   = [e for (_, e, _) in ranges]

    def closest(target: int, candidates: List[int]) -> int:
        return min(candidates, key=lambda x: abs(x - target))

    snapped_from = closest(req_from, from_values) if req_from is not None else from_values[0]
    snapped_to   = closest(req_to,   to_values)   if req_to   is not None else to_values[-1]

    lo, hi = min(snapped_from, snapped_to), max(snapped_from, snapped_to)

    window = [(b, e, n) for (b, e, n) in ranges if (b >= lo and e <= hi)]

    if req_from is not None or req_to is not None:
        print("== Selection window ==")
        print(f"Requested FROM_BLOCK: {req_from!r}  -> using closest existing from_block: {snapped_from}")
        print(f"Requested TO_BLOCK:   {req_to!r}    -> using closest existing to_block:   {snapped_to}")
        if not window:
            print("⚠️ No rows fall inside the selected window (after snapping).")
    return window

def main():

    if args.delete_rpc_data:
        try:
            disc_conn = psycopg2.connect(DB_URL)
            ensure_discrepancy_table(disc_conn, DB_SCHEMA, DB_DISC_TABLE)
            delete_rpc_data(disc_conn, DB_SCHEMA, DB_DISC_TABLE, PROVIDER)
        except Exception as ex:
            print(f"❌ Database error during deletion: {ex}")
        finally:
            try:
                disc_conn.close()
            except Exception:
                pass
        return  # Exit after deletion, skip RPC checks

    # Read all ranges from Postgres (source table auto-detected unless DB_TABLE provided)
    try:
        all_ranges = list(read_ranges_from_pg(DB_URL, explicit_table=DB_TABLE, schema=DB_SCHEMA))

        # Apply optional nearest-window filter (does not change comparison logic)
        selected = choose_window(all_ranges, REQ_FROM_BLOCK, REQ_TO_BLOCK) if (REQ_FROM_BLOCK is not None or REQ_TO_BLOCK is not None) else all_ranges

        # Open one connection for discrepancy writes
        disc_conn = psycopg2.connect(DB_URL)
        ensure_discrepancy_table(disc_conn, DB_SCHEMA, DB_DISC_TABLE)

        try:
            summary_rows: List[Tuple[int, int, int, int, str]] = []
            for b, e, n_old in selected:
                n_new = get_log_count(PROVIDER, CONTRACT, TOPIC, b, e)
                print(f"{b}: {n_new}")
                if n_new != n_old:
                    print(f"❗ Discrepancy between block {b}-{e}: {n_old} ({SOURCE_NAME}) vs {n_new} ({PROVIDER})")
                    # Only record a numeric discrepancy count (skip negative sentinel values)
                    if n_new >= 0 and n_old >= 0:
                        # diff = abs(n_new - n_old)
                        summary_rows.append((b, e, n_old, n_new, PROVIDER))
                        try:
                            insert_discrepancy(disc_conn, DB_SCHEMA, DB_DISC_TABLE, b, e, n_new, PROVIDER)
                        except Exception as write_err:
                            print(f"❌ DB insert error for {b}-{e}: {write_err}")

            # print discrepancy summary table
            print("\n=== Discrepancy summary ===")
            if not summary_rows:
                print("No discrepancies recorded.")
            else:
                print("| Block range | Initial DB log count | RPC log count | RPC provider |")
                print("|-------------|---------------------|-------------------|--------------|")
                for b, e, initial, discrep, prov in summary_rows:
                    print(f"| {b}-{e} | {initial} | {discrep} | {prov} |")

        finally:
            disc_conn.close()

    except Exception as ex:
        print(f"❌ Database error: {ex}")

if __name__ == "__main__":
    main()