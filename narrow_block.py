#!/usr/bin/env python3
import os
import json
import requests
import argparse
from typing import Optional, Tuple, Iterable, List

import psycopg2
from psycopg2.extras import RealDictCursor

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

# --- Configuration (env defaults) ---

# Reference provider = one that originally produced the DB counts
REF_PROVIDER_DEFAULT  = ""
TEST_PROVIDER_DEFAULT = ""

CONTRACT = os.getenv("CONTRACT", "0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d")
TOPIC    = os.getenv("TOPIC",    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

#DB_URL       = os.getenv("DB_URL", "postgresql://rpc_writer:z5eKm563OcvxhH44c6q01PjvhG7j@78.60.147.136:5432/rpc_checks")
#DB_SCHEMA    = os.getenv("DB_SCHEMA")                  # optional, e.g. "public"
#DB_DISC_TABLE = os.getenv("DB_DISC_TABLE", "rpc_discrepancies")  # source table for ranges

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
REF_PROVIDER = require_env("REF_PROVIDER")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_SCHEMA     = os.getenv("DB_SCHEMA")
DB_DISC_TABLE = os.getenv("DB_DISC_TABLE", "rpc_discrepancies")

# --- CLI handling (providers override env/defaults) ---
parser = argparse.ArgumentParser(
    description=(
        "Compare log ranges from Postgres rpc_discrepancies table between "
        "REF and TEST RPC providers, narrowing to per-block differences when needed."
    )
)
parser.add_argument("--ref_provider",  type=str, help="Reference RPC endpoint (default/env REF_PROVIDER)")
parser.add_argument("--test_provider", type=str, help="Test RPC endpoint (default/env TEST_PROVIDER)")
args, _ = parser.parse_known_args()

REF_PROVIDER  = args.ref_provider  or require_env("REF_PROVIDER")
TEST_PROVIDER = args.test_provider or os.getenv("TEST_PROVIDER", TEST_PROVIDER_DEFAULT)

# --- RPC helper (same style as before) ---

def get_log_count(provider: str, contract: str, topic: str, start_block: int, end_block: int) -> int:
    """Fetch number of logs for given block range via eth_getLogs."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": contract,
            "topics": [topic],
        }],
    }
    try:
        resp = requests.post(provider, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            # Always return something numeric, but log the error
            print(f"⚠️ RPC error {start_block}-{end_block} from {provider}: {json.dumps(data['error'])}")
            return -1
        return len(data.get("result", []))
    except Exception as e:
        print(f"⚠️ Error fetching logs {start_block}-{end_block} from {provider}: {e}")
        return -1

# --- Postgres helpers: read from rpc_discrepancies ---

def read_discrepancies_from_pg(
    db_url: str,
    schema: Optional[str],
    table: str,
    provider: Optional[str] = None,
) -> Iterable[Tuple[int, int, int]]:
    """
    Yields tuples (from_block, to_block, discrepancy_count) from PostgreSQL
    table rpc_discrepancies (or override via DB_DISC_TABLE).
    Orders by from_block ascending.
    """
    sch = schema or "public"
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            base_q = (
                f'SELECT "from_block","to_block","discrepancy_count" '
                f'FROM "{sch}"."{table}"'
            )

            if provider:
                q = base_q + ' WHERE "provider" = %s ORDER BY "from_block" ASC'
                print(f'Reading ranges from table: {sch}.{table} for provider: {provider}')
                cur.execute(q, (provider,))
            else:
                q = base_q + ' ORDER BY "from_block" ASC'
                print(f"Reading ranges from table: {sch}.{table} (no provider filter)")
                cur.execute(q)

            for b, e, disc in cur.fetchall():
                yield int(b), int(e), int(disc)
    finally:
        conn.close()

# --- Main logic: same behaviour as bash script, but DB-backed ---

def main():
    print(f"REF provider : {REF_PROVIDER}")
    print(f"TEST provider: {TEST_PROVIDER}")
    if DB_SCHEMA:
        print(f"DB schema    : {DB_SCHEMA}")
    print(f"Source table : {DB_DISC_TABLE}")
    print("---")

    try:
        # now source is rpc_discrepancies (from_block, to_block, discrepancy_count)
        ranges = list(read_discrepancies_from_pg(DB_URL, schema=DB_SCHEMA, table=DB_DISC_TABLE, provider=TEST_PROVIDER))
    except Exception as e:
        print(f"❌ Database error: {e}")
        return

    if not ranges:
        print(f'No ranges found in {DB_DISC_TABLE} for provider: {TEST_PROVIDER}')
        return

    for b, e, n_old in ranges:
        # n_old here is discrepancy_count from rpc_discrepancies
        print(f"Range {b} → {e} (stored discrepancy_count {n_old} from rpc_discrepancies)")

        n_test = get_log_count(TEST_PROVIDER, CONTRACT, TOPIC, b, e)
        n_ref  = get_log_count(REF_PROVIDER,  CONTRACT, TOPIC, b, e)

        # Show both totals
        print(f"  Totals: ref={n_ref}  test={n_test}")

        # If providers disagree on the range, narrow per block
        if n_test != n_ref:
            print(f"  DIFF: providers disagree over {b}-{e}  (ref={n_ref} vs test={n_test})")
            print("  Narrowing per block…")

            total = e - b + 1
            done_cnt = 0
            last_pct = -1

            # initial progress line
            print(f"  Progress: 0% (0/{total})", end="\r", flush=True)

            for blk in range(b, e + 1):
                done_cnt += 1
                pct = done_cnt * 100 // total
                if pct != last_pct and pct < 100:
                    print(f"  Progress: {pct}% ({done_cnt}/{total})", end="\r", flush=True)
                    last_pct = pct

                c_ref  = get_log_count(REF_PROVIDER,  CONTRACT, TOPIC, blk, blk)
                c_test = get_log_count(TEST_PROVIDER, CONTRACT, TOPIC, blk, blk)
                if c_ref != c_test:
                    print()  # break progress line
                    print(f"    Block {blk}: ref={c_ref}  test={c_test}")

            # final progress line
            print()
            print(f"  Progress: 100% ({total}/{total})")

        else:
            # Optional note comparing ref vs stored discrepancy_count
            if n_ref != n_old:
                print(f"  NOTE: rpc_discrepancies says {n_old} but ref now says {n_ref} (range {b}-{e})")
            else:
                print("  OK: matches")

        print()

if __name__ == "__main__":
    main()

