#!/usr/bin/env python3
import os, json, time, requests, sqlite3
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import datetime
from typing import Optional
import argparse

# ----------------- .env loader (same as other script) -----------------
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

# load .env before reading any env-based config
load_env_file(os.getenv("ENV_FILE", ".env"))

# ----------------- CONFIG (edit as needed) -----------------
PROVIDER = os.getenv("PROVIDER", "")
START    = int(os.getenv("START", "6306357"))
END      = int(os.getenv("END",   "42618965"))
RANGE    = int(os.getenv("RANGE", "1000"))
STEP     = int(os.getenv("STEP",  "10000"))
CONTRACT = os.getenv("CONTRACT", "0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d")
TOPIC    = os.getenv("TOPIC",    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

# ----------------- DB configuration from split env vars (no hard-coded defaults) -----------------
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

# Networking knobs (unchanged)
CONNECT_READ_TIMEOUT = (5, 120)   # (connect, read) secs — curl-like behavior
MAX_TOTAL_RETRIES    = 5
BACKOFF_FACTOR       = 0.8

# Auto-splitting knobs (unchanged)
MIN_RANGE            = int(os.getenv("MIN_RANGE", "500"))
SPLIT_ON_ERRORS      = True

# ----------------- HTTP session (unchanged) -----------------
def make_session(force_close: bool = True) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=MAX_TOTAL_RETRIES, connect=3, read=3,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Connection": "close" if force_close else "keep-alive",
    })
    return s

SESSION = make_session(force_close=True)

# ----------------- RPC logic (unchanged) -----------------
def _rpc_logs(provider: str, b_from: int, b_to: int, contract: str, topic: str):
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(b_from),
            "toBlock":   hex(b_to),
            "address":   contract,
            "topics":   [topic]
        }]
    }
    r = SESSION.post(provider, json=payload, timeout=CONNECT_READ_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if "result" in data and isinstance(data["result"], list):
        return len(data["result"])
    raise RuntimeError(f"Bad RPC response: {json.dumps(data)[:400]}")

def get_log_count_with_splitting(b_from: int, b_to: int) -> int:
    queue = [(b_from, b_to)]
    total = 0
    while queue:
        f, t = queue.pop()
        try:
            total += _rpc_logs(PROVIDER, f, t, CONTRACT, TOPIC)
        except (requests.ReadTimeout, requests.ConnectTimeout, requests.HTTPError,
                RuntimeError, requests.ConnectionError) as e:
            width = t - f + 1
            if (not SPLIT_ON_ERRORS) or width <= MIN_RANGE:
                raise
            mid = f + (width // 2) - 1
            queue.append((mid + 1, t))
            queue.append((f, mid))
    return total

# ----------------- DB layer -----------------
def is_postgres(url: str) -> bool:
    return url.lower().startswith("postgres://") or url.lower().startswith("postgresql://")

def _normalize_provider_base(p: str) -> str:
    """
    Strip scheme and trailing slash so that different textual variants
    of the same host:port are treated as one base.
    """
    p = p.strip()
    if p.startswith("http://"):
        p = p[len("http://"):]
    elif p.startswith("https://"):
        p = p[len("https://"):]
    return p.rstrip("/")

# --- SQLite backend ---
def sqlite_connect(path: str):
    # path like sqlite:///file.sqlite or sqlite:////absolute/path.sqlite
    if path.startswith("sqlite:///"):
        db_file = path[len("sqlite:///"):]
    elif path.startswith("sqlite://"):
        # allow sqlite:///<path>
        db_file = path[len("sqlite://"):]
        if db_file.startswith("/"):
            db_file = db_file[1:]
    else:
        db_file = path
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def sqlite_init(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS log_ranges (
        from_block INTEGER NOT NULL,
        to_block   INTEGER NOT NULL,
        cnt        INTEGER,
        status     TEXT NOT NULL,         -- 'OK' or 'ERROR'
        error_type TEXT,
        error_msg  TEXT,
        provider   TEXT NOT NULL,
        contract   TEXT NOT NULL,
        topic      TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (from_block, to_block, provider)
    );
    """)
    conn.commit()

def sqlite_upsert(conn, b_from: int, b_to: int, cnt: Optional[int], status: str,
                  error_type: Optional[str], error_msg: Optional[str]):
    conn.execute("""
    INSERT INTO log_ranges (from_block, to_block, cnt, status, error_type, error_msg,
                            provider, contract, topic, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(from_block, to_block, provider) DO UPDATE SET
        cnt=excluded.cnt,
        status=excluded.status,
        error_type=excluded.error_type,
        error_msg=excluded.error_msg,
        provider=excluded.provider,
        contract=excluded.contract,
        topic=excluded.topic,
        updated_at=excluded.updated_at;
    """, (b_from, b_to, cnt, status, error_type, error_msg,
          PROVIDER, CONTRACT, TOPIC, datetime.utcnow().isoformat(timespec="seconds")+"Z"))
    conn.commit()

def sqlite_clean_provider(conn, provider: str):
    """
    Delete all rows for a given provider in SQLite backend (handling small
    variations like scheme and trailing slash).
    """
    base = _normalize_provider_base(provider)
    variants = (
        base,
        f"http://{base}",
        f"https://{base}",
        f"{base}/",
        f"http://{base}/",
        f"https://{base}/",
    )
    placeholders = ",".join("?" for _ in variants)
    conn.execute(f"DELETE FROM log_ranges WHERE provider IN ({placeholders})", variants)
    conn.commit()

# --- Postgres backend (optional) ---
_pg = None
def pg_connect(url: str):
    global _pg
    try:
        import psycopg2
        _pg = psycopg2
    except ImportError as e:
        raise SystemExit("psycopg2-binary not installed. Run: pip install psycopg2-binary") from e
    return _pg.connect(url)

def pg_init(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS log_ranges (
            from_block BIGINT NOT NULL,
            to_block   BIGINT NOT NULL,
            cnt        BIGINT,
            status     TEXT NOT NULL,
            error_type TEXT,
            error_msg  TEXT,
            provider   TEXT NOT NULL,
            contract   TEXT NOT NULL,
            topic      TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (from_block, to_block, provider)
        );
        """)

def pg_upsert(conn, b_from: int, b_to: int, cnt: Optional[int], status: str,
              error_type: Optional[str], error_msg: Optional[str]):
    with conn, conn.cursor() as cur:
        cur.execute("""
        INSERT INTO log_ranges (from_block, to_block, cnt, status, error_type, error_msg,
                                provider, contract, topic, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
        ON CONFLICT (from_block, to_block, provider)
        DO UPDATE SET
            cnt=EXCLUDED.cnt,
            status=EXCLUDED.status,
            error_type=EXCLUDED.error_type,
            error_msg=EXCLUDED.error_msg,
            provider=EXCLUDED.provider,
            contract=EXCLUDED.contract,
            topic=EXCLUDED.topic,
            updated_at=NOW();
        """, (b_from, b_to, cnt, status, error_type, error_msg,
              PROVIDER, CONTRACT, TOPIC))

def pg_clean_provider(conn, provider: str):
    """
    Delete all rows for a given provider in Postgres backend (handling small
    variations like scheme and trailing slash).
    """
    base = _normalize_provider_base(provider)
    variants = (
        base,
        f"http://{base}",
        f"https://{base}",
        f"{base}/",
        f"http://{base}/",
        f"https://{base}/",
    )
    placeholders = ",".join(["%s"] * len(variants))
    with conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM log_ranges WHERE provider IN ({placeholders})",
            variants,
        )

# --- DB factory ---
class DB:
    def __init__(self, url: str):
        self.url = url
        if is_postgres(url):
            self.kind = "pg"
            self.conn = pg_connect(url)
            pg_init(self.conn)
        else:
            self.kind = "sqlite"
            self.conn = sqlite_connect(url)
            sqlite_init(self.conn)

    def upsert_ok(self, b_from: int, b_to: int, cnt: int):
        if self.kind == "pg":
            pg_upsert(self.conn, b_from, b_to, cnt, "OK", None, None)
        else:
            sqlite_upsert(self.conn, b_from, b_to, cnt, "OK", None, None)

    def upsert_err(self, b_from: int, b_to: int, etype: str, emsg: str):
        if self.kind == "pg":
            pg_upsert(self.conn, b_from, b_to, None, "ERROR", etype, emsg[:900])
        else:
            sqlite_upsert(self.conn, b_from, b_to, None, "ERROR", etype, emsg[:900])

    def clean_provider(self, provider: str):
        """
        Remove all records for the given provider from log_ranges.
        """
        if self.kind == "pg":
            pg_clean_provider(self.conn, provider)
        else:
            sqlite_clean_provider(self.conn, provider)


# ----------------- main (minimal change to logic) -----------------
def main():
    global PROVIDER, START, END, RANGE, STEP

    parser = argparse.ArgumentParser(description="Fetch log ranges and store them in DB.")
    parser.add_argument(
        "--provider", "-p",
        help=f"RPC endpoint URL (default: {PROVIDER})"
    )
    parser.add_argument(
        "--start",
        type=int,
        help=f"Start block (default: {START})"
    )
    parser.add_argument(
        "--end",
        type=int,
        help=f"End block (default: {END})"
    )
    parser.add_argument(
        "--range",
        type=int,
        help=f"Range size per request (default: {RANGE})"
    )
    parser.add_argument(
        "--step",
        type=int,
        help=f"Step between starting blocks (default: {STEP})"
    )

    args = parser.parse_args()

    if args.provider:
        PROVIDER = args.provider
    if args.start is not None:
        START = args.start
    if args.end is not None:
        END = args.end
    if args.range is not None:
        RANGE = args.range
    if args.step is not None:
        STEP = args.step


    db = DB(DB_URL)
    db.clean_provider(PROVIDER)
    processed = 0
    print("Writing results to DB ...\n")
    for b in range(START, END + 1, STEP):
        e = min(b + RANGE - 1, END)
        try:
            cnt = get_log_count_with_splitting(b, e)
            line = f"{b} {e} {cnt}"
            print(line)
            db.upsert_ok(b, e, cnt)
        except Exception as ex:
            # Keep same stdout format with ERROR for grep-friendliness
            msg = f"{b} {e} ERROR: {type(ex).__name__}: {ex}"
            print(msg)
            db.upsert_err(b, e, type(ex).__name__, str(ex))
        processed += 1
    print(f"Done. Ranges processed: {processed}")

if __name__ == "__main__":
    main()
