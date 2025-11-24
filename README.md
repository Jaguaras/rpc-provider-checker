# rpc-provider-checker

`rpc-provider-verifier` is a toolkit for testing and verifying EVM RPC providers by comparing their `eth_getLogs` results over large block ranges. It stores per-range log counts in Postgres, detects discrepancies between a reference provider and providers under test, and surfaces them via summary tables so you can quickly see which RPC endpoints are missing events, inconsistent, or unreliable.

## One-time configuration

### Setup the database

If you don't have access to the database, you will need to setup localy `postgresql`. 

### Python dependencies

Install python dependencies:

```bash
sudo apt update
sudo apt install -y python3 python3-pip
pip install requests urllib3 psycopg2-binary
```

### Buildup blocks database

#### If you already have database access

You may already have block data populated from `Nethermind v1.34.1+04297e8b` (`alpha non-public release`). This version is currently treated as the most reliable source compared with other RPC providers.

It’s recommended to use this data as the reference when comparing other RPC providers. If you rebuild the blocks database, it will overwrite the existing data.

#### If you don’t have database access

You’ll need to build the block data yourself using a reliable RPC provider.

Run:

```bash
./logcounts_to_db.py \
  --provider <YOUR_RELIABLE_RPC_PROVIDER> \
  --start 6306357 \ # optional
  --end 42618965 \ # optional
  --range 1000 \ # optional
  --step 10000 \ # optional
```

The arguments `--start`, `--end`, `--range`, and `--step` default to:

- `start`: `6306357`
- `end`: `42618965`
- `range`: `1000`
- `step`: `10000`

You can omit them if these defaults are fine for your use case.

## How to use it

### Compare RPC providers

Compare the RPC provider you want to test against the reference blocks database.

By default, the script checks block ranges from `6306357` to `42618965`. If you want to test a specific range, add `--from_block <BLOCK_NUMBER>` and `--to_block <BLOCK_NUMBER>`.


```bash
./verify_logs.py \
--test_provider <TEST_RPC_PROVIDER> \
--from_block <BLOCK_NUMBER> \ # optional
--to_block <BLOCK_NUMBER> \ # optional
```

### Narrow down to a specific block

First, the verification script will find block ranges that have discrepancies between the reference database and the RPC provider under test. To narrow this down to the exact block(s) that differ, run:

```bash
./narrow_block.py \
--test_provider <TEST_RPC_PROVIDER>
```