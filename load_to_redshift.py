#!/usr/bin/env python3
"""
Load Informatica_runs_stats CSV into Redshift table (bulk insert).

Looks for yesterday's stats file in output/, loads it, then moves it to archive/.

Requires REDSHIFT_PASSWORD (and optional REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER).
See .env.example.

Usage:
  export $(cat .env.example | xargs)
  python load_to_redshift.py
  python load_to_redshift.py --file output/Informatica_runs_stats_2026-06-22.csv
"""

import argparse
import csv
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2

PDT = timezone(timedelta(hours=-7))
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
ARCHIVE_DIR = Path(__file__).resolve().parent / "archive"
STATS_CSV_PREFIX = "Informatica_runs_stats"

BATCH_SIZE = 1000  # insert 1000 rows at a time

INSERT_SQL = """
    INSERT INTO powerbipoc.fact_mapping_execution (
        stat_date,
        mapping_task_name,
        parent_taskflow,
        location,
        status,
        start_time_utc,
        end_time_utc,
        duration_sec,
        error_message
    ) VALUES
"""


def load_redshift_config() -> dict:
    host = os.environ.get("REDSHIFT_HOST", "db-prod.foreverliving.com").strip()
    port = int(os.environ.get("REDSHIFT_PORT", "5445").strip())
    dbname = os.environ.get("REDSHIFT_DB", "flpprod").strip()
    user = os.environ.get("REDSHIFT_USER", "nsuser").strip()
    password = os.environ.get("REDSHIFT_PASSWORD", "").strip()
    if not password:
        print("Set REDSHIFT_PASSWORD (see .env.example)", file=sys.stderr)
        sys.exit(1)
    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }


def yesterday_pdt() -> str:
    yesterday = datetime.now(PDT).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def stats_csv_path(stat_date: str) -> Path:
    return OUTPUT_DIR / f"{STATS_CSV_PREFIX}_{stat_date}.csv"


def find_yesterday_csv() -> Path | None:
    path = stats_csv_path(yesterday_pdt())
    return path if path.exists() else None


def load_csv(file_path: Path) -> list[dict]:
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        raise SystemExit(1)

    with file_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"📄 Loaded {len(rows)} rows from {file_path.name}")
    return rows


def to_nullable(value: str, cast=None):
    if value == "" or value is None:
        return None
    if cast:
        try:
            return cast(value)
        except (ValueError, TypeError):
            return None
    return value


def build_records(rows: list[dict]) -> list[tuple]:
    records = []
    for row in rows:
        records.append((
            to_nullable(row.get("stat_date")),
            to_nullable(row.get("mapping_task_name")),
            to_nullable(row.get("parent_taskflow")),
            to_nullable(row.get("location")),
            to_nullable(row.get("status")),
            to_nullable(row.get("start_time_utc")),
            to_nullable(row.get("end_time_utc")),
            to_nullable(row.get("duration_sec"), cast=int),
            to_nullable(row.get("error_message")),
        ))
    return records


def insert_batch(cur, batch: list[tuple]) -> None:
    args = ",".join(
        cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", rec).decode("utf-8")
        for rec in batch
    )
    cur.execute(INSERT_SQL + args)


def insert_to_redshift(records: list[tuple], redshift_config: dict) -> None:
    conn = None
    try:
        print("Connecting to Redshift...")
        conn = psycopg2.connect(**redshift_config)
        print("✅ Connected!")
        cur = conn.cursor()

        total = len(records)
        inserted = 0

        for i in range(0, total, BATCH_SIZE):
            batch = records[i: i + BATCH_SIZE]
            insert_batch(cur, batch)
            inserted += len(batch)
            print(f"   Inserted {inserted}/{total} rows...", flush=True)

        conn.commit()
        print(f"✅ Done! {total} rows inserted into powerbipoc.fact_mapping_execution")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Insert failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def archive_csv(file_path: Path) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    dest = ARCHIVE_DIR / file_path.name
    if dest.exists():
        dest.unlink()
    shutil.move(str(file_path), str(dest))
    print(f"📦 Archived to {dest}")
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load yesterday's Informatica_runs_stats CSV to Redshift"
    )
    parser.add_argument(
        "--file",
        help=(
            "Optional path to a specific CSV file. "
            "Default: output/Informatica_runs_stats_<yesterday PDT>.csv"
        ),
    )
    args = parser.parse_args()

    if args.file:
        csv_path = Path(args.file)
    else:
        stat_date = yesterday_pdt()
        csv_path = stats_csv_path(stat_date)
        if not csv_path.exists():
            print(
                f"No file found for yesterday ({stat_date}): {csv_path.name}\n"
                "Nothing to load."
            )
            return

    rows = load_csv(csv_path)
    if not rows:
        print(f"⚠️  {csv_path.name} is empty — skipping Redshift load and archive.")
        return

    records = build_records(rows)
    redshift_config = load_redshift_config()
    insert_to_redshift(records, redshift_config)
    archive_csv(csv_path)


if __name__ == "__main__":
    main()
