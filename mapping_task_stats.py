#!/usr/bin/env python3
"""
Mapping task (MTT) stats — separate from taskflow-level reporting.

Counts mapping task executions only, so nested taskflows are not double-counted
at the parent taskflow level. Each row is one MTT run.

Default output: output/Informatica_runs_stats_YYYY-MM-DD.csv (one file per stat day).
Default source: taskflow status API subtasks (assetType=MTT only).
Optional source: activity log (type=MTT, including child entries).

Usage:
  export IICS_USERNAME=... IICS_PASSWORD=... IICS_POD_URL=...
  python mapping_task_stats.py --date 2026-05-24
  python mapping_task_stats.py --days 7
  python mapping_task_stats.py --date 2026-05-24 --source activity
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

PDT = timezone(timedelta(hours=-7))
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
STATS_CSV_PREFIX = "Informatica_runs_stats"


def stats_csv_path(stat_date: str) -> Path:
    return OUTPUT_DIR / f"{STATS_CSV_PREFIX}_{stat_date}.csv"

ACTIVITY_ROW_LIMIT = 200
TASKFLOW_ROW_LIMIT = 50
REQUEST_TIMEOUT = 180


@dataclass
class Session:
    ic_session_id: str
    server_url: str

    @property
    def tf_base_url(self) -> str:
        url = self.server_url.rstrip("/")
        if url.endswith("/saas"):
            return url[: -len("/saas")]
        return url

    def v2_headers(self) -> dict[str, str]:
        return {
            "icSessionId": self.ic_session_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def v3_headers(self) -> dict[str, str]:
        return {
            "INFA-SESSION-ID": self.ic_session_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }


def load_credentials() -> tuple[str, str, str]:
    username = os.environ.get("IICS_USERNAME", "").strip()
    password = os.environ.get("IICS_PASSWORD", "").strip()
    pod_url = os.environ.get("IICS_POD_URL", "https://dm-us.informaticacloud.com").strip().rstrip("/")
    if not username or not password:
        print("Set IICS_USERNAME and IICS_PASSWORD (see .env.example)", file=sys.stderr)
        sys.exit(1)
    return username, password, pod_url


def login(pod_url: str, username: str, password: str) -> Session:
    resp = requests.post(
        f"{pod_url}/ma/api/v2/user/login",
        json={"username": username, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return Session(ic_session_id=data["icSessionId"], server_url=data["serverUrl"].rstrip("/"))


def pdt_day_to_utc_range(day: datetime) -> tuple[datetime, datetime]:
    if day.tzinfo is None:
        day = day.replace(tzinfo=PDT)
    start_pdt = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end_pdt = day.replace(hour=23, minute=59, second=59, microsecond=0)
    return start_pdt.astimezone(timezone.utc), end_pdt.astimezone(timezone.utc)


def utc_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def is_success_state(state: Any) -> bool:
    s = str(state).upper()
    return s in ("1", "SUCCESS", "COMPLETED", "SUCCEEDED")


def duration_from_activity(rec: dict[str, Any]) -> float | None:
    start = parse_utc(rec.get("startTimeUtc"))
    end = parse_utc(rec.get("endTimeUtc"))
    if start and end:
        return (end - start).total_seconds()
    try:
        return int(rec.get("runTime", 0)) / 1000
    except (TypeError, ValueError):
        return None


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fetch_taskflow_runs(
    session: Session,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    url = f"{session.tf_base_url}/active-bpel/services/tf/status"
    all_runs: list[dict[str, Any]] = []
    offset = 0

    while True:
        params = {
            "startTime": utc_iso(start_utc),
            "endTime": utc_iso(end_utc),
            "rowLimit": TASKFLOW_ROW_LIMIT,
            "offset": offset,
            "subtaskDetails": "Yes",
        }
        resp = requests.get(
            url,
            headers=session.v3_headers(),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 401:
            resp = requests.get(
                url,
                headers=session.v2_headers(),
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, dict):
            if data.get("status") == "No status available.":
                break
            batch = [data]
        elif isinstance(data, list):
            batch = data
        else:
            break

        if not batch:
            break
        all_runs.extend(batch)
        print(
            f"   taskflow pages: offset={offset} +{len(batch)} (total {len(all_runs)})",
            flush=True,
        )
        if len(batch) < TASKFLOW_ROW_LIMIT:
            break
        offset += TASKFLOW_ROW_LIMIT
        time.sleep(0.2)

    return all_runs


def mtt_rows_from_taskflow_subtasks(
    taskflow_runs: list[dict[str, Any]],
    stat_date: str,
) -> list[dict[str, Any]]:
    """
    One row per mapping task (MTT) subtask — parent taskflow runs are not counted.
    Nested taskflow subtasks (assetType=TASKFLOW) are skipped here.
    """
    rows: list[dict[str, Any]] = []
    skipped_nested_tf = 0

    for tf in taskflow_runs:
        parent_name = tf.get("assetName")
        parent_run_id = tf.get("runId")
        parent_location = tf.get("location")
        parent_started_by = tf.get("startedBy")
        subtasks = (
            tf.get("subtaskDetails", {})
            .get("details", {})
            .get("tasks", [])
        )

        for st in subtasks:
            asset_type = str(st.get("assetType", "")).upper()
            if asset_type != "MTT":
                if asset_type == "TASKFLOW":
                    skipped_nested_tf += 1
                continue

            status = st.get("status")
            rows.append(
                {
                    "stat_date": stat_date,
                    "source": "taskflow_subtask",
                    "mapping_task_name": st.get("assetName"),
                    "mapping_task_type": "MTT",
                    "parent_taskflow": parent_name,
                    "parent_taskflow_run_id": parent_run_id,
                    "mapping_task_run_id": st.get("runId"),
                    "location": st.get("location") or parent_location,
                    "status": status,
                    "success": is_success_state(status),
                    "start_time_utc": st.get("startTime"),
                    "end_time_utc": st.get("endTime"),
                    "duration_sec": safe_int(st.get("duration")) or None,
                    "rows_processed": safe_int(st.get("rowsProcessed")),
                    "success_rows": safe_int(st.get("successRows")),
                    "error_rows": safe_int(st.get("errorRows")),
                    "started_by": st.get("startedBy") or parent_started_by,
                    "error_message": (st.get("errorMessage") or "")[:500],
                }
            )

    if skipped_nested_tf:
        print(
            f"   note: skipped {skipped_nested_tf} nested taskflow subtasks "
            f"(use --source activity to try capturing deeper MTT runs)",
            flush=True,
        )
    return rows


def _record_key(rec: dict[str, Any]) -> str:
    return f"{rec.get('id')}|{rec.get('runId')}|{rec.get('startTimeUtc')}"


def fetch_activity_logs(
    session: Session,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    url = f"{session.server_url}/api/v2/activity/activityLog"
    all_records: list[dict[str, Any]] = []
    seen: set[str] = set()
    offset = 0
    page = 0
    prev_first: str | None = None

    while True:
        page += 1
        params = {
            "rowLimit": ACTIVITY_ROW_LIMIT,
            "offset": offset,
            "startTime": utc_iso(start_utc),
            "endTime": utc_iso(end_utc),
        }
        resp = requests.get(
            url,
            headers=session.v2_headers(),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        first_key = _record_key(batch[0])
        if page > 1 and first_key == prev_first:
            print("   activity log pagination repeated; stopping.", flush=True)
            break
        prev_first = first_key

        new_count = 0
        for rec in batch:
            key = _record_key(rec)
            if key not in seen:
                seen.add(key)
                all_records.append(rec)
                new_count += 1

        print(f"   activity page {page}: +{new_count} (total {len(all_records)})", flush=True)
        if len(batch) < ACTIVITY_ROW_LIMIT:
            break
        offset += ACTIVITY_ROW_LIMIT
        time.sleep(0.15)

    return all_records


def _collect_mtt_from_activity_tree(node: dict[str, Any], out: list[dict[str, Any]]) -> None:
    if str(node.get("type", "")).upper() == "MTT":
        out.append(node)
    for child in node.get("entries") or []:
        if isinstance(child, dict):
            _collect_mtt_from_activity_tree(child, out)


def mtt_rows_from_activity_log(
    records: list[dict[str, Any]],
    stat_date: str,
) -> list[dict[str, Any]]:
    """Extract MTT runs from activity log (top-level + nested entries)."""
    mtt_records: list[dict[str, Any]] = []
    for rec in records:
        _collect_mtt_from_activity_tree(rec, mtt_records)

    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for rec in mtt_records:
        key = _record_key(rec)
        if key in seen:
            continue
        seen.add(key)

        state = rec.get("state")
        rows.append(
            {
                "stat_date": stat_date,
                "source": "activity_log",
                "mapping_task_name": rec.get("objectName"),
                "mapping_task_type": "MTT",
                "object_id": rec.get("objectId"),
                "parent_taskflow": None,
                "parent_taskflow_run_id": rec.get("contextExternalId"),
                "mapping_task_run_id": rec.get("runId"),
                "location": None,
                "status": state,
                "success": is_success_state(state),
                "start_time_utc": rec.get("startTimeUtc"),
                "end_time_utc": rec.get("endTimeUtc"),
                "duration_sec": duration_from_activity(rec),
                "rows_processed": safe_int(rec.get("successTargetRows")),
                "success_rows": safe_int(rec.get("successTargetRows")),
                "error_rows": safe_int(rec.get("failedTargetRows")),
                "success_source_rows": safe_int(rec.get("successSourceRows")),
                "failed_source_rows": safe_int(rec.get("failedSourceRows")),
                "schedule_name": rec.get("scheduleName"),
                "run_context": rec.get("runContextType"),
                "started_by": rec.get("startedBy"),
                "error_message": (rec.get("errorMsg") or "")[:500],
            }
        )
    return rows


def aggregate_by_mapping_task(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "total_runs": 0,
            "success_runs": 0,
            "failed_runs": 0,
            "durations": [],
            "rows_processed": 0,
            "success_rows": 0,
            "error_rows": 0,
        }
    )
    for r in rows:
        name = r.get("mapping_task_name") or "?"
        b = buckets[name]
        b["total_runs"] += 1
        if r.get("success"):
            b["success_runs"] += 1
        else:
            b["failed_runs"] += 1
        if r.get("duration_sec") is not None:
            b["durations"].append(float(r["duration_sec"]))
        b["rows_processed"] += safe_int(r.get("rows_processed"))
        b["success_rows"] += safe_int(r.get("success_rows"))
        b["error_rows"] += safe_int(r.get("error_rows"))

    summary = []
    for name, b in sorted(buckets.items()):
        durs = b["durations"]
        summary.append(
            {
                "mapping_task_name": name,
                "total_runs": b["total_runs"],
                "success_runs": b["success_runs"],
                "failed_runs": b["failed_runs"],
                "avg_duration_sec": round(sum(durs) / len(durs), 1) if durs else None,
                "max_duration_sec": round(max(durs), 1) if durs else None,
                "rows_processed": b["rows_processed"],
                "success_rows": b["success_rows"],
                "error_rows": b["error_rows"],
            }
        )
    return summary


@dataclass
class DayResult:
    stat_date: str
    rows: list[dict[str, Any]] = field(default_factory=list)


def collect_day(
    session: Session,
    pdt_day: datetime,
    source: str,
) -> DayResult:
    start_utc, end_utc = pdt_day_to_utc_range(pdt_day)
    stat_date = pdt_day.strftime("%Y-%m-%d")
    print(f"\n── {stat_date} PDT ({utc_iso(start_utc)} → {utc_iso(end_utc)})")

    if source == "activity":
        print("   Fetching activity log (MTT only)...")
        raw = fetch_activity_logs(session, start_utc, end_utc)
        rows = mtt_rows_from_activity_log(raw, stat_date)
    else:
        print("   Fetching taskflow runs → extracting MTT subtasks...")
        tf_runs = fetch_taskflow_runs(session, start_utc, end_utc)
        rows = mtt_rows_from_taskflow_subtasks(tf_runs, stat_date)

    print(f"   → {len(rows)} mapping task runs")
    return DayResult(stat_date=stat_date, rows=rows)


def print_report(days: list[DayResult], output_files: list[Path], source: str) -> None:
    all_rows = [r for d in days for r in d.rows]
    total = len(all_rows)
    ok = sum(1 for r in all_rows if r.get("success"))
    unique_tasks = len({r.get("mapping_task_name") for r in all_rows})

    print("\n" + "=" * 70)
    print("  MAPPING TASK (MTT) STATS REPORT")
    print(f"  Source: {source}")
    print("=" * 70)

    for d in days:
        n = len(d.rows)
        ok_d = sum(1 for r in d.rows if r.get("success"))
        uniq = len({r.get("mapping_task_name") for r in d.rows})
        print(f"\n📅 {d.stat_date}")
        print(f"   MTT runs          : {n}")
        print(f"   Unique MTT names  : {uniq}")
        if n:
            print(f"   Success           : {ok_d}/{n} ({100 * ok_d // n}%)")

        by_job = aggregate_by_mapping_task(d.rows)
        top = sorted(by_job, key=lambda x: x["total_runs"], reverse=True)[:10]
        if top:
            print("   Top mapping tasks by run count:")
            for j in top:
                print(
                    f"      {j['mapping_task_name'][:45]:<45} "
                    f"runs={j['total_runs']:>4} ok={j['success_runs']}"
                )

    print(f"\n📊 PERIOD TOTAL")
    print(f"   MTT runs          : {total}")
    print(f"   Unique MTT names  : {unique_tasks}")
    if total:
        print(f"   Success           : {ok}/{total} ({100 * ok // total}%)")
    print("\n💾 Output files:")
    for path in output_files:
        print(f"   {path}")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect Informatica mapping task (MTT) run stats"
    )
    parser.add_argument("--date", help="Single PDT day YYYY-MM-DD")
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of complete PDT days ending yesterday (default: 1)",
    )
    parser.add_argument(
        "--source",
        choices=("subtasks", "activity"),
        default="subtasks",
        help=(
            "subtasks=MTT from taskflow status API (fast, default); "
            "activity=MTT from activity log (slower, includes standalone MTT)"
        ),
    )
    args = parser.parse_args()

    username, password, pod_url = load_credentials()
    print(f"Logging in to {pod_url}...")
    session = login(pod_url, username, password)
    print(f"✅ Session OK | {session.server_url}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_files: list[Path] = []

    if args.date:
        pdt_days = [datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=PDT)]
    else:
        yesterday = datetime.now(PDT).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        pdt_days = [yesterday - timedelta(days=i) for i in range(args.days - 1, -1, -1)]

    day_results: list[DayResult] = []
    all_rows: list[dict[str, Any]] = []

    for pdt_day in pdt_days:
        dr = collect_day(session, pdt_day, args.source)
        day_results.append(dr)
        all_rows.extend(dr.rows)
        csv_path = stats_csv_path(dr.stat_date)
        write_csv(csv_path, dr.rows)
        if csv_path.exists():
            output_files.append(csv_path)
        # summary = aggregate_by_mapping_task(dr.rows)
        # for row in summary:
        #     row["stat_date"] = dr.stat_date
        # write_csv(out_dir / f"daily_summary_{dr.stat_date}.csv", summary)

    # write_csv(out_dir / "mapping_task_runs_all.csv", all_rows)
    # # period_summary = []
    # # for dr in day_results:
    # #     for row in aggregate_by_mapping_task(dr.rows):
    # #         row["stat_date"] = dr.stat_date
    # #         period_summary.append(row)
    # # write_csv(out_dir / "period_summary_by_mapping_task.csv", period_summary)

    print_report(day_results, output_files, args.source)


if __name__ == "__main__":
    main()
