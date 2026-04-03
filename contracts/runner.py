import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pandas as pd
import yaml

UUID_RE = re.compile(r"^[0-9a-f-]{36}$")


def load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    bad: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                bad.append(f"line {lineno}: {e}")
    if bad:
        print(f"Warning: skipped {len(bad)} bad JSONL line(s) in {path}")
    if not rows:
        raise ValueError(f"No valid JSON rows in {path}")
    return rows


def _flatten_token_count(base: dict, r: dict) -> None:
    tc = r.get("token_count")
    if isinstance(tc, dict):
        for tk, tv in tc.items():
            base[f"token_count_{tk}"] = tv


def _flatten_payload_metadata(base: dict, r: dict) -> None:
    pl = r.get("payload")
    if isinstance(pl, dict):
        for pk, pv in pl.items():
            base[f"payload_{pk}"] = pv
    md = r.get("metadata")
    if isinstance(md, dict):
        for mk, mv in md.items():
            base[f"metadata_{mk}"] = mv


def flatten_record(record: dict) -> list[dict]:
    """Must match contracts/generator.py flatten_record logic."""
    base: dict = {}
    for key, value in record.items():
        if key in ("extracted_facts", "entities"):
            continue
        if key == "token_count":
            continue
        if key == "payload" or key == "metadata":
            continue
        if isinstance(value, (list, dict)):
            continue
        base[key] = value

    _flatten_token_count(base, record)
    _flatten_payload_metadata(base, record)

    rows: list[dict] = []
    facts = record.get("extracted_facts")
    if isinstance(facts, list) and facts:
        for fact in facts:
            row = dict(base)
            if isinstance(fact, dict):
                row.update({f"fact_{k}": v for k, v in fact.items()})
            rows.append(row)
    else:
        rows.append(base)
    return rows


def flatten(records: list[dict]) -> pd.DataFrame:
    rows = []
    for rec in records:
        rows.extend(flatten_record(rec))
    return pd.DataFrame(rows)


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def check_statistical_drift(column: str, current_mean: float, current_std: float, baselines: dict):
    if column not in baselines:
        return None
    b = baselines[column]
    z_score = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)
    if z_score > 3:
        return {
            "status": "FAIL",
            "z_score": round(z_score, 2),
            "message": f"{column} mean drifted {z_score:.1f} stddev from baseline",
        }
    if z_score > 2:
        return {
            "status": "WARN",
            "z_score": round(z_score, 2),
            "message": f"{column} mean within warning range ({z_score:.1f} stddev)",
        }
    return {"status": "PASS", "z_score": round(z_score, 2)}


def mk_result(
    check_id: str,
    column_name: str,
    check_type: str,
    status: str,
    actual: str,
    expected: str,
    severity: str,
    message: str,
    records_failing: int = 0,
    sample_failing=None,
):
    if sample_failing is None:
        sample_failing = []
    return {
        "check_id": check_id,
        "column_name": column_name,
        "check_type": check_type,
        "status": status,
        "actual_value": actual,
        "expected": expected,
        "severity": severity,
        "records_failing": int(records_failing),
        "sample_failing": sample_failing[:5],
        "message": message,
    }


def load_baselines(path: Path, contract_id: str) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if "contracts" in payload and contract_id in payload["contracts"]:
        return payload["contracts"][contract_id].get("columns", {})
    return {}


def save_baselines(path: Path, contract_id: str, df: pd.DataFrame) -> None:
    baselines = {}
    for col in df.select_dtypes(include="number").columns:
        baselines[col] = {
            "mean": float(df[col].mean()),
            "stddev": float(df[col].std() if not pd.isna(df[col].std()) else 0.0),
        }
    payload: dict = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    if "contracts" not in payload:
        payload["contracts"] = {}
    payload["contracts"][contract_id] = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "columns": baselines,
    }
    payload["written_at"] = datetime.now(timezone.utc).isoformat()
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def parse_ts(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def should_block(mode: str, results: list[dict]) -> tuple[bool, str]:
    """AUDIT: never block. WARN: block on FAIL+CRITICAL. ENFORCE: block on FAIL+CRITICAL|HIGH."""
    for r in results:
        if r.get("status") != "FAIL":
            continue
        sev = r.get("severity", "LOW")
        if mode == "WARN" and sev == "CRITICAL":
            return True, f"check_id={r.get('check_id')} severity=CRITICAL"
        if mode == "ENFORCE" and sev in ("CRITICAL", "HIGH"):
            return True, f"check_id={r.get('check_id')} severity={sev}"
    return False, ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Run validation checks for a generated contract.")
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--mode",
        choices=("AUDIT", "WARN", "ENFORCE"),
        default="AUDIT",
        help="AUDIT=log only; WARN=exit 1 on FAIL+CRITICAL; ENFORCE=exit 1 on FAIL+CRITICAL|HIGH.",
    )
    parser.add_argument(
        "--no-baseline-write",
        action="store_true",
        help="Do not write schema_snapshots/baselines.json (use on violated data after clean baseline exists).",
    )
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    Path("schema_snapshots").mkdir(exist_ok=True)

    with args.contract.open("r", encoding="utf-8") as f:
        contract = yaml.safe_load(f)
    contract_id = contract.get("id", "unknown-contract")
    records = load_jsonl(args.data)
    df = flatten(records)

    schema = contract.get("schema", {})
    results = []

    for col, clause in schema.items():
        check_prefix = f"{contract_id}.{col}"
        if col not in df.columns:
            results.append(
                mk_result(
                    f"{check_prefix}.exists",
                    col,
                    "exists",
                    "ERROR",
                    "missing column",
                    "column present",
                    "CRITICAL",
                    "Column missing in flattened dataset.",
                )
            )
            continue

        series = df[col]
        non_null = series.dropna()

        if clause.get("required"):
            null_count = int(series.isna().sum())
            status = "PASS" if null_count == 0 else "FAIL"
            results.append(
                mk_result(
                    f"{check_prefix}.required",
                    col,
                    "required",
                    status,
                    f"nulls={null_count}",
                    "nulls=0",
                    "CRITICAL",
                    "Required field contains nulls." if null_count else "Required field check passed.",
                    records_failing=null_count,
                )
            )

        expected_type = clause.get("type")
        if expected_type == "number":
            is_num = pd.api.types.is_numeric_dtype(series)
            status = "PASS" if is_num else "FAIL"
            results.append(
                mk_result(
                    f"{check_prefix}.type",
                    col,
                    "type",
                    status,
                    str(series.dtype),
                    "numeric",
                    "CRITICAL",
                    "Numeric type mismatch." if not is_num else "Numeric type check passed.",
                )
            )
        elif expected_type == "integer":
            is_int = pd.api.types.is_integer_dtype(series)
            status = "PASS" if is_int else "FAIL"
            results.append(
                mk_result(
                    f"{check_prefix}.type",
                    col,
                    "type",
                    status,
                    str(series.dtype),
                    "integer",
                    "CRITICAL",
                    "Integer type mismatch." if not is_int else "Integer type check passed.",
                )
            )

        if "enum" in clause:
            allowed = set(str(x) for x in clause["enum"])
            bad = [str(v) for v in non_null if str(v) not in allowed]
            status = "PASS" if not bad else "FAIL"
            results.append(
                mk_result(
                    f"{check_prefix}.enum",
                    col,
                    "enum",
                    status,
                    f"non_conforming={len(bad)}",
                    f"in {sorted(allowed)}",
                    "CRITICAL" if bad else "LOW",
                    "Enum conformance failed." if bad else "Enum conformance passed.",
                    records_failing=len(bad),
                    sample_failing=bad[:5],
                )
            )

        if clause.get("format") == "uuid":
            invalid = [str(v) for v in non_null if not UUID_RE.match(str(v))]
            status = "PASS" if not invalid else "FAIL"
            results.append(
                mk_result(
                    f"{check_prefix}.uuid",
                    col,
                    "format_uuid",
                    status,
                    f"invalid={len(invalid)}",
                    "all values match ^[0-9a-f-]{36}$",
                    "CRITICAL" if invalid else "LOW",
                    "UUID format validation failed." if invalid else "UUID format validation passed.",
                    records_failing=len(invalid),
                    sample_failing=invalid[:5],
                )
            )

        pat = clause.get("pattern")
        if pat and clause.get("type") == "string":
            try:
                cre = re.compile(pat)
            except re.error:
                cre = None
            if cre:
                bad = [str(v) for v in non_null if not cre.search(str(v))]
                status = "PASS" if not bad else "FAIL"
                results.append(
                    mk_result(
                        f"{check_prefix}.pattern",
                        col,
                        "pattern",
                        status,
                        f"non_matching={len(bad)}",
                        f"regex {pat}",
                        "CRITICAL" if bad else "LOW",
                        "Pattern validation failed." if bad else "Pattern validation passed.",
                        records_failing=len(bad),
                        sample_failing=bad[:5],
                    )
                )

        if clause.get("format") == "date-time":
            unparseable = []
            for v in non_null:
                s = str(v).replace("Z", "+00:00")
                try:
                    datetime.fromisoformat(s)
                except ValueError:
                    unparseable.append(str(v))
            status = "PASS" if not unparseable else "FAIL"
            results.append(
                mk_result(
                    f"{check_prefix}.datetime",
                    col,
                    "format_datetime",
                    status,
                    f"unparseable={len(unparseable)}",
                    "ISO-8601 parseable",
                    "CRITICAL" if unparseable else "LOW",
                    "Date-time format validation failed." if unparseable else "Date-time format validation passed.",
                    records_failing=len(unparseable),
                    sample_failing=unparseable[:5],
                )
            )

        if "minimum" in clause or "maximum" in clause:
            if pd.api.types.is_numeric_dtype(series) and not non_null.empty:
                observed_min = float(non_null.min())
                observed_max = float(non_null.max())
                lo = float(clause.get("minimum", observed_min))
                hi = float(clause.get("maximum", observed_max))
                min_ok = observed_min >= lo
                max_ok = observed_max <= hi
                status = "PASS" if (min_ok and max_ok) else "FAIL"
                failing = int(
                    ((series < float(clause.get("minimum", float("-inf")))) & series.notna()).sum()
                    + ((series > float(clause.get("maximum", float("inf")))) & series.notna()).sum()
                )
                results.append(
                    mk_result(
                        f"{check_prefix}.range",
                        col,
                        "range",
                        status,
                        f"min={observed_min}, max={observed_max}, mean={float(non_null.mean()):.3f}",
                        f"min>={clause.get('minimum','-inf')}, max<={clause.get('maximum','inf')}",
                        "CRITICAL" if status == "FAIL" else "LOW",
                        "Range check failed." if status == "FAIL" else "Range check passed.",
                        records_failing=max(failing, 0),
                    )
                )
            elif not pd.api.types.is_numeric_dtype(series):
                results.append(
                    mk_result(
                        f"{check_prefix}.range",
                        col,
                        "range",
                        "ERROR",
                        "non-numeric column",
                        "numeric required for range",
                        "CRITICAL",
                        "Cannot execute range check on non-numeric data.",
                    )
                )

        if clause.get("unique"):
            dup = int(series.duplicated().sum())
            status = "PASS" if dup == 0 else "FAIL"
            results.append(
                mk_result(
                    f"{check_prefix}.unique",
                    col,
                    "unique",
                    status,
                    f"duplicates={dup}",
                    "duplicates=0",
                    "HIGH" if dup else "LOW",
                    "Uniqueness violated." if dup else "Uniqueness check passed.",
                    records_failing=dup,
                )
            )

    if "occurred_at" in df.columns and "recorded_at" in df.columns:
        bad_rows = []
        for i, row in df.iterrows():
            o = parse_ts(row.get("occurred_at"))
            r = parse_ts(row.get("recorded_at"))
            if o is None or r is None:
                continue
            if r < o:
                bad_rows.append(str(i))
        status = "PASS" if not bad_rows else "FAIL"
        results.append(
            mk_result(
                f"{contract_id}.recorded_at_ge_occurred_at",
                "recorded_at,occurred_at",
                "temporal_order",
                status,
                f"violations={len(bad_rows)}",
                "recorded_at >= occurred_at",
                "CRITICAL" if bad_rows else "LOW",
                "Temporal order violated." if bad_rows else "recorded_at >= occurred_at for all parseable rows.",
                records_failing=len(bad_rows),
                sample_failing=bad_rows[:5],
            )
        )

    baseline_path = Path("schema_snapshots/baselines.json")
    baseline_columns = load_baselines(baseline_path, contract_id)

    for col in df.select_dtypes(include="number").columns:
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        drift = check_statistical_drift(
            col,
            float(non_null.mean()),
            float(non_null.std() if not pd.isna(non_null.std()) else 0.0),
            baseline_columns,
        )
        if drift is None:
            continue
        status = drift["status"]
        severity = "HIGH" if status == "FAIL" else ("MEDIUM" if status == "WARN" else "LOW")
        results.append(
            mk_result(
                f"{contract_id}.{col}.drift",
                col,
                "statistical_drift",
                status,
                f"z={drift['z_score']}",
                "<=2 warn, <=3 fail threshold",
                severity,
                drift.get("message", "No drift detected."),
            )
        )

    if not baseline_columns and not args.no_baseline_write:
        save_baselines(baseline_path, contract_id, df)

    status_counts = {"PASS": 0, "FAIL": 0, "WARN": 0, "ERROR": 0}
    for r in results:
        if r["status"] in status_counts:
            status_counts[r["status"]] += 1

    block, block_reason = should_block(args.mode, results)
    pipeline_action = "BLOCK" if block else "PASS"

    report = {
        "report_id": str(uuid4()),
        "contract_id": contract_id,
        "snapshot_id": hash_file(args.data),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "validation_mode": args.mode,
        "pipeline_action": pipeline_action,
        "block_reason": block_reason if block else None,
        "total_checks": len(results),
        "passed": status_counts["PASS"],
        "failed": status_counts["FAIL"],
        "warned": status_counts["WARN"],
        "errored": status_counts["ERROR"],
        "results": results,
    }

    with args.output.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"Validation report written: {args.output}")
    print(f"Mode={args.mode} pipeline_action={pipeline_action}")

    if args.mode != "AUDIT" and block:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
