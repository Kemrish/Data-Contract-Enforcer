"""
Enforcer Report — aggregates validation_reports, violation_log, ai_extensions → report_data.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

import registry_util

SEVERITY_DEDUCTIONS = {"CRITICAL": 20, "HIGH": 10, "MEDIUM": 5, "LOW": 1, "WARNING": 2}


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_registry_subscribers(registry_path: Path, contract_id: str) -> list[str]:
    if not registry_path.exists():
        return []
    with registry_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return [
        s.get("subscriber_id")
        for s in data.get("subscriptions") or []
        if s.get("contract_id") == contract_id
    ]


def collect_validation_reports(reports_dir: Path) -> list[dict]:
    skip = {"ai_extensions.json", "schema_evolution.json"}
    reports = []
    for p in sorted(reports_dir.glob("*.json")):
        if p.name.startswith("migration_impact") or p.name in skip:
            continue
        try:
            data = load_json(p)
            if "results" not in data:
                continue
            reports.append(data)
        except (json.JSONDecodeError, OSError):
            continue
    return reports


def score_single_report(rep: dict) -> int:
    total = max(rep.get("total_checks", 0), 1)
    passed = rep.get("passed", 0)
    base = int(round((passed / total) * 100))
    for r in rep.get("results") or []:
        if r.get("status") in ("FAIL", "ERROR"):
            base -= SEVERITY_DEDUCTIONS.get(r.get("severity", "LOW"), 1)
    return max(0, min(100, base))


def compute_health_score(reports: list[dict]) -> tuple[int, list[dict], dict[str, int]]:
    all_fails: list[dict] = []
    sev_counts: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    per_report_scores: list[int] = []
    for rep in reports:
        cid = rep.get("contract_id", "")
        per_report_scores.append(score_single_report(rep))
        if not rep.get("results"):
            continue
        for r in rep.get("results") or []:
            if r.get("status") in ("FAIL", "ERROR"):
                row = dict(r)
                row["contract_id"] = cid
                all_fails.append(row)
                sev = r.get("severity", "LOW")
                sev_counts[sev] = sev_counts.get(sev, 0) + 1

    score = min(per_report_scores) if per_report_scores else 100
    return score, all_fails, sev_counts


def load_violation_log_plain(path: Path) -> list[str]:
    lines_out: list[str] = []
    if not path.exists():
        return lines_out
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        try:
            o = json.loads(ln)
        except json.JSONDecodeError:
            continue
        cid = o.get("contract_id", "?")
        chk = o.get("check_id", "?")
        src = o.get("source_component", "contract_validation")
        lines_out.append(
            f"[{src}] {cid}: {chk} — injection_note={o.get('injection_note', False)}"
        )
    return lines_out[-10:]


def latest_migration_report(reports_dir: Path) -> dict[str, Any] | None:
    files = sorted(reports_dir.glob("migration_impact_*.json"))
    if not files:
        return None
    try:
        return load_json(files[-1])
    except (json.JSONDecodeError, OSError):
        return None


def schema_evolution_plain(reports_dir: Path) -> list[str]:
    p = reports_dir / "schema_evolution.json"
    if not p.exists():
        return ["No schema_evolution.json found — run contracts/schema_analyzer.py."]
    try:
        ev = load_json(p)
    except (json.JSONDecodeError, OSError):
        return ["schema_evolution.json could not be read."]
    out = []
    for ch in ev.get("changes") or []:
        if ch.get("verdict") == "BREAKING":
            out.append(f"BREAKING: {ch.get('field')} — {ch.get('message')}")
    if not out:
        out.append("No breaking schema deltas in the compared snapshot pair.")
    mig = latest_migration_report(reports_dir)
    if mig:
        out.append(
            f"Overall compatibility: {mig.get('compatibility_summary', {}).get('overall')}. "
            f"See {mig.get('generated_at', '')} migration file for checklist and rollback."
        )
    return out


def plain_language(
    result: dict,
    registry_path: Path,
) -> str:
    contract_id = result.get("contract_id") or result.get("check_id", "").split(".")[0] or "unknown-contract"
    subs = load_registry_subscribers(registry_path, contract_id)
    sub_str = ", ".join(s for s in subs if s) or "no registered subscribers"
    col = result.get("column_name", "unknown field")
    return (
        f"System contract `{contract_id}`: field '{col}' failed {result.get('check_type')} check. "
        f"Expected {result.get('expected')}; observed {result.get('actual_value')}. "
        f"Downstream subscribers: {sub_str}. "
        f"Failing records (approx): {result.get('records_failing', 'n/a')}."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate enforcer_report/report_data.json")
    parser.add_argument("--reports-dir", type=Path, default=Path("validation_reports"))
    parser.add_argument("--violations", type=Path, default=Path("violation_log/violations.jsonl"))
    parser.add_argument("--ai-extensions", type=Path, default=Path("validation_reports/ai_extensions.json"))
    parser.add_argument("--registry", type=Path, default=Path("contract_registry/subscriptions.yaml"))
    parser.add_argument("--output", type=Path, default=Path("enforcer_report/report_data.json"))
    args = parser.parse_args()

    reports = collect_validation_reports(args.reports_dir)
    score, all_fails, sev_counts = compute_health_score(reports)

    sev_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    top3 = sorted(
        all_fails,
        key=lambda x: sev_order.index(x.get("severity", "LOW")) if x.get("severity") in sev_order else 99,
    )[:3]
    top_plain = [plain_language(v, args.registry.resolve()) for v in top3]

    violations_n = 0
    violation_plain = []
    if args.violations.exists():
        violations_n = sum(
            1
            for ln in args.violations.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.startswith("#")
        )
        violation_plain = load_violation_log_plain(args.violations)

    reg_report = registry_util.validate_registry(args.registry.resolve())

    ai: dict[str, Any] = {}
    if args.ai_extensions.exists():
        ai = load_json(args.ai_extensions)

    now = datetime.now(timezone.utc)
    period_start = (now - timedelta(days=7)).date()
    narrative = (
        f"Data health score {score}/100 based on {len(reports)} validation report(s). "
        + (
            "All monitored checks within configured thresholds."
            if score >= 90
            else f"Attention required: {sev_counts.get('CRITICAL', 0)} critical issue(s)."
        )
    )

    schema_plain = schema_evolution_plain(args.reports_dir.resolve())

    ai_plain = (
        f"Embedding: {ai.get('embedding_drift', {}).get('status', 'n/a')} "
        f"(score={ai.get('embedding_drift', {}).get('drift_score')}). "
        f"Prompt inputs quarantined: {ai.get('prompt_input_validation', {}).get('quarantined_count', 0)}. "
        f"Verdict schema violations: {ai.get('llm_output_schema_violation_rate', {}).get('schema_violations', 0)} "
        f"at rate {ai.get('llm_output_schema_violation_rate', {}).get('violation_rate', 0)}."
    )

    report = {
        "generated_at": now.isoformat(),
        "period": f"{period_start} to {now.date()}",
        "data_health_score": score,
        "health_narrative": narrative,
        "registry_validation": {
            "coverage_ok": reg_report.get("coverage_ok"),
            "warnings": reg_report.get("warnings", []),
            "contract_ids_present": reg_report.get("contract_ids_present", []),
        },
        "violations_this_week": {
            "by_severity": sev_counts,
            "total_failed_checks": len(all_fails),
            "violation_log_entries": violations_n,
            "violation_log_plain_language": violation_plain,
        },
        "top_violations_plain_language": top_plain,
        "schema_changes_detected_plain_language": schema_plain,
        "schema_changes_summary": " ".join(schema_plain[:5]),
        "ai_risk_assessment": {
            "embedding_drift": ai.get("embedding_drift", {}),
            "prompt_input_validation": ai.get("prompt_input_validation", {}),
            "llm_output_violation_rate": ai.get("llm_output_schema_violation_rate", {}),
            "summary_plain_language": ai_plain,
            "summary": (
                "Embedding drift within threshold"
                if ai.get("embedding_drift", {}).get("status") == "PASS"
                else str(ai.get("embedding_drift", {}).get("message", "see embedding_drift"))
            ),
        },
        "recommended_actions_prioritized": [
            {
                "priority": 1,
                "action": "Restore Week 3 `fact_confidence` to float 0.0–1.0 in `src/week3/extractor.py` if range/drift FAIL appears.",
                "risk_reduced": "Silent ranking corruption for Week 4 cartographer consumers.",
            },
            {
                "priority": 2,
                "action": "Run `contracts/runner.py --mode ENFORCE` in CI for published JSONL artifacts.",
                "risk_reduced": "Blocks bad data before downstream business logic.",
            },
            {
                "priority": 3,
                "action": "Set `OPENAI_API_KEY` and re-run `contracts/ai_extensions.py` to establish embedding baseline.",
                "risk_reduced": "Semantic drift visibility on extraction text.",
            },
        ],
        "recommendations": [
            "If confidence range FAIL: restore float 0.0–1.0 in src/week3/extractor.py to match contract week3-document-refinery-extractions clause fact_confidence.range.",
            "Add contracts/runner.py --mode ENFORCE as a CI gate before Week 3 artifact publish.",
            "Refresh schema_snapshots/baselines.json after intentional schema migrations (monthly or on schema change).",
        ],
        "sources": {
            "validation_reports": [str(p) for p in args.reports_dir.glob("*.json")],
            "violation_log": str(args.violations),
            "ai_extensions": str(args.ai_extensions) if args.ai_extensions.exists() else None,
        },
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {args.output} (data_health_score={score})")


if __name__ == "__main__":
    main()
