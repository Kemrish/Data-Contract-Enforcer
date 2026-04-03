"""
SchemaEvolutionAnalyzer — diff timestamped contract snapshots, classify changes, migration impact.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def load_snapshot_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def classify_change(field: str, old: dict | None, new: dict | None) -> tuple[str, str]:
    """Returns (verdict, message) — BREAKING or COMPATIBLE."""
    if old is None and new is not None:
        req = new.get("required", False)
        if req:
            return "BREAKING", f"New required field {field}"
        return "COMPATIBLE", f"New optional field {field}"
    if old is not None and new is None:
        return "BREAKING", f"Field removed: {field}"
    if old is None or new is None:
        return "COMPATIBLE", "No change"
    if old.get("type") != new.get("type"):
        return "BREAKING", f"Type changed {old.get('type')} -> {new.get('type')} for {field}"
    omax = old.get("maximum")
    nmax = new.get("maximum")
    omin = old.get("minimum")
    nmin = new.get("minimum")
    if omax != nmax or omin != nmin:
        return "BREAKING", f"Range changed for {field}: min {omin}->{nmin}, max {omax}->{nmax}"
    old_enum = set(old.get("enum") or [])
    new_enum = set(new.get("enum") or [])
    if old_enum - new_enum:
        return "BREAKING", f"Enum values removed from {field}: {old_enum - new_enum}"
    if new_enum - old_enum:
        return "COMPATIBLE", f"Enum values added to {field}: {new_enum - old_enum}"
    opat = old.get("pattern")
    npat = new.get("pattern")
    if opat != npat and (opat or npat):
        return "BREAKING", f"Regex/pattern changed for {field}"
    ofmt = old.get("format")
    nfmt = new.get("format")
    if ofmt != nfmt:
        return "BREAKING", f"Format changed {ofmt} -> {nfmt} for {field}"
    if old.get("unique") and not new.get("unique"):
        return "BREAKING", f"Unique constraint removed from {field}"
    if not old.get("required") and new.get("required"):
        return "BREAKING", f"Field {field} became required"
    if old.get("required") and not new.get("required"):
        return "COMPATIBLE", f"Field {field} became optional"
    return "COMPATIBLE", f"No material structural change to {field}"


def find_snapshots(contract_id: str, root: Path) -> list[Path]:
    d = root / "schema_snapshots" / contract_id
    if not d.exists():
        return []
    files = sorted(d.glob("*.yaml"))
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description="Diff schema snapshots for a contract.")
    parser.add_argument("--contract-id", required=True, help="e.g. week3-document-refinery-extractions")
    parser.add_argument(
        "--snapshot-a",
        type=Path,
        default=None,
        help="First snapshot YAML (older)",
    )
    parser.add_argument(
        "--snapshot-b",
        type=Path,
        default=None,
        help="Second snapshot YAML (newer)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default="",
        help="Ignored in this CLI; use two snapshots",
    )
    parser.add_argument("--output", type=Path, default=Path("validation_reports/schema_evolution.json"))
    parser.add_argument(
        "--migration-output",
        type=Path,
        default=None,
        help="Write migration impact JSON (default: validation_reports/migration_impact_<id>_<ts>.json)",
    )
    args = parser.parse_args()

    root = Path(".").resolve()
    snaps = find_snapshots(args.contract_id, root)
    if args.snapshot_a and args.snapshot_b:
        path_a, path_b = args.snapshot_a.resolve(), args.snapshot_b.resolve()
    elif len(snaps) >= 2:
        path_a, path_b = snaps[-2], snaps[-1]
    else:
        raise SystemExit(
            f"Need at least two snapshots under schema_snapshots/{args.contract_id}/ or pass --snapshot-a/--snapshot-b"
        )

    old_c = load_snapshot_yaml(path_a)
    new_c = load_snapshot_yaml(path_b)
    old_s = old_c.get("schema") or {}
    new_s = new_c.get("schema") or {}

    all_keys = sorted(set(old_s.keys()) | set(new_s.keys()))
    changes: list[dict[str, Any]] = []
    breaking: list[dict] = []
    compatible: list[dict] = []

    for k in all_keys:
        verdict, msg = classify_change(k, old_s.get(k), new_s.get(k))
        changes.append({"field": k, "verdict": verdict, "message": msg})
        if verdict == "BREAKING":
            breaking.append({"field": k, "message": msg})
        else:
            compatible.append({"field": k, "message": msg})

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    mig_path = args.migration_output or Path(f"validation_reports/migration_impact_{args.contract_id}_{ts}.json")

    migration = {
        "contract_id": args.contract_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_old": str(path_a),
        "snapshot_new": str(path_b),
        "compatibility_summary": {
            "breaking_changes": len(breaking),
            "compatible_changes": len(compatible),
            "overall": "BREAKING" if breaking else "COMPATIBLE",
        },
        "changes": changes,
        "breaking_changes": breaking,
        "migration_checklist": [
            "Notify all subscribers in contract_registry/subscriptions.yaml",
            "Update ValidationRunner baselines after intentional migration",
            "Re-run ContractGenerator on representative data",
            "Coordinate deployment order: producer first, consumers second",
        ],
        "rollback_plan": [
            "Revert producer deploy to prior git tag",
            "Restore previous schema snapshot from schema_snapshots/",
            "Re-import prior baselines.json entry for this contract_id",
        ],
        "per_consumer_failure_modes": [
            {
                "subscriber": "week4-brownfield-cartographer",
                "if_field": "extracted_facts.confidence",
                "failure": "Ranking and thresholds interpret confidence as 0-1 probability; 0-100 scale corrupts ordering",
            }
        ],
    }

    report = {
        "contract_id": args.contract_id,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_old": str(path_a),
        "snapshot_new": str(path_b),
        "changes": changes,
        "migration_impact_file": str(mig_path),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    mig_path.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    mig_path.write_text(json.dumps(migration, indent=2), encoding="utf-8")
    print(f"Schema evolution report: {args.output}")
    print(f"Migration impact: {mig_path}")


if __name__ == "__main__":
    main()
