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

import registry_util


def load_snapshot_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def detect_critical_narrow_type_change(field: str, old: dict, new: dict) -> tuple[str, str] | None:
    """
    Escalate probable probability-scale / narrow-type regressions to CRITICAL (rubric).
    e.g. float 0.0–1.0 confidence → integer 0–100 or widened numeric range.
    """
    ot, nt = old.get("type"), new.get("type")
    omax, omin = _num(old.get("maximum")), _num(old.get("minimum"))
    nmax, nmin = _num(new.get("maximum")), _num(new.get("minimum"))
    confish = "confidence" in field.lower() or field.endswith("_confidence")

    # Float/range [0,1] → integer (classic 0–100 integer bug)
    if ot == "number" and nt == "integer":
        if omax is not None and omax <= 1.0 and omin is not None and omin >= 0.0:
            if nmax is None or nmax > 1.0:
                return (
                    "probability_float_to_integer_scale",
                    f"CRITICAL: {field} moved from bounded probability float (~0.0-1.0) to integer type - "
                    "likely 0-100 centiscore encoding; downstream thresholds and rankings break.",
                )
        if confish:
            return (
                "confidence_type_integer",
                f"CRITICAL: {field} changed from number to integer while consumers expect fractional confidence.",
            )

    # Still number but range widened from probability band to centiscore band
    if ot == "number" and nt == "number":
        if omax is not None and omax <= 1.0 and nmax is not None and nmax >= 10.0:
            return (
                "probability_range_to_centiscore",
                f"CRITICAL: {field} maximum widened from probability-scale (<=1.0) to {nmax} - "
                "consistent with 0-100 rescaling; treat as BREAKING for all probability semantics.",
            )
        if confish and omax is not None and omax <= 1.0 and nmax is not None and nmax > 1.0:
            return (
                "confidence_maximum_escalation",
                f"CRITICAL: {field} max increased from {omax} to {nmax} on a confidence-like field.",
            )

    # Type to number with huge max where old was tight [0,1]
    if ot == "number" and nt == "number":
        if omin == 0.0 and omax == 1.0 and nmin == 0.0 and nmax is not None and nmax >= 50.0:
            return (
                "unit_scale_change_0_1_to_0_100",
                f"CRITICAL: {field} appears to shift from unit interval [0,1] to [0,{int(nmax)}]-style scale.",
            )

    return None


def classify_change(field: str, old: dict | None, new: dict | None) -> dict[str, Any]:
    """
    Returns verdict (BREAKING|COMPATIBLE), message, severity (CRITICAL|HIGH|LOW), taxonomy_class.
    """
    base = {
        "field": field,
        "verdict": "COMPATIBLE",
        "message": "",
        "severity": "LOW",
        "taxonomy_class": "none",
    }
    if old is None and new is not None:
        req = new.get("required", False)
        if req:
            return {
                **base,
                "verdict": "BREAKING",
                "message": f"New required field {field}",
                "severity": "HIGH",
                "taxonomy_class": "required_field_added",
            }
        return {
            **base,
            "verdict": "COMPATIBLE",
            "message": f"New optional field {field}",
            "taxonomy_class": "optional_field_added",
        }
    if old is not None and new is None:
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Field removed: {field}",
            "severity": "HIGH",
            "taxonomy_class": "field_removed",
        }
    if old is None or new is None:
        return {**base, "message": "No change"}
    crit = detect_critical_narrow_type_change(field, old, new)
    if crit:
        tax, msg = crit
        return {
            **base,
            "verdict": "BREAKING",
            "message": msg,
            "severity": "CRITICAL",
            "taxonomy_class": tax,
        }
    if old.get("type") != new.get("type"):
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Type changed {old.get('type')} -> {new.get('type')} for {field}",
            "severity": "HIGH",
            "taxonomy_class": "type_change",
        }
    omax = old.get("maximum")
    nmax = new.get("maximum")
    omin = old.get("minimum")
    nmin = new.get("minimum")
    if omax != nmax or omin != nmin:
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Range changed for {field}: min {omin}->{nmin}, max {omax}->{nmax}",
            "severity": "HIGH",
            "taxonomy_class": "range_change",
        }
    old_enum = set(old.get("enum") or [])
    new_enum = set(new.get("enum") or [])
    if old_enum - new_enum:
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Enum values removed from {field}: {old_enum - new_enum}",
            "severity": "HIGH",
            "taxonomy_class": "enum_narrowing",
        }
    if new_enum - old_enum:
        return {
            **base,
            "verdict": "COMPATIBLE",
            "message": f"Enum values added to {field}: {new_enum - old_enum}",
            "taxonomy_class": "enum_extension",
        }
    opat = old.get("pattern")
    npat = new.get("pattern")
    if opat != npat and (opat or npat):
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Regex/pattern changed for {field}",
            "severity": "HIGH",
            "taxonomy_class": "pattern_change",
        }
    ofmt = old.get("format")
    nfmt = new.get("format")
    if ofmt != nfmt:
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Format changed {ofmt} -> {nfmt} for {field}",
            "severity": "HIGH",
            "taxonomy_class": "format_change",
        }
    if old.get("unique") and not new.get("unique"):
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Unique constraint removed from {field}",
            "severity": "HIGH",
            "taxonomy_class": "unique_removed",
        }
    if not old.get("required") and new.get("required"):
        return {
            **base,
            "verdict": "BREAKING",
            "message": f"Field {field} became required",
            "severity": "HIGH",
            "taxonomy_class": "optional_to_required",
        }
    if old.get("required") and not new.get("required"):
        return {
            **base,
            "verdict": "COMPATIBLE",
            "message": f"Field {field} became optional",
            "taxonomy_class": "required_to_optional",
        }
    return {
        **base,
        "verdict": "COMPATIBLE",
        "message": f"No material structural change to {field}",
        "taxonomy_class": "unchanged",
    }


def blast_radius_from_registry(contract_id: str, registry_path: Path) -> dict[str, Any]:
    subs, errs = registry_util.load_subscriptions(registry_path)
    if errs:
        return {
            "subscriber_count": 0,
            "source": str(registry_path),
            "load_errors": errs,
            "subscribers": [],
        }
    rows = registry_util.subscribers_for_contract(subs, contract_id)
    out = []
    for s in rows:
        bf = s.get("breaking_fields") or []
        out.append(
            {
                "subscriber_id": s.get("subscriber_id"),
                "subscriber_team": s.get("subscriber_team"),
                "fields_consumed": list(s.get("fields_consumed") or []),
                "breaking_fields_count": len(bf) if isinstance(bf, list) else 0,
                "validation_mode": s.get("validation_mode"),
            }
        )
    return {
        "subscriber_count": len(out),
        "source": str(registry_path),
        "subscribers": out,
    }


def per_consumer_failure_modes_from_registry(contract_id: str, registry_path: Path) -> list[dict[str, Any]]:
    subs, _ = registry_util.load_subscriptions(registry_path)
    modes: list[dict[str, Any]] = []
    for s in registry_util.subscribers_for_contract(subs, contract_id):
        sid = s.get("subscriber_id", "unknown")
        for bf in s.get("breaking_fields") or []:
            if isinstance(bf, dict):
                fld = bf.get("field", "")
                reason = bf.get("reason", "")
            else:
                fld, reason = str(bf), ""
            modes.append(
                {
                    "subscriber_id": sid,
                    "if_field": fld,
                    "failure": reason or f"Schema or semantic drift on {fld} breaks assumptions for {sid}",
                    "source": "contract_registry",
                }
            )
        if not s.get("breaking_fields"):
            fc = s.get("fields_consumed") or []
            modes.append(
                {
                    "subscriber_id": sid,
                    "if_field": ", ".join(fc) if fc else "(contract-wide)",
                    "failure": f"Any BREAKING change to consumed fields affects {sid} ingestion or validation.",
                    "source": "contract_registry",
                }
            )
    return modes


def lineage_extra_downstream_count(contract_id: str, lineage_path: Path | None) -> dict[str, Any]:
    if not lineage_path or not lineage_path.exists():
        return {"lineage_edges_count": None, "lineage_file": None, "note": "No lineage file provided"}
    try:
        lines = [ln.strip() for ln in lineage_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        if not lines:
            return {"lineage_edges_count": 0, "lineage_file": str(lineage_path)}
        snap = json.loads(lines[-1])
        edges = snap.get("edges") or []
        return {
            "lineage_edges_count": len(edges),
            "lineage_nodes_count": len(snap.get("nodes") or []),
            "lineage_file": str(lineage_path),
        }
    except (json.JSONDecodeError, OSError):
        return {"lineage_edges_count": None, "lineage_file": str(lineage_path), "note": "Could not parse lineage JSONL"}


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
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("contract_registry/subscriptions.yaml"),
        help="Registry for blast-radius and per-consumer failure modes.",
    )
    parser.add_argument(
        "--lineage",
        type=Path,
        default=Path("outputs/week4/lineage_snapshots.jsonl"),
        help="Optional lineage JSONL (last line = snapshot) for graph size metrics.",
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
    critical: list[dict] = []

    reg_path = args.registry.resolve()
    lin_path = args.lineage.resolve() if args.lineage.exists() else None

    for k in all_keys:
        cd = classify_change(k, old_s.get(k), new_s.get(k))
        entry = {
            "field": k,
            "verdict": cd["verdict"],
            "message": cd["message"],
            "severity": cd["severity"],
            "taxonomy_class": cd["taxonomy_class"],
        }
        changes.append(entry)
        if cd["verdict"] == "BREAKING":
            breaking.append({"field": k, "message": cd["message"], "severity": cd["severity"], "taxonomy_class": cd["taxonomy_class"]})
        else:
            compatible.append({"field": k, "message": cd["message"], "taxonomy_class": cd["taxonomy_class"]})
        if cd["severity"] == "CRITICAL":
            critical.append(entry)

    br = blast_radius_from_registry(args.contract_id, reg_path)
    lin_m = lineage_extra_downstream_count(args.contract_id, lin_path)
    per_consumer = per_consumer_failure_modes_from_registry(args.contract_id, reg_path)

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
            "critical_changes": len(critical),
            "overall": "BREAKING" if breaking else "COMPATIBLE",
        },
        "critical_schema_changes": critical,
        "changes": changes,
        "breaking_changes": breaking,
        "blast_radius": {
            **br,
            **lin_m,
            "affected_subscriber_count": br.get("subscriber_count", 0),
            "metric_notes": "affected_subscriber_count from registry; lineage_* from week4 snapshot when present.",
        },
        "migration_checklist": [
            f"Notify {br.get('subscriber_count', 0)} registered subscriber(s) in {reg_path.as_posix()}",
            "Update ValidationRunner baselines after intentional migration",
            "Re-run ContractGenerator on representative JSONL for this contract_id",
            "Coordinate deployment order: producer first, consumers second",
        ],
        "rollback_plan": [
            "Revert producer deploy to prior git tag",
            "Restore previous schema snapshot from schema_snapshots/",
            "Re-import prior baselines.json entry for this contract_id",
        ],
        "per_consumer_failure_modes": per_consumer,
    }

    report = {
        "contract_id": args.contract_id,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_old": str(path_a),
        "snapshot_new": str(path_b),
        "changes": changes,
        "critical_schema_changes": critical,
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
