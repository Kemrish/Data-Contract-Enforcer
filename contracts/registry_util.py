"""
Contract registry — load, validate schema, verify subscription coverage for blast-radius sourcing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# Minimum interfaces required for Tier-1 demo (project diagram)
REQUIRED_CONTRACT_COVERAGE = frozenset(
    {
        "week3-document-refinery-extractions",
        "week4-brownfield-cartographer-lineage",
        "week5-event-records",
        "langsmith-trace-runs",
    }
)


def load_subscriptions(path: Path) -> tuple[list[dict], list[str]]:
    """Load subscriptions; returns (rows, load_errors)."""
    errs: list[str] = []
    if not path.exists():
        return [], [f"Registry file not found: {path}"]
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        return [], [f"YAML parse error: {e}"]
    if raw is None:
        return [], ["Registry is empty"]
    subs = raw.get("subscriptions")
    if subs is None:
        return [], ["Missing top-level 'subscriptions' key"]
    if not isinstance(subs, list):
        return [], ["'subscriptions' must be a list"]
    return list(subs), errs


def validate_subscription_row(row: Any, index: int) -> list[str]:
    errs = []
    if not isinstance(row, dict):
        return [f"subscriptions[{index}] must be a mapping"]
    cid = row.get("contract_id")
    sid = row.get("subscriber_id")
    if not cid or not isinstance(cid, str):
        errs.append(f"subscriptions[{index}]: contract_id required (string)")
    if not sid or not isinstance(sid, str):
        errs.append(f"subscriptions[{index}]: subscriber_id required (string)")
    fc = row.get("fields_consumed")
    if fc is not None and not isinstance(fc, list):
        errs.append(f"subscriptions[{index}]: fields_consumed must be a list")
    bf = row.get("breaking_fields")
    if bf is not None:
        if not isinstance(bf, list):
            errs.append(f"subscriptions[{index}]: breaking_fields must be a list")
        else:
            for j, b in enumerate(bf):
                if isinstance(b, dict):
                    if "field" not in b:
                        errs.append(f"subscriptions[{index}].breaking_fields[{j}]: missing 'field'")
                elif not isinstance(b, str):
                    errs.append(f"subscriptions[{index}].breaking_fields[{j}]: must be string or mapping")
    return errs


def validate_registry(path: Path) -> dict[str, Any]:
    """
    Full validation: parse YAML, row shape, coverage of required contract_ids.
    Returns dict with subscriptions, errors, warnings, coverage_ok.
    """
    subs, load_errs = load_subscriptions(path)
    row_errs: list[str] = []
    for i, row in enumerate(subs):
        row_errs.extend(validate_subscription_row(row, i))

    contract_ids = {s.get("contract_id") for s in subs if isinstance(s, dict)}
    missing = sorted(REQUIRED_CONTRACT_COVERAGE - contract_ids)
    warnings: list[str] = []
    if missing:
        warnings.append(
            f"Subscription coverage gap: no entries for contract_id(s): {missing}. "
            "Blast radius queries for those contracts will return empty."
        )

    return {
        "registry_path": str(path),
        "subscriptions": subs,
        "errors": load_errs + row_errs,
        "warnings": warnings,
        "coverage_ok": len(missing) == 0,
        "contract_ids_present": sorted(c for c in contract_ids if c),
    }


def subscribers_for_contract(subs: list[dict], contract_id: str) -> list[dict]:
    return [s for s in subs if isinstance(s, dict) and s.get("contract_id") == contract_id]
