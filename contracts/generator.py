import argparse
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# Narrative / high-cardinality fields: do not lock to enum from small samples (Bitol quality).
NO_ENUM_FIELDS = frozenset(
    {
        "fact_text",
        "fact_source_excerpt",
        "source_path",
        "description",
    }
)

CONTRACT_META = {
    "week3-document-refinery-extractions": {
        "title": "Week 3 Document Refinery — Extraction Records",
        "owner": "week3-document-refinery",
        "description": (
            "One JSON object per line. Each record describes one processed document and "
            "flattened extracted facts used for profiling and validation."
        ),
        "limitations": (
            "confidence on extracted facts must remain a float in [0.0, 1.0] (not 0–100). "
            "SHA-256 source_hash must be 64 hex chars. UUID fields must be RFC-4122 string form."
        ),
    },
    "week5-event-records": {
        "title": "Week 5 Event Sourcing — Domain Events",
        "owner": "week5-event-sourcing",
        "description": (
            "Append-only event log (JSONL). Each line is one domain event with metadata, payload, "
            "and temporal fields for ordering and audit."
        ),
        "limitations": (
            "recorded_at must be >= occurred_at. sequence_number must be monotonic per aggregate_id "
            "in a single ordered stream. event_type must remain PascalCase per registry."
        ),
    },
}


def load_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


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
    """Explode extracted_facts; flatten token_count, payload, metadata for profiling."""
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
                for fk, fv in fact.items():
                    row[f"fact_{fk}"] = fv
            rows.append(row)
    else:
        rows.append(base)
    return rows


def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    rows = []
    for r in records:
        rows.extend(flatten_record(r))
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def infer_type(dtype_str: str) -> str:
    mapping = {
        "float64": "number",
        "int64": "integer",
        "Int64": "integer",
        "bool": "boolean",
        "object": "string",
    }
    return mapping.get(dtype_str, "string")


def profile_column(series: pd.Series, col_name: str) -> dict:
    non_null = series.dropna()
    normalized = non_null.apply(
        lambda v: json.dumps(v, sort_keys=True) if isinstance(v, (list, dict)) else v
    )
    profile = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": int(normalized.nunique()),
        "sample_values": [str(v) for v in normalized.unique()[:5]],
    }
    if pd.api.types.is_numeric_dtype(series):
        profile["stats"] = {
            "min": float(series.min()),
            "max": float(series.max()),
            "mean": float(series.mean()),
            "p25": float(series.quantile(0.25)),
            "p50": float(series.quantile(0.50)),
            "p75": float(series.quantile(0.75)),
            "p95": float(series.quantile(0.95)),
            "p99": float(series.quantile(0.99)),
            "stddev": float(series.std() if not pd.isna(series.std()) else 0.0),
        }
    return profile


def is_full_enum(profile: dict) -> bool:
    return len(profile["sample_values"]) > 0 and len(profile["sample_values"]) == profile["cardinality_estimate"]


def column_to_clause(profile: dict, contract_id: str) -> dict:
    name = profile["name"]
    clause: dict = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0,
        "description": f"Observed field `{name}` from flattened JSONL profile.",
    }

    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "Confidence score for extracted facts. Must remain 0.0–1.0 float. "
            "BREAKING if rescaled to 0–100."
        )

    if name == "source_hash" and clause["type"] == "string":
        clause["pattern"] = "^[a-f0-9]{64}$"
        clause["description"] = "SHA-256 of source content (64 lowercase hex characters)."

    # user_id fields are strings in canonical event metadata, not UUIDs.
    if (name.endswith("_id") and not name.endswith("_user_id")) or name in (
        "event_id",
        "aggregate_id",
    ):
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-f-]{36}$"

    if name.endswith("_at"):
        clause["format"] = "date-time"

    if name == "extraction_model" and clause["type"] == "string":
        clause["pattern"] = "^(claude|gpt)-"
        clause["description"] = "Model identifier. Must match claude-* or gpt-* prefix."

    if name == "fact_fact_id":
        clause["unique"] = True
        clause["description"] = "Stable fact identifier; must be unique across all rows in this snapshot."

    if name == "event_id" and contract_id == "week5-event-records":
        clause["unique"] = True
        clause["description"] = "Primary key for the event stream row (globally unique)."

    if name == "payload_doc_id" and clause["type"] == "string":
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-f-]{36}$"
        clause["description"] = "Document identifier carried in event payload; relates to Week 3 doc_id (see dbt relationships test)."

    use_enum = (
        clause["type"] == "string"
        and profile["cardinality_estimate"] <= 12
        and is_full_enum(profile)
        and name not in NO_ENUM_FIELDS
        and name != "extraction_model"
        and not name.startswith("metadata_")
    )
    if use_enum:
        clause["enum"] = profile["sample_values"]

    if "stats" in profile:
        clause["stats"] = profile["stats"]

    return clause


def schema_annotations(contract_id: str) -> dict:
    """Human-readable annotations for key fields (Bitol-adjacent documentation)."""
    common = {
        "doc_id": {
            "business_meaning": "Primary document key shared with downstream lineage and events.",
        },
        "fact_confidence": {
            "business_meaning": "Normalized model confidence for each extracted fact.",
        },
        "payload_doc_id": {
            "business_meaning": "Join key from event payload to Week 3 extraction records.",
            "foreign_key": {
                "target_contract": "week3-document-refinery-extractions",
                "target_field": "doc_id",
            },
        },
        "recorded_at": {
            "business_meaning": "Time persisted to the event log; must not precede occurred_at.",
        },
    }
    if contract_id == "week3-document-refinery-extractions":
        return {k: v for k, v in common.items() if k in ("doc_id", "fact_confidence")}
    if contract_id == "week5-event-records":
        return {k: v for k, v in common.items() if k in ("payload_doc_id", "recorded_at")}
    return {}


def normalize_output_name(contract_id: str) -> str:
    if contract_id == "week5-event-records":
        return "week5_events"
    if contract_id == "week3-document-refinery-extractions":
        return "week3_extractions"
    m = re.match(r"^(week\d+)-.*-([a-z_]+)$", contract_id)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    return contract_id.replace("-", "_")


def inject_lineage(contract: dict, lineage_path: Path, contract_id: str) -> dict:
    with lineage_path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    snapshot = json.loads(lines[-1]) if lines else {"edges": [], "nodes": []}

    consumers: list[str] = []
    for edge in snapshot.get("edges", []):
        source = str(edge.get("source", "")).lower()
        if contract_id.startswith("week3") and ("week3" in source or "extraction" in source):
            consumers.append(str(edge.get("target", "unknown-target")))
        if contract_id.startswith("week5") and ("week5" in source or "event" in source):
            consumers.append(str(edge.get("target", "unknown-target")))

    unique = sorted(set(consumers))
    downstream = [
        {
            "id": c,
            "fields_consumed": ["doc_id", "extracted_facts"],
            "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
            "description": "Inferred downstream consumer from Week 4 lineage edges.",
        }
        for c in unique
    ]

    if contract_id.startswith("week3") and not downstream:
        downstream = [
            {
                "id": "week4-brownfield-cartographer",
                "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
                "breaking_if_changed": ["extracted_facts.confidence"],
                "description": "Lineage and graph ingestion of extraction outputs.",
            },
            {
                "id": "week5-event-sourcing",
                "fields_consumed": ["doc_id"],
                "breaking_if_changed": ["doc_id"],
                "description": "Events may embed doc_id in payload for correlation.",
            },
        ]

    if contract_id.startswith("week5"):
        downstream = [
            {
                "id": "week8-data-contract-enforcer",
                "fields_consumed": [
                    "event_id",
                    "event_type",
                    "aggregate_id",
                    "sequence_number",
                    "payload",
                    "occurred_at",
                    "recorded_at",
                ],
                "breaking_if_changed": ["event_type", "sequence_number", "recorded_at"],
                "description": "Contract validation and health reporting.",
            }
        ]

    contract["lineage"] = {
        "upstream": [
            {
                "id": "week3-document-refinery",
                "description": "Produces document identifiers referenced in event payloads.",
                "fields_produced": ["doc_id"],
            }
        ]
        if contract_id.startswith("week5")
        else [],
        "downstream": downstream,
    }
    return contract


def build_quality_checks(schema: dict) -> dict:
    checks = []
    for col, clause in schema.items():
        if clause.get("required"):
            checks.append(f"missing_count({col}) = 0")
        if "enum" in clause:
            vals = ",".join(str(v) for v in clause["enum"])
            checks.append(f"accepted_values({col}) in [{vals}]")
        if "minimum" in clause:
            checks.append(f"min({col}) >= {clause['minimum']}")
        if "maximum" in clause:
            checks.append(f"max({col}) <= {clause['maximum']}")
        if clause.get("unique"):
            checks.append(f"duplicate_count({col}) = 0")
    if not checks:
        checks.append("row_count >= 1")
    return {"type": "SodaChecks", "specification": {"checks for dataset": checks}}


def make_dbt_schema(contract: dict, model_name: str, contract_id: str) -> dict:
    """dbt schema.yml: native tests only (not_null, unique, accepted_values, relationships)."""
    columns = []
    for name, clause in contract["schema"].items():
        tests: list = []
        if clause.get("required"):
            tests.append("not_null")
        if clause.get("unique"):
            tests.append("unique")
        if "enum" in clause:
            tests.append({"accepted_values": {"values": clause["enum"]}})
        if name == "payload_doc_id":
            tests.append(
                {
                    "relationships": {
                        "to": "ref('week3_extractions')",
                        "field": "doc_id",
                    }
                }
            )

        col = {"name": name, "description": clause.get("description", ""), "tests": tests}
        columns.append(col)

    meta = {
        "contract_id": contract_id,
        "bitol_contract_file": f"{model_name}.yaml",
        "notes": (
            "Range checks (e.g. fact_confidence 0..1) and temporal rules are enforced by "
            "ValidationRunner and SodaChecks in the Bitol YAML; add dbt_expectations or SQL tests if needed."
        ),
    }
    return {"version": 2, "models": [{"name": model_name, "meta": meta, "columns": columns}]}


def build_contract(args: argparse.Namespace, profiles: dict) -> dict:
    meta = CONTRACT_META.get(
        args.contract_id,
        {
            "title": args.contract_id,
            "owner": "platform",
            "description": "Auto-generated data contract.",
            "limitations": "Review profiling enums before production; widen or replace with patterns.",
        },
    )
    schema = {name: column_to_clause(profile, args.contract_id) for name, profile in profiles.items()}
    annotations = schema_annotations(args.contract_id)

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": args.contract_id,
        "info": {
            "title": meta["title"],
            "version": "1.0.0",
            "owner": meta["owner"],
            "description": meta["description"],
            "contact": {
                "name": "Data platform owner",
                "email": "data-contracts@example.com",
            },
        },
        "terms": {
            "usage": "Internal inter-system data contract. Not for external publication.",
            "limitations": meta["limitations"],
        },
        "servers": {
            "local": {
                "type": "local",
                "path": str(args.source).replace("\\", "/"),
                "format": "jsonl",
            }
        },
        "schema": schema,
        "schema_annotations": annotations,
        "quality": build_quality_checks(schema),
    }
    return contract


def save_snapshot(output_path: Path, contract_id: str) -> None:
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"
    shutil.copy(output_path, snapshot_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Bitol-compatible data contracts from JSONL.")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--lineage", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)

    records = load_jsonl(args.source)
    df = flatten_for_profile(records)
    if df.empty:
        raise ValueError(f"No rows found in source: {args.source}")

    profiles = {col: profile_column(df[col], col) for col in df.columns}
    contract = build_contract(args, profiles)
    contract = inject_lineage(contract, args.lineage, args.contract_id)

    out_stem = normalize_output_name(args.contract_id)
    yaml_path = args.output / f"{out_stem}.yaml"
    with yaml_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(contract, f, sort_keys=False, default_flow_style=False, allow_unicode=True)

    dbt_schema = make_dbt_schema(contract, out_stem, args.contract_id)
    dbt_path = args.output / f"{out_stem}_dbt.yml"
    with dbt_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(dbt_schema, f, sort_keys=False, default_flow_style=False, allow_unicode=True)

    save_snapshot(yaml_path, args.contract_id)
    print(f"Contract written: {yaml_path}")
    print(f"dbt schema written: {dbt_path}")
    print(f"Schema fields: {len(contract['schema'])}")


if __name__ == "__main__":
    main()
