import argparse
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

import registry_util

# Narrative / high-cardinality fields: do not lock to enum from small samples (Bitol quality).
NO_ENUM_FIELDS = frozenset(
    {
        "fact_text",
        "fact_source_excerpt",
        "source_path",
        "description",
        "governance_tags_json",
        "inputs",
        "outputs",
        "name",
        "error",
        "code_ref_symbol",
        "node_label",
    }
)

OUTPUT_STEM = {
    "week3-document-refinery-extractions": "week3_extractions",
    "week5-event-records": "week5_events",
    "week1-intent-code-correlator-intents": "week1_intent_records",
    "week4-brownfield-cartographer-lineage": "week4_lineage",
    "langsmith-trace-runs": "langsmith_traces",
}

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
    "week1-intent-code-correlator-intents": {
        "title": "Week 1 Intent–Code Correlator — Intent Records",
        "owner": "week1-intent-correlator",
        "description": (
            "JSONL of intent statements with linked code references used by the Digital Courtroom "
            "and downstream governance."
        ),
        "limitations": (
            "code_refs.confidence is 0.0–1.0. created_at is ISO-8601 Z. code_refs must be non-empty."
        ),
    },
    "week4-brownfield-cartographer-lineage": {
        "title": "Week 4 Brownfield Cartographer — Lineage Snapshots",
        "owner": "week4-brownfield-cartographer",
        "description": (
            "Graph snapshots (nodes/edges) used by Week 7 attribution and blast-radius enrichment."
        ),
        "limitations": (
            "git_commit is 40 hex chars. edge endpoints must exist in nodes[]. node_id uses type::path form."
        ),
    },
    "langsmith-trace-runs": {
        "title": "LangSmith / LLM Trace Export — Run Records",
        "owner": "platform-observability",
        "description": (
            "Exported trace rows (JSONL) for AI contract checks: token math, run_type, timings, cost."
        ),
        "limitations": (
            "total_tokens must equal prompt_tokens + completion_tokens. run_type is a closed enum."
        ),
    },
}


def infer_contract_id(source: Path) -> str:
    s = source.as_posix().lower()
    name = source.name.lower()
    if "week3" in s and "extraction" in name:
        return "week3-document-refinery-extractions"
    if "week5" in s and "event" in name:
        return "week5-event-records"
    if "week1" in s and "intent" in name:
        return "week1-intent-code-correlator-intents"
    if "week4" in s and "lineage" in name:
        return "week4-brownfield-cartographer-lineage"
    if name == "runs.jsonl" and ("trace" in s or "traces" in s):
        return "langsmith-trace-runs"
    raise ValueError(
        f"Cannot infer --contract-id from {source}. Pass --contract-id explicitly."
    )


def load_jsonl(path: Path) -> list[dict]:
    """Parse JSONL; skip bad lines with stderr warning; fail if no valid rows."""
    records: list[dict] = []
    bad_lines: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                bad_lines.append(f"line {lineno}: {e}")
    if bad_lines:
        print(
            f"Warning: skipped {len(bad_lines)} invalid JSONL line(s) in {path}: "
            f"{bad_lines[0]}{' ...' if len(bad_lines) > 1 else ''}"
        )
    if not records:
        raise ValueError(f"No valid JSON rows in {path}" + (f"; errors: {bad_lines[:5]}" if bad_lines else ""))
    return records


def flatten_week1_record(record: dict) -> list[dict]:
    base = {
        "intent_id": record.get("intent_id"),
        "description": record.get("description"),
        "created_at": record.get("created_at"),
        "governance_tags_json": json.dumps(record.get("governance_tags") or [], sort_keys=True),
    }
    rows: list[dict] = []
    for ref in record.get("code_refs") or []:
        row = dict(base)
        if isinstance(ref, dict):
            row["code_ref_file"] = ref.get("file")
            row["code_ref_line_start"] = ref.get("line_start")
            row["code_ref_line_end"] = ref.get("line_end")
            row["code_ref_symbol"] = ref.get("symbol")
            row["code_ref_confidence"] = ref.get("confidence")
        rows.append(row)
    if not rows:
        rows.append(dict(base))
    return rows


def flatten_lineage_snapshot_record(snap: dict) -> list[dict]:
    rows: list[dict] = []
    sid = snap.get("snapshot_id")
    gc = snap.get("git_commit")
    cap = snap.get("captured_at")
    root = snap.get("codebase_root")
    for n in snap.get("nodes") or []:
        md = n.get("metadata") or {}
        if not isinstance(n, dict):
            continue
        rows.append(
            {
                "snapshot_id": sid,
                "git_commit": gc,
                "captured_at": cap,
                "codebase_root": root,
                "node_id": n.get("node_id"),
                "node_type": n.get("type"),
                "node_label": n.get("label"),
                "metadata_path": md.get("path"),
                "metadata_language": md.get("language"),
                "edge_count_estimate": len(snap.get("edges") or []),
                "node_count_estimate": len(snap.get("nodes") or []),
            }
        )
    if not rows:
        rows.append(
            {
                "snapshot_id": sid,
                "git_commit": gc,
                "captured_at": cap,
                "codebase_root": root,
                "node_id": None,
                "node_type": None,
                "node_label": None,
                "metadata_path": None,
                "metadata_language": None,
                "edge_count_estimate": len(snap.get("edges") or []),
                "node_count_estimate": 0,
            }
        )
    return rows


def flatten_trace_record(record: dict) -> dict:
    row: dict = {}
    for k, v in record.items():
        if k in ("inputs", "outputs", "tags"):
            row[k] = (
                json.dumps(v, sort_keys=True, default=str)
                if v is not None and not isinstance(v, str)
                else v
            )
        elif isinstance(v, (list, dict)):
            row[k] = json.dumps(v, sort_keys=True, default=str)
        else:
            row[k] = v
    return row


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


def records_to_dataframe(records: list[dict], contract_id: str) -> pd.DataFrame:
    if contract_id == "week1-intent-code-correlator-intents":
        rows = []
        for r in records:
            rows.extend(flatten_week1_record(r))
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    if contract_id == "week4-brownfield-cartographer-lineage":
        rows = []
        for snap in records:
            rows.extend(flatten_lineage_snapshot_record(snap))
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    if contract_id == "langsmith-trace-runs":
        return pd.DataFrame([flatten_trace_record(r) for r in records])
    rows = []
    for r in records:
        rows.extend(flatten_record(r))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


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
    else:
        # Dominant character-class hint for strings (rubric: structural profiling)
        sstr = non_null.astype(str).head(500)
        if len(sstr) > 0:
            alnum = sum(1 for v in sstr if v and (v[0].isalnum()))
            url = sum(1 for v in sstr if v.startswith(("http://", "https://", "/")))
            uuidish = sum(1 for v in sstr if len(v) == 36 and v.count("-") == 4)
            n = len(sstr)
            profile["string_shape"] = {
                "alnum_prefix_ratio": round(alnum / n, 3),
                "looks_like_url_or_path_ratio": round(url / n, 3),
                "looks_like_uuid_ratio": round(uuidish / n, 3),
                "dominant_pattern": (
                    "uuid_like"
                    if uuidish > n * 0.5
                    else "url_or_path"
                    if url > n * 0.3
                    else "alphanumeric_text"
                    if alnum > n * 0.6
                    else "mixed"
                ),
            }
    return profile


def apply_ydata_profiling(df: pd.DataFrame, profiles: dict[str, dict], enabled: bool) -> dict[str, dict]:
    if not enabled or df.empty or len(df.columns) == 0:
        return profiles
    try:
        from ydata_profiling import ProfileReport
    except ImportError:
        return profiles
    try:
        kwargs = dict(
            minimal=True,
            progress_bar=False,
            explorative=False,
            title="ContractGenerator",
        )
        try:
            report = ProfileReport(df, **kwargs, pool_size=1)
        except TypeError:
            report = ProfileReport(df, **kwargs)
        variables = report.description_set.get("variables") or {}
    except Exception:
        return profiles
    for col, prof in profiles.items():
        vd = variables.get(col)
        if not isinstance(vd, dict):
            continue
        extra: dict = {}
        for k in ("skewness", "kurtosis", "variance", "iqr"):
            val = vd.get(k)
            if isinstance(val, (int, float)) and not (isinstance(val, float) and pd.isna(val)):
                extra[k] = float(val) if isinstance(val, float) else int(val)
        nm = vd.get("n_missing")
        if nm is not None and not pd.isna(nm):
            try:
                extra["n_missing"] = int(nm)
            except (TypeError, ValueError):
                pass
        p_miss = vd.get("p_missing")
        if isinstance(p_miss, (int, float)) and not pd.isna(p_miss):
            extra["p_missing"] = float(p_miss)
        if extra:
            prof["ydata_profiling"] = extra
    return profiles


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

    if name == "git_commit" and clause["type"] == "string":
        clause["pattern"] = "^[a-f0-9]{40}$"
        clause["description"] = "40-character Git commit SHA (hex)."

    if name == "node_id" and clause["type"] == "string":
        clause["description"] = "Stable node identifier (type::path or type::label)."

    if (name.endswith("_id") and not name.endswith("_user_id")) or name in (
        "event_id",
        "aggregate_id",
        "intent_id",
        "snapshot_id",
        "session_id",
    ):
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-f-]{36}$"

    if name == "id" and contract_id == "langsmith-trace-runs" and clause["type"] == "string":
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-f-]{36}$"

    if name.endswith("_at") or name in ("start_time", "end_time"):
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

    if name in ("code_ref_line_start", "code_ref_line_end") and clause["type"] in ("integer", "number"):
        clause["type"] = "integer"
        clause["description"] = "1-indexed line span in source file."

    use_enum = (
        clause["type"] == "string"
        and profile["cardinality_estimate"] <= 12
        and is_full_enum(profile)
        and name not in NO_ENUM_FIELDS
        and name != "extraction_model"
        and not name.startswith("metadata_")
        and name not in ("node_type", "run_type")
    )
    if use_enum:
        clause["enum"] = profile["sample_values"]

    if name == "node_type" and clause["type"] == "string" and profile["cardinality_estimate"] <= 20:
        if is_full_enum(profile):
            clause["enum"] = profile["sample_values"]

    if name == "run_type" and clause["type"] == "string":
        clause["enum"] = ["llm", "chain", "tool", "retriever", "embedding"]
        clause["description"] = "LangSmith run classification (closed set)."

    if "stats" in profile:
        clause["stats"] = profile["stats"]

    if profile.get("ydata_profiling"):
        clause["ydata_profiling"] = profile["ydata_profiling"]

    if profile.get("string_shape"):
        clause["string_shape"] = profile["string_shape"]

    return clause


def schema_annotations(contract_id: str) -> dict:
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
        "intent_id": {"business_meaning": "Stable intent record key."},
        "code_ref_file": {"business_meaning": "Repository-relative path for Digital Courtroom target_ref."},
        "node_id": {"business_meaning": "Lineage node identity for graph traversal."},
        "total_tokens": {"business_meaning": "Must equal prompt + completion tokens."},
    }
    if contract_id == "week3-document-refinery-extractions":
        return {k: v for k, v in common.items() if k in ("doc_id", "fact_confidence")}
    if contract_id == "week5-event-records":
        return {k: v for k, v in common.items() if k in ("payload_doc_id", "recorded_at")}
    if contract_id == "week1-intent-code-correlator-intents":
        return {k: v for k, v in common.items() if k in ("intent_id", "code_ref_file")}
    if contract_id == "week4-brownfield-cartographer-lineage":
        return {k: v for k, v in common.items() if k == "node_id"}
    if contract_id == "langsmith-trace-runs":
        return {k: v for k, v in common.items() if k == "total_tokens"}
    return {}


def normalize_output_name(contract_id: str) -> str:
    return OUTPUT_STEM.get(contract_id, contract_id.replace("-", "_"))


def load_registry(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return list(data.get("subscriptions") or [])


def _breaking_field_names(breaking_fields: list) -> list[str]:
    out = []
    for b in breaking_fields or []:
        if isinstance(b, dict) and "field" in b:
            out.append(str(b["field"]))
        elif isinstance(b, str):
            out.append(b)
    return out


def registry_downstream_entries(contract_id: str, subscriptions: list[dict]) -> list[dict]:
    entries = []
    for sub in subscriptions:
        if sub.get("contract_id") != contract_id:
            continue
        bf = sub.get("breaking_fields") or []
        reasons = {}
        for b in bf:
            if isinstance(b, dict) and "field" in b and "reason" in b:
                reasons[str(b["field"])] = b["reason"]
        fields_breaking = _breaking_field_names(bf)
        entries.append(
            {
                "id": sub.get("subscriber_id", "unknown-subscriber"),
                "subscriber_team": sub.get("subscriber_team"),
                "fields_consumed": list(sub.get("fields_consumed") or []),
                "breaking_if_changed": fields_breaking,
                "breaking_field_reasons": reasons,
                "validation_mode": sub.get("validation_mode"),
                "contact": sub.get("contact"),
                "description": "Subscriber from contract_registry/subscriptions.yaml",
                "source": "contract_registry",
            }
        )
    return entries


def line_from_contract_field(contract_field: str) -> str | None:
    """Map flattened profiling column to logical contract line for registry matching."""
    if contract_field == "fact_confidence":
        return "extracted_facts.confidence"
    if contract_field == "doc_id":
        return "doc_id"
    if contract_field.startswith("fact_"):
        return "extracted_facts." + contract_field[5:]
    return contract_field


def annotate_schema_downstream(schema: dict, downstream: list[dict]) -> None:
    logical_to_col: dict[str, list[str]] = {}
    for col in schema:
        line = line_from_contract_field(col) or col
        logical_to_col.setdefault(line, []).append(col)
        logical_to_col.setdefault(col, []).append(col)

    for col in schema:
        consumers = []
        line = line_from_contract_field(col) or col
        for d in downstream:
            fc = d.get("fields_consumed") or []
            bf = d.get("breaking_if_changed") or []
            hit = False
            for f in fc:
                if f == col or f == line or (isinstance(f, str) and col.startswith(f.replace(".", "_"))):
                    hit = True
                    break
            for f in bf:
                if f == col or f == line:
                    hit = True
                    break
            if hit:
                consumers.append(
                    {
                        "subscriber_id": d.get("id"),
                        "fields_consumed": fc,
                        "breaking_if_changed": bf,
                        "source": d.get("source", "lineage"),
                    }
                )
        if consumers:
            ann = schema[col].setdefault("downstream_consumers", [])
            ann.extend(consumers)


def graph_downstream_from_snapshot(snapshot: dict, contract_id: str) -> list[dict]:
    consumers: list[str] = []
    for edge in snapshot.get("edges", []):
        source = str(edge.get("source", "")).lower()
        if contract_id.startswith("week3") and ("week3" in source or "extraction" in source):
            consumers.append(str(edge.get("target", "unknown-target")))
        if contract_id.startswith("week5") and ("week5" in source or "event" in source):
            consumers.append(str(edge.get("target", "unknown-target")))
        if contract_id.startswith("week1") and ("week1" in source or "intent" in source):
            consumers.append(str(edge.get("target", "unknown-target")))

    unique = sorted(set(consumers))
    return [
        {
            "id": c,
            "fields_consumed": ["doc_id", "extracted_facts"],
            "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
            "description": "Inferred downstream consumer from Week 4 lineage edges.",
            "source": "lineage_graph",
        }
        for c in unique
    ]


def default_downstream(contract_id: str) -> list[dict]:
    if contract_id.startswith("week3"):
        return [
            {
                "id": "week4-brownfield-cartographer",
                "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
                "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                "description": "Lineage and graph ingestion of extraction outputs.",
                "source": "default",
            },
            {
                "id": "week5-event-sourcing",
                "fields_consumed": ["doc_id"],
                "breaking_if_changed": ["doc_id"],
                "description": "Events may embed doc_id in payload for correlation.",
                "source": "default",
            },
        ]
    if contract_id.startswith("week5"):
        return [
            {
                "id": "week7-validation-runner",
                "fields_consumed": [
                    "event_id",
                    "event_type",
                    "aggregate_id",
                    "sequence_number",
                    "payload",
                    "occurred_at",
                    "recorded_at",
                ],
                "breaking_if_changed": ["event_type", "sequence_number", "recorded_at", "payload"],
                "description": "Contract validation on event payloads.",
                "source": "default",
            }
        ]
    if contract_id.startswith("week1"):
        return [
            {
                "id": "week2-digital-courtroom",
                "fields_consumed": ["intent_id", "code_refs", "description"],
                "breaking_if_changed": ["code_refs.file", "intent_id"],
                "description": "Verdict target_ref links to intent code_refs.",
                "source": "default",
            }
        ]
    if contract_id.startswith("week4"):
        return [
            {
                "id": "week7-violation-attributor",
                "fields_consumed": ["nodes", "edges", "git_commit"],
                "breaking_if_changed": ["edges.source", "edges.target", "git_commit"],
                "description": "Blame chain and blast-radius enrichment.",
                "source": "default",
            }
        ]
    if contract_id.startswith("langsmith"):
        return [
            {
                "id": "week7-ai-contract-extensions",
                "fields_consumed": ["run_type", "total_tokens", "total_cost", "start_time", "end_time"],
                "breaking_if_changed": ["total_tokens", "run_type"],
                "description": "AI-specific contract checks on traces.",
                "source": "default",
            }
        ]
    return []


def merge_downstream(
    graph_entries: list[dict],
    registry_entries: list[dict],
) -> list[dict]:
    by_id: dict[str, dict] = {}
    for d in graph_entries + registry_entries:
        sid = d.get("id")
        if not sid:
            continue
        if sid not in by_id:
            by_id[sid] = dict(d)
        else:
            cur = by_id[sid]
            cur["source"] = ",".join(
                sorted(set((cur.get("source") or "").split(",") + [(d.get("source") or "")]) - {""})
            )
            cur_fc = set(cur.get("fields_consumed") or []) | set(d.get("fields_consumed") or [])
            cur_bf = set(cur.get("breaking_if_changed") or []) | set(d.get("breaking_if_changed") or [])
            cur["fields_consumed"] = sorted(cur_fc)
            cur["breaking_if_changed"] = sorted(cur_bf)
            r1 = cur.get("breaking_field_reasons") or {}
            r2 = d.get("breaking_field_reasons") or {}
            cur["breaking_field_reasons"] = {**r1, **r2}
    return list(by_id.values())


def inject_lineage(
    contract: dict,
    lineage_path: Path | None,
    contract_id: str,
    registry_path: Path,
) -> dict:
    snapshot: dict = {"edges": [], "nodes": []}
    if lineage_path is not None and lineage_path.exists():
        with lineage_path.open("r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        if lines:
            snapshot = json.loads(lines[-1])

    graph_d = graph_downstream_from_snapshot(snapshot, contract_id)
    if contract_id.startswith("week3") and not graph_d:
        graph_d = default_downstream(contract_id)
    elif contract_id.startswith("week5") and not graph_d:
        graph_d = default_downstream(contract_id)
    elif contract_id.startswith("week1") and not graph_d:
        graph_d = default_downstream(contract_id)
    elif contract_id.startswith("week4") and not graph_d:
        graph_d = default_downstream(contract_id)
    elif contract_id.startswith("langsmith") and not graph_d:
        graph_d = default_downstream(contract_id)

    reg = registry_downstream_entries(contract_id, load_registry(registry_path))
    downstream = merge_downstream(graph_d, reg)
    if not downstream:
        downstream = default_downstream(contract_id)

    upstream: list[dict] = []
    if contract_id.startswith("week5"):
        upstream = [
            {
                "id": "week3-document-refinery",
                "description": "Produces document identifiers referenced in event payloads.",
                "fields_produced": ["doc_id"],
            }
        ]

    contract["lineage"] = {"upstream": upstream, "downstream": downstream}
    annotate_schema_downstream(contract["schema"], downstream)
    return contract


def build_quality_checks(schema: dict, profiling_alerts: list[dict]) -> dict:
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
    for a in profiling_alerts:
        if a.get("soda_hint"):
            checks.append(str(a["soda_hint"]))
    if not checks:
        checks.append("row_count >= 1")
    spec_key = "checks for dataset"
    return {"type": "SodaChecks", "specification": {spec_key: checks}}


def compute_profiling_alerts(schema: dict) -> list[dict]:
    alerts: list[dict] = []
    for name, clause in schema.items():
        if "confidence" not in name.lower():
            continue
        st = clause.get("stats") or {}
        mean = st.get("mean")
        if mean is None:
            continue
        if mean > 0.99:
            alerts.append(
                {
                    "field": name,
                    "severity": "WARNING",
                    "code": "CONFIDENCE_MEAN_NEAR_CLAMP",
                    "message": (
                        f"Mean confidence {mean:.4f} is > 0.99 — distribution may be clamped or "
                        "artifact of small sample; validate producer is not saturating scores."
                    ),
                    "soda_hint": f"# alert: mean({name}) suspiciously high (review distribution)",
                }
            )
        if mean < 0.01:
            alerts.append(
                {
                    "field": name,
                    "severity": "WARNING",
                    "code": "CONFIDENCE_MEAN_NEAR_ZERO",
                    "message": (
                        f"Mean confidence {mean:.4f} is < 0.01 — possible pipeline regression or "
                        "mis-scaled output."
                    ),
                    "soda_hint": f"# alert: mean({name}) suspiciously low",
                }
            )
    return alerts


def make_dbt_schema(contract: dict, model_name: str, contract_id: str) -> dict:
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


def is_ambiguous_clause(clause: dict) -> bool:
    d = clause.get("description") or ""
    return "Observed field `" in d and "from flattened JSONL profile." in d


def llm_annotate_columns(
    contract_id: str,
    schema: dict[str, dict],
    profiles: dict[str, dict],
    column_names: list[str],
    llm_model: str,
) -> dict[str, dict]:
    try:
        import anthropic
    except ImportError:
        return {}

    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return {}

    client = anthropic.Anthropic(api_key=key)
    out: dict[str, dict] = {}
    adjacent_preview = ", ".join(sorted(schema.keys())[:25])

    for col in column_names[:8]:
        clause = schema.get(col) or {}
        prof = profiles.get(col) or {}
        if not is_ambiguous_clause(clause):
            continue
        samples = prof.get("sample_values") or []
        user_msg = (
            f"Table contract_id={contract_id}, column={col!r}.\n"
            f"Inferred type: {clause.get('type')}. Samples: {samples[:5]}.\n"
            f"Other columns (subset): {adjacent_preview}.\n"
            "Reply with compact JSON only: "
            '{"description":"plain English","validation_hint":"one-line rule",'
            '"related_columns":["col_a"]}'
        )
        try:
            msg = client.messages.create(
                model=llm_model,
                max_tokens=400,
                messages=[{"role": "user", "content": user_msg}],
            )
            text = msg.content[0].text if msg.content else ""
            m = re.search(r"\{[\s\S]*\}", text)
            if not m:
                continue
            parsed = json.loads(m.group(0))
            out[col] = {
                "description": parsed.get("description"),
                "validation_hint": parsed.get("validation_hint"),
                "related_columns": parsed.get("related_columns") or [],
            }
        except Exception:
            continue
    return out


def build_contract(
    args: argparse.Namespace,
    profiles: dict,
    profiling_alerts: list[dict],
    llm_annotations: dict[str, dict],
) -> dict:
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

    # Overlay LLM descriptions where present
    for col, ann in llm_annotations.items():
        if col in schema and ann.get("description"):
            schema[col]["description"] = str(ann["description"])
            if ann.get("validation_hint"):
                schema[col]["llm_validation_hint"] = str(ann["validation_hint"])
            if ann.get("related_columns"):
                schema[col]["llm_related_columns"] = list(ann["related_columns"])

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
        "profiling_alerts": profiling_alerts,
        "quality": build_quality_checks(schema, profiling_alerts),
    }
    if llm_annotations:
        contract["llm_annotations"] = llm_annotations
    return contract


def save_snapshot(output_path: Path, contract_id: str) -> None:
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"
    shutil.copy(output_path, snapshot_path)


def default_lineage_path(source: Path) -> Path | None:
    cands = [
        Path("outputs/week4/lineage_snapshots.jsonl"),
        source.parent.parent / "week4" / "lineage_snapshots.jsonl",
        Path(__file__).resolve().parent.parent / "outputs" / "week4" / "lineage_snapshots.jsonl",
    ]
    for p in cands:
        if p.exists():
            return p
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Bitol-compatible data contracts from JSONL.",
    )
    parser.add_argument("--source", required=True, type=Path, help="Input JSONL path.")
    parser.add_argument(
        "--contract-id",
        default=None,
        help="Contract id (default: infer from --source path).",
    )
    parser.add_argument(
        "--lineage",
        default=None,
        type=Path,
        help="Week 4 lineage_snapshots.jsonl (default: outputs/week4/ if present).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("generated_contracts"),
        help="Directory for generated YAML (default: generated_contracts/).",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("contract_registry/subscriptions.yaml"),
        help="Subscription registry YAML.",
    )
    parser.add_argument(
        "--no-ydata",
        action="store_true",
        help="Skip ydata-profiling enrichment (faster; pandas stats only).",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip Anthropic LLM annotations (requires ANTHROPIC_API_KEY otherwise).",
    )
    parser.add_argument(
        "--llm-model",
        default=os.environ.get("ANTHROPIC_MODEL", "claude-3-5-haiku-20241022"),
        help="Anthropic model id for column annotations.",
    )
    args = parser.parse_args()

    args.source = args.source.resolve()
    args.output = args.output.resolve()
    args.registry = Path(args.registry).resolve()

    reg_val = registry_util.validate_registry(args.registry)
    if reg_val["errors"]:
        raise ValueError("Invalid contract registry:\n" + "\n".join(reg_val["errors"]))
    for w in reg_val["warnings"]:
        print(f"Registry warning: {w}")

    if not args.contract_id:
        args.contract_id = infer_contract_id(args.source)

    if args.lineage is None:
        args.lineage = default_lineage_path(args.source)
    elif args.lineage is not None:
        args.lineage = args.lineage.resolve()
        if not args.lineage.exists():
            print(f"Warning: lineage file not found at {args.lineage}; using graph-free defaults + registry.")
            args.lineage = None

    args.output.mkdir(parents=True, exist_ok=True)

    records = load_jsonl(args.source)
    df = records_to_dataframe(records, args.contract_id)
    if df.empty:
        raise ValueError(f"No rows found in source: {args.source}")

    profiles = {col: profile_column(df[col], col) for col in df.columns}
    profiles = apply_ydata_profiling(df, profiles, enabled=not args.no_ydata)

    # Provisional schema for ambiguous-column detection (before lineage mutates clauses)
    provisional = {name: column_to_clause(p, args.contract_id) for name, p in profiles.items()}
    ambiguous = [c for c, cl in provisional.items() if is_ambiguous_clause(cl)]
    llm_annotations: dict[str, dict] = {}
    if not args.no_llm and ambiguous:
        llm_annotations = llm_annotate_columns(
            args.contract_id,
            provisional,
            profiles,
            sorted(ambiguous),
            args.llm_model,
        )

    schema_pre = {name: column_to_clause(p, args.contract_id) for name, p in profiles.items()}
    for col, ann in llm_annotations.items():
        if col in schema_pre and ann.get("description"):
            schema_pre[col]["description"] = str(ann["description"])

    profiling_alerts = compute_profiling_alerts(schema_pre)

    class Namespace:
        pass

    ns = Namespace()
    ns.contract_id = args.contract_id
    ns.source = args.source

    contract = build_contract(ns, profiles, profiling_alerts, llm_annotations)
    contract = inject_lineage(contract, args.lineage, args.contract_id, args.registry)

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
    print(f"Profiling alerts: {len(contract.get('profiling_alerts') or [])}")
    print(f"LLM annotations: {len(contract.get('llm_annotations') or {})}")


if __name__ == "__main__":
    main()
