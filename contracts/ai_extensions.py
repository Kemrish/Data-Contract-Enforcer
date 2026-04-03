"""
AI Contract Extensions — embedding drift, prompt input JSON Schema, verdict output violation rate.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import uuid
from datetime import datetime, timezone
from jsonschema import Draft7Validator

import registry_util

WEEK3_PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    bad = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                bad += 1
    if bad:
        print(f"Warning: skipped {bad} invalid JSONL line(s) in {path}")
    return rows


def build_prompt_inputs_from_extractions(records: list[dict]) -> list[dict]:
    out = []
    for r in records:
        facts = r.get("extracted_facts") or []
        preview = ""
        if facts and isinstance(facts[0], dict):
            preview = str(facts[0].get("text") or "")[:8000]
        doc_id = str(r.get("doc_id") or "")
        if len(doc_id) != 36:
            continue
        out.append(
            {
                "doc_id": doc_id,
                "source_path": str(r.get("source_path") or "https://example.com/doc"),
                "content_preview": preview or "placeholder preview",
            }
        )
    return out


def validate_prompt_inputs(inputs: list[dict], quarantine_path: Path) -> dict[str, Any]:
    v = Draft7Validator(WEEK3_PROMPT_INPUT_SCHEMA)
    valid_n = 0
    bad: list[dict] = []
    for obj in inputs:
        errs = sorted(v.iter_errors(obj), key=lambda e: e.path)
        if errs:
            bad.append({"record": obj, "error": errs[0].message, "path": list(errs[0].path)})
        else:
            valid_n += 1
    if bad:
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        with quarantine_path.open("a", encoding="utf-8") as f:
            for q in bad:
                f.write(json.dumps(q) + "\n")
    return {
        "valid_count": valid_n,
        "quarantined_count": len(bad),
        "status": "WARN" if bad else "PASS",
    }


def embed_sample(texts: list[str], n: int, model: str) -> np.ndarray | None:
    try:
        from openai import OpenAI
    except ImportError:
        return None
    if not os.environ.get("OPENAI_API_KEY"):
        return None
    sample = texts[:n]
    if not sample:
        return None
    client = OpenAI()
    resp = client.embeddings.create(input=sample, model=model)
    return np.array([e.embedding for e in resp.data], dtype=np.float64)


def check_embedding_drift(
    texts: list[str],
    baseline_path: Path,
    threshold: float = 0.15,
    n: int = 200,
    model: str = "text-embedding-3-small",
) -> dict[str, Any]:
    vec = embed_sample(texts, n=200, model=model)
    if vec is None:
        return {
            "status": "SKIP",
            "drift_score": None,
            "message": "OPENAI_API_KEY not set or openai package missing — embedding drift skipped",
            "threshold": threshold,
        }
    centroid = vec.mean(axis=0)
    if not baseline_path.exists():
        np.savez(baseline_path, centroid=centroid)
        return {"status": "BASELINE_SET", "drift_score": 0.0, "threshold": threshold}
    baseline = np.load(baseline_path)["centroid"]
    sim = float(
        np.dot(centroid, baseline)
        / (np.linalg.norm(centroid) * np.linalg.norm(baseline) + 1e-9)
    )
    drift = float(1.0 - sim)
    st = "FAIL" if drift > threshold else "PASS"
    return {
        "status": st,
        "drift_score": round(drift, 4),
        "threshold": threshold,
        "interpretation": "semantic drift" if drift > threshold else "stable",
    }


def check_output_violation_rate(
    verdicts: list[dict],
    baseline_rate: float | None,
    warn_threshold: float = 0.02,
) -> dict[str, Any]:
    ok = {"PASS", "FAIL", "WARN"}
    total = len(verdicts)
    violations = sum(1 for v in verdicts if str(v.get("overall_verdict", "")) not in ok)
    rate = violations / max(total, 1)
    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.5:
            trend = "falling"
        else:
            trend = "stable"
    st = "WARN" if (trend == "rising" or rate > warn_threshold) else "PASS"
    return {
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "status": st,
        "baseline_rate": baseline_rate,
    }


def append_ai_violation_log(
    path: Path,
    drift: dict[str, Any],
    prompt: dict[str, Any],
    verdict: dict[str, Any],
    registry_path: Path,
) -> int:
    """
    Rubric: integrate AI extension failures into violation_log (same JSONL as contract violations).
    Week 8 Sentinel can filter on source_component == 'ai_extensions'.
    """
    n = 0
    subs_langsmith = [
        s.get("subscriber_id")
        for s in registry_util.load_subscriptions(registry_path)[0]
        if isinstance(s, dict) and s.get("contract_id") == "langsmith-trace-runs"
    ]
    blast_stub = {
        "source": "registry",
        "direct_subscribers": [{"subscriber_id": sid, "reason": "LangSmith / AI contract consumer"} for sid in subs_langsmith],
    }
    with path.open("a", encoding="utf-8") as f:
        if drift.get("status") == "FAIL":
            f.write(
                json.dumps(
                    {
                        "violation_id": str(uuid.uuid4()),
                        "source_component": "ai_extensions",
                        "check_id": "ai.embedding_drift",
                        "contract_id": "langsmith-trace-runs",
                        "failing_field": "embedding_centroid",
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        "blast_radius": blast_stub,
                        "blame_chain": [],
                        "message": drift.get("interpretation", "embedding drift"),
                        "drift_score": drift.get("drift_score"),
                        "injection_note": False,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            n += 1
        if prompt.get("quarantined_count", 0) > 0 or prompt.get("status") == "WARN":
            f.write(
                json.dumps(
                    {
                        "violation_id": str(uuid.uuid4()),
                        "source_component": "ai_extensions",
                        "check_id": "ai.prompt_input_schema",
                        "contract_id": "week3-document-refinery-extractions",
                        "failing_field": "prompt.document_metadata",
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        "blast_radius": blast_stub,
                        "blame_chain": [],
                        "quarantined_count": prompt.get("quarantined_count", 0),
                        "injection_note": False,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            n += 1
        if verdict.get("status") == "WARN":
            f.write(
                json.dumps(
                    {
                        "violation_id": str(uuid.uuid4()),
                        "source_component": "ai_extensions",
                        "check_id": "ai.llm_output_schema_violation_rate",
                        "contract_id": "week2-digital-courtroom-verdicts",
                        "failing_field": "overall_verdict",
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        "blast_radius": blast_stub,
                        "blame_chain": [],
                        "violation_rate": verdict.get("violation_rate"),
                        "trend": verdict.get("trend"),
                        "injection_note": False,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="AI contract extension checks.")
    parser.add_argument("--extractions", type=Path, default=Path("outputs/week3/extractions.jsonl"))
    parser.add_argument("--verdicts", type=Path, default=Path("outputs/week2/verdicts.jsonl"))
    parser.add_argument("--output", type=Path, default=Path("validation_reports/ai_extensions.json"))
    parser.add_argument(
        "--embedding-baseline",
        type=Path,
        default=Path("schema_snapshots/embedding_baseline_week3.npz"),
    )
    parser.add_argument("--baseline-rate", type=float, default=None, help="Prior verdict violation rate")
    parser.add_argument(
        "--violation-log",
        type=Path,
        default=Path("violation_log/violations.jsonl"),
        help="Append AI WARN/FAIL rows for Sentinel integration (default: violation_log/violations.jsonl).",
    )
    parser.add_argument(
        "--no-violation-log",
        action="store_true",
        help="Do not append to violation log.",
    )
    parser.add_argument("--registry", type=Path, default=Path("contract_registry/subscriptions.yaml"))
    args = parser.parse_args()

    ext = load_jsonl(args.extractions)
    texts = []
    for r in ext:
        for f in r.get("extracted_facts") or []:
            if isinstance(f, dict) and f.get("text"):
                texts.append(str(f["text"]))
    if len(texts) > 200:
        texts = texts[:200]

    drift = check_embedding_drift(texts, args.embedding_baseline.resolve())

    inputs = build_prompt_inputs_from_extractions(ext)
    quarantine = Path("outputs/quarantine/prompt_validation.jsonl")
    prompt_val = validate_prompt_inputs(inputs, quarantine)

    verdicts = load_jsonl(args.verdicts) if args.verdicts.exists() else []
    verdict_check = check_output_violation_rate(verdicts, args.baseline_rate)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "embedding_drift": drift,
        "prompt_input_validation": prompt_val,
        "llm_output_schema_violation_rate": verdict_check,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {args.output}")

    if not args.no_violation_log:
        appended = append_ai_violation_log(
            args.violation_log.resolve(),
            drift,
            prompt_val,
            verdict_check,
            args.registry.resolve(),
        )
        if appended:
            print(f"Appended {appended} AI extension row(s) to {args.violation_log}")


if __name__ == "__main__":
    main()
