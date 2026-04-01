import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


def ensure(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def build_week3(n: int = 60) -> list[dict]:
    rows = []
    base_time = datetime.now(timezone.utc) - timedelta(days=1)
    entity_types = ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]
    for i in range(n):
        doc_id = str(uuid4())
        entities = []
        for _ in range(3):
            entities.append(
                {
                    "entity_id": str(uuid4()),
                    "name": random.choice(["Acme Corp", "Nairobi", "Invoice 81", "John Doe"]),
                    "type": random.choice(entity_types),
                    "canonical_value": "canonical-value",
                }
            )
        extracted_facts = []
        for _ in range(4):
            extracted_facts.append(
                {
                    "fact_id": str(uuid4()),
                    "text": random.choice(
                        [
                            "Payment due in 30 days.",
                            "Service uptime target is 99.9%.",
                            "Contract signed on 2025-11-10.",
                        ]
                    ),
                    "entity_refs": [entities[0]["entity_id"], entities[1]["entity_id"]],
                    "confidence": round(random.uniform(0.62, 0.99), 3),
                    "page_ref": random.randint(1, 12),
                    "source_excerpt": "Excerpt from source document.",
                }
            )
        rows.append(
            {
                "doc_id": doc_id,
                "source_path": f"https://example.com/docs/{i}",
                "source_hash": "".join(random.choice("abcdef0123456789") for _ in range(64)),
                "extracted_facts": extracted_facts,
                "entities": entities,
                "extraction_model": "claude-3-5-sonnet-20241022",
                "processing_time_ms": random.randint(600, 2200),
                "token_count": {"input": random.randint(1500, 4000), "output": random.randint(300, 1000)},
                "extracted_at": iso(base_time + timedelta(minutes=i)),
            }
        )
    return rows


def build_week5(n: int = 60) -> list[dict]:
    rows = []
    base_time = datetime.now(timezone.utc) - timedelta(days=1)
    agg_ids = [str(uuid4()) for _ in range(8)]
    seq = {a: 0 for a in agg_ids}
    for i in range(n):
        agg = random.choice(agg_ids)
        seq[agg] += 1
        occurred = base_time + timedelta(seconds=i * 10)
        recorded = occurred + timedelta(seconds=1)
        rows.append(
            {
                "event_id": str(uuid4()),
                "event_type": random.choice(["DocumentProcessed", "DocumentQueued", "ValidationFailed"]),
                "aggregate_id": agg,
                "aggregate_type": "Document",
                "sequence_number": seq[agg],
                "payload": {"doc_id": str(uuid4()), "status": "ok"},
                "metadata": {
                    "causation_id": None,
                    "correlation_id": str(uuid4()),
                    "user_id": "system-user",
                    "source_service": "week3-document-refinery",
                },
                "schema_version": "1.0",
                "occurred_at": iso(occurred),
                "recorded_at": iso(recorded),
            }
        )
    return rows


def build_week4_lineage() -> list[dict]:
    snap = {
        "snapshot_id": str(uuid4()),
        "codebase_root": str(Path.cwd()),
        "git_commit": "".join(random.choice("abcdef0123456789") for _ in range(40)),
        "nodes": [
            {"node_id": "file::src/week3/extractor.py", "type": "FILE", "label": "extractor.py", "metadata": {"path": "src/week3/extractor.py"}},
            {"node_id": "pipeline::week4-cartographer", "type": "PIPELINE", "label": "week4 cartographer", "metadata": {"path": "src/week4/cartographer.py"}},
            {"node_id": "pipeline::week5-events", "type": "PIPELINE", "label": "week5 events", "metadata": {"path": "src/week5/events.py"}},
        ],
        "edges": [
            {"source": "file::src/week3/extractor.py", "target": "pipeline::week4-cartographer", "relationship": "PRODUCES", "confidence": 0.95},
            {"source": "file::src/week3/extractor.py", "target": "pipeline::week5-events", "relationship": "CONSUMES", "confidence": 0.72},
        ],
        "captured_at": iso(datetime.now(timezone.utc)),
    }
    return [snap]


def build_week1(n: int = 12) -> list[dict]:
    rows = []
    for _ in range(n):
        rows.append(
            {
                "intent_id": str(uuid4()),
                "description": "Extract facts from source docs",
                "code_refs": [{"file": "src/week3/extractor.py", "line_start": 10, "line_end": 40, "symbol": "extract", "confidence": 0.9}],
                "governance_tags": ["auth", "pii"],
                "created_at": iso(datetime.now(timezone.utc)),
            }
        )
    return rows


def build_week2(n: int = 60) -> list[dict]:
    rows = []
    for _ in range(n):
        rows.append(
            {
                "verdict_id": str(uuid4()),
                "target_ref": "src/week3/extractor.py",
                "rubric_id": "".join(random.choice("abcdef0123456789") for _ in range(64)),
                "rubric_version": "1.2.0",
                "scores": {"correctness": {"score": random.randint(1, 5), "evidence": ["ok"], "notes": "good"}},
                "overall_verdict": random.choice(["PASS", "FAIL", "WARN"]),
                "overall_score": round(random.uniform(1, 5), 2),
                "confidence": round(random.uniform(0.5, 0.99), 3),
                "evaluated_at": iso(datetime.now(timezone.utc)),
            }
        )
    return rows


def build_traces(n: int = 60) -> list[dict]:
    rows = []
    for _ in range(n):
        p = random.randint(1000, 3000)
        c = random.randint(200, 900)
        rows.append(
            {
                "id": str(uuid4()),
                "name": "week3-extraction-chain",
                "run_type": random.choice(["llm", "chain", "tool", "retriever", "embedding"]),
                "inputs": {},
                "outputs": {},
                "error": None,
                "start_time": iso(datetime.now(timezone.utc) - timedelta(seconds=3)),
                "end_time": iso(datetime.now(timezone.utc)),
                "total_tokens": p + c,
                "prompt_tokens": p,
                "completion_tokens": c,
                "total_cost": round(random.uniform(0.001, 0.02), 5),
                "tags": ["week3", "extraction"],
                "parent_run_id": None,
                "session_id": str(uuid4()),
            }
        )
    return rows


def main() -> None:
    ensure("outputs/week1")
    ensure("outputs/week2")
    ensure("outputs/week3")
    ensure("outputs/week4")
    ensure("outputs/week5")
    ensure("outputs/traces")

    write_jsonl(Path("outputs/week1/intent_records.jsonl"), build_week1())
    write_jsonl(Path("outputs/week2/verdicts.jsonl"), build_week2())
    write_jsonl(Path("outputs/week3/extractions.jsonl"), build_week3())
    write_jsonl(Path("outputs/week4/lineage_snapshots.jsonl"), build_week4_lineage())
    write_jsonl(Path("outputs/week5/events.jsonl"), build_week5())
    write_jsonl(Path("outputs/traces/runs.jsonl"), build_traces())
    print("Sample outputs generated.")


if __name__ == "__main__":
    main()
