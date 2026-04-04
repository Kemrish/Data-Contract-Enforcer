#!/usr/bin/env python3
"""
Pull real artifacts from Week 3 (doc-refinery), Week 4 (Brownfield Cartographer),
and Week 5 (The Ledger) into outputs/week{3,4,5} JSONL shapes for Contract Enforcer.

Resolution order for each project root:
  1) <repo>/upstream/<folder>
  2) <repo>/../<folder> on Desktop-style layouts
  3) Explicit CLI overrides

After syncing, regenerate contracts from the new JSONL:

  python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts/
  python contracts/generator.py --source outputs/week4/lineage_snapshots.jsonl --contract-id week4-brownfield-cartographer-lineage --output generated_contracts/
  python contracts/generator.py --source outputs/week5/events.jsonl --contract-id week5-event-records --output generated_contracts/
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _first_existing(paths: Iterable[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


def default_doc_refinery_root(repo: Path) -> Path | None:
    p = _first_existing(
        [
            repo / "upstream" / "doc-refinery",
            repo.parent / "doc-refinery",
        ]
    )
    return p


def default_cartographer_root(repo: Path) -> Path | None:
    p = _first_existing(
        [
            repo / "upstream" / "brownfield-cartographer",
            repo.parent / "TRP1" / "New folder" / "Brownfield-Cartographer",
        ]
    )
    return p


def default_ledger_root(repo: Path) -> Path | None:
    p = _first_existing(
        [
            repo / "upstream" / "the-ledger",
            repo.parent / "The Ledger",
        ]
    )
    return p


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def ledger_row_to_extraction(row: dict[str, Any]) -> dict[str, Any]:
    """Map doc-refinery extraction_ledger.jsonl line → week3 extractions.jsonl row."""
    doc_id = str(row.get("doc_id", "unknown"))
    doc_name = str(row.get("document_name", doc_id))
    conf = float(row.get("confidence_score", 0.0))
    conf = max(0.0, min(1.0, conf))
    ms = int(row.get("processing_time_ms", 0))
    ts = row.get("timestamp")
    if isinstance(ts, (int, float)):
        extracted_at = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        extracted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    fact_id = str(uuid.uuid4())
    return {
        "doc_id": doc_id if len(doc_id) == 36 and doc_id.count("-") == 4 else str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)),
        "source_path": doc_name,
        "source_hash": _sha256_hex(doc_id + doc_name),
        "extracted_facts": [
            {
                "fact_id": fact_id,
                "text": f"Ledger extraction run for {doc_name}",
                "entity_refs": [],
                "confidence": conf,
                "page_ref": 1,
                "source_excerpt": "",
            }
        ],
        "entities": [],
        "extraction_model": str(row.get("strategy_used", "layout_aware")),
        "processing_time_ms": ms,
        "token_count": {"input": 0, "output": 0},
        "extracted_at": extracted_at,
    }


def sync_week3_extractions(ledger_path: Path, dest: Path) -> int:
    lines_out: list[dict[str, Any]] = []
    for ln in ledger_path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            row = json.loads(ln)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            lines_out.append(ledger_row_to_extraction(row))
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in lines_out) + "\n", encoding="utf-8")
    return len(lines_out)


def _canon_lineage_endpoint(endpoint: str) -> str:
    if endpoint.startswith("sql:"):
        path_part = endpoint[4:]
        name = Path(path_part.replace("/", "\\")).name
        return f"file::{name}"
    return f"dataset::{endpoint}"


def cartography_to_snapshot(lineage_json: Path, last_run_json: Path | None) -> dict[str, Any]:
    data = json.loads(lineage_json.read_text(encoding="utf-8"))
    meta = data.get("metadata") or {}
    commit = "0" * 40
    if last_run_json and last_run_json.exists():
        try:
            lr = json.loads(last_run_json.read_text(encoding="utf-8"))
            c = lr.get("commit")
            if isinstance(c, str) and len(c) == 40 and all(x in "0123456789abcdef" for x in c.lower()):
                commit = c.lower()
        except (json.JSONDecodeError, OSError):
            pass

    nodes: dict[str, dict[str, Any]] = {}
    for ds in data.get("datasets") or []:
        if not isinstance(ds, dict):
            continue
        name = ds.get("name")
        if not name:
            continue
        nid = _canon_lineage_endpoint(str(name))
        nodes[nid] = {
            "node_id": nid,
            "type": "DATASET",
            "label": str(name),
            "metadata": {
                "path": ds.get("source_file"),
                "storage_type": ds.get("storage_type"),
            },
        }
    for tf in data.get("transformations") or []:
        if not isinstance(tf, dict):
            continue
        tid = tf.get("id")
        if not tid or not str(tid).startswith("sql:"):
            continue
        nid = _canon_lineage_endpoint(str(tid))
        path_part = str(tid)[4:]
        nodes[nid] = {
            "node_id": nid,
            "type": "PIPELINE",
            "label": Path(path_part.replace("/", "\\")).name,
            "metadata": {"path": path_part, "transformation_type": tf.get("transformation_type")},
        }

    edges_out: list[dict[str, Any]] = []
    for e in data.get("edges") or []:
        if not isinstance(e, dict):
            continue
        s, t = e.get("source"), e.get("target")
        if s is None or t is None:
            continue
        et = str(e.get("edge_type", "RELATES")).upper()
        edges_out.append(
            {
                "source": _canon_lineage_endpoint(str(s)),
                "target": _canon_lineage_endpoint(str(t)),
                "relationship": et,
                "confidence": 0.9,
            }
        )

    # So Week 3 contract lineage hints still see a producer edge (generator graph_downstream_from_snapshot).
    bridge_src = "file::src/week3/extractor.py"
    bridge_tgt = next((n["node_id"] for n in nodes.values() if n.get("type") == "DATASET"), None)
    if bridge_tgt:
        nodes.setdefault(
            bridge_src,
            {"node_id": bridge_src, "type": "FILE", "label": "extractor.py", "metadata": {"path": "src/week3/extractor.py"}},
        )
        edges_out.append(
            {"source": bridge_src, "target": bridge_tgt, "relationship": "PRODUCES", "confidence": 0.95}
        )

    return {
        "snapshot_id": str(uuid.uuid4()),
        "codebase_root": str(meta.get("repo_path", "")),
        "git_commit": commit,
        "nodes": list(nodes.values()),
        "edges": edges_out,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def sync_week4_lineage(lineage_json: Path, last_run_json: Path | None, dest: Path) -> None:
    snap = cartography_to_snapshot(lineage_json, last_run_json)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(snap, ensure_ascii=False) + "\n", encoding="utf-8")


def _stable_uuid(*parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).digest()
    return str(uuid.UUID(bytes=digest[:16]))


def seed_event_to_week5(row: dict[str, Any], seq: int) -> dict[str, Any]:
    """Map The Ledger seed_events.jsonl → week5 events envelope (UUID-friendly)."""
    stream = str(row.get("stream_id", "stream"))
    et = str(row.get("event_type", "Unknown"))
    recorded = str(row.get("recorded_at", datetime.now(timezone.utc).isoformat()))
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    app = payload.get("application_id") or payload.get("package_id") or stream
    agg = _stable_uuid("agg", str(app))
    return {
        "event_id": _stable_uuid("ev", stream, et, recorded, str(seq)),
        "event_type": et,
        "aggregate_id": agg,
        "aggregate_type": "Document",
        "sequence_number": seq,
        "payload": payload,
        "metadata": {
            "causation_id": None,
            "correlation_id": stream,
            "user_id": "system-user",
            "source_service": "the-ledger",
        },
        "schema_version": "1.0",
        "occurred_at": recorded,
        "recorded_at": recorded,
    }


def sync_week5_events(seed_path: Path, dest: Path) -> int:
    out: list[dict[str, Any]] = []
    seq = 0
    for ln in seed_path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            row = json.loads(ln)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            seq += 1
            out.append(seed_event_to_week5(row, seq))
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in out) + "\n", encoding="utf-8")
    return len(out)


def main() -> None:
    repo = _repo_root()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--doc-refinery", type=Path, default=None, help="Root of doc-refinery clone")
    ap.add_argument("--cartographer", type=Path, default=None, help="Root of Brownfield-Cartographer clone")
    ap.add_argument("--ledger", type=Path, default=None, help="Root of The Ledger clone")
    ap.add_argument("--out-week3", type=Path, default=repo / "outputs" / "week3" / "extractions.jsonl")
    ap.add_argument("--out-week4", type=Path, default=repo / "outputs" / "week4" / "lineage_snapshots.jsonl")
    ap.add_argument("--out-week5", type=Path, default=repo / "outputs" / "week5" / "events.jsonl")
    args = ap.parse_args()

    dr = args.doc_refinery or default_doc_refinery_root(repo)
    cart = args.cartographer or default_cartographer_root(repo)
    led = args.ledger or default_ledger_root(repo)

    if not dr:
        print("doc-refinery root not found. Place it at upstream/doc-refinery or ../doc-refinery, or pass --doc-refinery.")
    else:
        ledger = dr / ".refinery" / "extraction_ledger.jsonl"
        if not ledger.exists():
            print(f"No extraction ledger at {ledger} (run doc-refinery extractions first).")
        else:
            n = sync_week3_extractions(ledger, args.out_week3.resolve())
            print(f"Week 3: wrote {n} rows -> {args.out_week3}")

    if not cart:
        print("Brownfield Cartographer root not found. Use upstream/brownfield-cartographer or pass --cartographer.")
    else:
        lg = cart / "out" / ".cartography" / "lineage_graph.json"
        if not lg.exists():
            alt = sorted(cart.glob("**/.cartography/lineage_graph.json"))
            lg = alt[-1] if alt else lg
        lr = cart / "out" / ".cartography" / "last_run.json"
        if not lr.exists():
            alt_lr = sorted(cart.glob("**/.cartography/last_run.json"))
            lr = alt_lr[-1] if alt_lr else lr
        if not lg.exists():
            print(f"No lineage_graph.json under {cart} (run cartographer analyze first).")
        else:
            sync_week4_lineage(lg, lr if lr.exists() else None, args.out_week4.resolve())
            print(f"Week 4: wrote snapshot -> {args.out_week4} (from {lg})")

    if not led:
        print("The Ledger root not found. Use upstream/the-ledger or ../The Ledger, or pass --ledger.")
    else:
        seed = led / "data" / "seed_events.jsonl"
        if not seed.exists():
            print(f"No seed_events.jsonl at {seed}.")
        else:
            n = sync_week5_events(seed, args.out_week5.resolve())
            print(f"Week 5: wrote {n} rows -> {args.out_week5}")

    print("\nRegenerate contracts from the updated JSONL (see script docstring).")


if __name__ == "__main__":
    main()
