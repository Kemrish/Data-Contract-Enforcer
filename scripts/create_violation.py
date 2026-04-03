#!/usr/bin/env python3
"""Create outputs/week3/extractions_violated.jsonl — confidence scaled 0.0–1.0 → 0–100 (rubric demo)."""

import json
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    src = root / "outputs" / "week3" / "extractions.jsonl"
    dst = root / "outputs" / "week3" / "extractions_violated.jsonl"
    if not src.exists():
        raise SystemExit(f"Missing {src}; run scripts/generate_sample_outputs.py first.")

    out = []
    with src.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            for fact in r.get("extracted_facts") or []:
                if isinstance(fact.get("confidence"), (int, float)):
                    fact["confidence"] = round(float(fact["confidence"]) * 100, 1)
            out.append(r)

    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w", encoding="utf-8") as f:
        for r in out:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(out)} records to {dst}")


if __name__ == "__main__":
    main()
