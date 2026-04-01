# Data Contract Enforcer (Interim)

## What this project does

It formalizes schema promises between your Week 1-5 systems, validates real data against those promises, and produces machine-readable evidence (`generated_contracts/`, `validation_reports/`, `schema_snapshots/`).

## Quick start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Generate sample local data (if you do not already have your own outputs):

```bash
python scripts/generate_sample_outputs.py
```

3. Generate Week 3 and Week 5 contracts:

```bash
python contracts/generator.py --source outputs/week3/extractions.jsonl --contract-id week3-document-refinery-extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
python contracts/generator.py --source outputs/week5/events.jsonl --contract-id week5-event-records --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
```

Expected files:
- `generated_contracts/week3_extractions.yaml`
- `generated_contracts/week3_extractions_dbt.yml`
- `generated_contracts/week5_events.yaml`
- `generated_contracts/week5_events_dbt.yml`

4. Run validation (structured JSON: PASS / FAIL / WARN / ERROR):

```bash
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/thursday_baseline.json
python contracts/runner.py --contract generated_contracts/week5_events.yaml --data outputs/week5/events.jsonl --output validation_reports/week5_baseline.json
```

Expected files:
- `validation_reports/thursday_baseline.json`
- `validation_reports/week5_baseline.json`

5. Check schema snapshot and baseline artifacts:

- `schema_snapshots/week3-document-refinery-extractions/*.yaml`
- `schema_snapshots/week5-event-records/*.yaml`
- `schema_snapshots/baselines.json` — numeric drift baselines are **namespaced per contract** under `contracts.<contract_id>.columns`

