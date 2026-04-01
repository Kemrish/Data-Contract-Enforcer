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

## Rubric alignment (generated artifacts)

| Rubric area | What the repo shows |
|-------------|---------------------|
| Bitol-style YAML | `kind`, `apiVersion`, `id`, `info` (incl. `contact`), `terms.usage` / `terms.limitations`, `servers`, `schema` (≥8 fields per contract), `schema_annotations`, `quality`, `lineage` |
| dbt counterparts | `*_dbt.yml`: `not_null`, `unique` (where contract marks `unique: true`), `accepted_values` (enums), `relationships` on `payload_doc_id` → `ref('week3_extractions').doc_id` (Week 5) |
| Generator | `python contracts/generator.py ...` writes YAML + dbt + `schema_snapshots/<contract_id>/` copy |
| Runner | Pattern / UUID / datetime / range / enum / uniqueness; `recorded_at >= occurred_at` for events; statistical drift vs per-contract baselines |

## Git ignore

`.gitignore` excludes local submission drafts such as `INTERIM_SUBMISSION_REPORT.md`. **If you already committed that file**, run `git rm --cached INTERIM_SUBMISSION_REPORT.md` once so Git stops tracking it.

## Interim deliverables included

- `DOMAIN_NOTES.md` with 5 required answers and concrete examples
- Runnable `contracts/generator.py`
- Runnable `contracts/runner.py`
- Real generated contracts for Week 3 and Week 5
- Real validation report from local run
