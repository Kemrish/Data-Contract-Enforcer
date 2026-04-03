# Data Contract Enforcer — Sunday submission runbook

End-to-end commands for a fresh clone with Python 3.11+, `pip install -r requirements.txt`, and sample data under `outputs/` (run `python scripts/generate_sample_outputs.py` if needed).

---

### Step 0 — Registry

File: `contract_registry/subscriptions.yaml` (already committed).  
Verify: at least four `subscriber_id` entries for Week 3→4, Week 4→7, Week 5→7, LangSmith→7 style dependencies.

```bash
grep -c subscriber_id contract_registry/subscriptions.yaml
```

Expected: integer ≥ 4.

---

### Step 1 — ContractGenerator

Generates Bitol-style YAML + dbt + `schema_snapshots/<contract_id>/<timestamp>.yaml`.

**Evaluator command (minimum):**

```bash
python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts/
```

**All five targets:**

```bash
python contracts/generator.py --source outputs/week1/intent_records.jsonl --output generated_contracts/
python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts/
python contracts/generator.py --source outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
python contracts/generator.py --source outputs/week5/events.jsonl --output generated_contracts/
python contracts/generator.py --source outputs/traces/runs.jsonl --output generated_contracts/
```

Expected (examples):

- `generated_contracts/week3_extractions.yaml` and `week3_extractions_dbt.yml`
- Console: `Contract written: ...` and `Schema fields: N`
- New file under `schema_snapshots/week3-document-refinery-extractions/*.yaml`

Optional: `--no-ydata` (faster), `--no-llm` (skip Anthropic blurbs; set `ANTHROPIC_API_KEY` for LLM annotations).

---

### Step 2 — ValidationRunner

Structured JSON report; **`--mode`** controls blocking: **AUDIT** (default, never exit 1), **WARN** (exit 1 on FAIL+CRITICAL), **ENFORCE** (exit 1 on FAIL+CRITICAL|HIGH). Use **`--no-baseline-write`** on non-baseline runs so `schema_snapshots/baselines.json` is not overwritten by bad data.

**Clean baseline (run once on clean data):**

```bash
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/clean_week3.json --mode AUDIT
```

Expected: `validation_reports/clean_week3.json` with `"failed": 0` (or only acceptable WARNs), `pipeline_action`: `"PASS"`, `exit code 0`. Baselines updated under `schema_snapshots/baselines.json` for the contract.

**Injected violation (scale change 0–100):**

```bash
python scripts/create_violation.py
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions_violated.jsonl --output validation_reports/violated_week3.json --mode AUDIT --no-baseline-write
```

Expected: `violated_week3.json` contains `"status": "FAIL"` for `fact_confidence.range` and drift; `exit code 0` in AUDIT.

**Strict enforcement demo:**

```bash
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions_violated.jsonl --output validation_reports/violated_enforce.json --mode ENFORCE --no-baseline-write
```

Expected: `pipeline_action` `"BLOCK"`, **exit code 1**.

---

### Step 3 — ViolationAttributor

**Requires:** a validation report with FAIL rows (e.g. Step 2 violated run). Appends JSON lines to `violation_log/violations.jsonl`.

```bash
python contracts/attributor.py --validation-report validation_reports/violated_week3.json --lineage outputs/week4/lineage_snapshots.jsonl --registry contract_registry/subscriptions.yaml --output violation_log/violations.jsonl
```

Expected: console `Appended N violation record(s)`; each line has `blast_radius.direct_subscribers`, `blame_chain`, `injection_note` true when the report path contains `violated`.

---

### Step 4 — SchemaEvolutionAnalyzer

Diffs two snapshots under `schema_snapshots/<contract_id>/` (or pass explicit paths).

```bash
python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --output validation_reports/schema_evolution.json
```

Expected: `validation_reports/schema_evolution.json` and `validation_reports/migration_impact_week3-document-refinery-extractions_<timestamp>.json` with breaking/compatible classification and migration checklist.

---

### Step 5 — AI Contract Extensions

```bash
python contracts/ai_extensions.py --extractions outputs/week3/extractions.jsonl --verdicts outputs/week2/verdicts.jsonl --output validation_reports/ai_extensions.json
```

Expected: `validation_reports/ai_extensions.json` with `embedding_drift` ( **`BASELINE_SET`** / **`PASS`** / **`FAIL`** if `OPENAI_API_KEY` set; else **`SKIP`** ), `prompt_input_validation`, `llm_output_schema_violation_rate`.

---

### Step 6 — ReportGenerator

```bash
python contracts/report_generator.py --reports-dir validation_reports --violations violation_log/violations.jsonl --ai-extensions validation_reports/ai_extensions.json --output enforcer_report/report_data.json
```

Expected: `enforcer_report/report_data.json` with `data_health_score` between **0** and **100**, `top_violations_plain_language`, `recommendations` pointing at repo paths (e.g. `src/week3/extractor.py`). PDF export is out of band (e.g. render from JSON for Google Drive).

---

### Open `enforcer_report/report_data.json`

Confirm:

- `data_health_score` is 0–100.
- `top_violations_plain_language` names the failing contract and field.
- `recommendations` reference concrete files and contracts.

---

### Repository layout (submission)

| Path | Role |
|------|------|
| `contracts/generator.py` | Contract generator |
| `contracts/runner.py` | Validation runner |
| `contracts/attributor.py` | Violation attribution |
| `contracts/schema_analyzer.py` | Snapshot diff + migration impact |
| `contracts/ai_extensions.py` | Embedding / prompt / verdict checks |
| `contracts/report_generator.py` | Enforcer report JSON |
| `contract_registry/subscriptions.yaml` | Blast-radius registry |
| `generated_contracts/` | Generated YAML + dbt |
| `validation_reports/` | Runner + AI + schema evolution outputs |
| `violation_log/violations.jsonl` | Violation records (first line may be `#` documentation) |
| `schema_snapshots/` | Timestamped contracts + `baselines.json` |
| `enforcer_report/report_data.json` | Machine-generated stakeholder summary |
| `scripts/create_violation.py` | Builds `outputs/week3/extractions_violated.jsonl` |
| `scripts/generate_sample_outputs.py` | Fills `outputs/` when empty |

---

### Domain notes

See `DOMAIN_NOTES.md` for Phase 0 answers (≥800 words) and evidence from this platform.
