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

### Step 0.5 — Real outputs from your Week 3 / 4 / 5 repos (optional)

If **doc-refinery**, **Brownfield-Cartographer**, and **The Ledger** live next to this repo on the Desktop, sync their artifacts into `outputs/`:

```bash
python scripts/sync_upstream_outputs.py
```

The script looks for:

| Source | Default path tried |
|--------|---------------------|
| Week 3 — extraction ledger | `../doc-refinery/.refinery/extraction_ledger.jsonl` or `upstream/doc-refinery/.refinery/extraction_ledger.jsonl` |
| Week 4 — lineage graph | `../TRP1/New folder/Brownfield-Cartographer/**/.cartography/lineage_graph.json` or `upstream/brownfield-cartographer/...` |
| Week 5 — domain events | `../The Ledger/data/seed_events.jsonl` or `upstream/the-ledger/data/seed_events.jsonl` |

Overrides: `--doc-refinery`, `--cartographer`, `--ledger` (each is the **project root**).

Writes: `outputs/week3/extractions.jsonl`, `outputs/week4/lineage_snapshots.jsonl` (one snapshot line), `outputs/week5/events.jsonl`.  
**Then re-run Step 1** for week 3, 4, and 5 so generated YAML matches the new data (especially week 5 event types).

To pin the repos *inside* this workspace, clone or junction them as `upstream/doc-refinery`, `upstream/brownfield-cartographer`, and `upstream/the-ledger` (see `upstream/.gitkeep`).

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
- **`generated_contracts/week3_extractions_profiling_evidence.json`** — row/column counts, per-column dtypes/null rates, numeric **mean/stddev/p95**, `confidence_0_1_contract_clauses_on`, and full audit trail from the JSONL (not template-only).
- YAML includes **`profiling_metadata`** (summary + pointer to the evidence file).
- Numeric means/stddev are **merged into** `schema_snapshots/baselines.json` for that `contract_id` (same shape ValidationRunner uses), unless `--no-persist-runner-baselines`.
- Console: `Contract written: ...` and `Schema fields: N`
- New file under `schema_snapshots/week3-document-refinery-extractions/*.yaml`

Optional: `--no-ydata` (faster), `--no-llm` (skip Anthropic blurbs; set `ANTHROPIC_API_KEY` for LLM annotations), `--no-profiling-evidence-json`, `--no-persist-runner-baselines`.

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

Expected: `validation_reports/schema_evolution.json` and `validation_reports/migration_impact_<contract>_<timestamp>.json` with:

- Per-field **taxonomy** + **severity** (`CRITICAL` for narrow type / scale regressions, e.g. probability float 0–1 → integer 0–100).
- **`blast_radius`**: `affected_subscriber_count` from `contract_registry/subscriptions.yaml`, plus optional lineage graph counts from `--lineage` (default `outputs/week4/lineage_snapshots.jsonl`).
- **`per_consumer_failure_modes`** built from registry `breaking_fields` (not static examples only).

Optional: `--registry path/to/subscriptions.yaml`, `--lineage path` or a missing file to skip graph metrics.

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
| `generated_contracts/` | Generated YAML + dbt + `*_profiling_evidence.json` (data-driven audit) |
| `validation_reports/` | Runner + AI + schema evolution outputs |
| `violation_log/violations.jsonl` | Violation records (first line may be `#` documentation) |
| `schema_snapshots/` | Timestamped contracts + `baselines.json` |
| `enforcer_report/report_data.json` | Machine-generated stakeholder summary |
| `scripts/create_violation.py` | Builds `outputs/week3/extractions_violated.jsonl` |
| `scripts/generate_sample_outputs.py` | Fills `outputs/` when empty |

---

### Domain notes

See `DOMAIN_NOTES.md` for Phase 0 answers (≥800 words) and evidence from this platform.
