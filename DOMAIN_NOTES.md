## 1) Backward-compatible vs breaking schema changes (from my Week 1-5 interfaces)

Backward-compatible changes preserve current consumers. Breaking changes require coordinated migration because current consumers fail or silently produce wrong output.

### Backward-compatible examples from this platform

1. **Week 5 `event_record.payload` adds a nullable key**  
   Adding `payload.processing_region` where missing values are allowed is compatible. Existing consumers that do not reference this key continue to work.

2. **Week 4 `lineage_snapshot.nodes[].metadata` adds optional `language_version`**  
   The existing cartographer readers already parse metadata as an object and do not require this property. This is additive and safe.

3. **Week 2 `verdict_record.scores` adds a new criterion name**  
   If downstream only computes aggregate over present criteria and does not require fixed criterion names, adding `robustness` is compatible.

### Breaking examples from this platform

1. **Week 3 `extracted_facts[].confidence` scale change 0.0-1.0 to 0-100**  
   This is breaking because consumers in Week 4 and reporting logic assume normalized probability-like semantics. Type may still be numeric, so this is silent corruption risk.

2. **Week 4 edge relationship enum removes `PRODUCES`**  
   Any pipeline expecting this relationship in lineage queries fails semantic filtering; attribution can miss edges and generate wrong blast radius.

3. **Week 5 rename `aggregate_id` to `entity_id` without alias period**  
   Consumers keyed on `aggregate_id` break immediately (missing field, dedupe logic failure, sequence checks fail).

The operating rule: additive nullable changes are usually compatible; removing, renaming, changing meaning, or narrowing representation is breaking.

## 2) Confidence scale failure path and Bitol clause

### Measured confidence distribution on current Week 3 output

Script output from local data check:

`min=0.621 max=0.990 mean=0.808`

This confirms current records are in expected 0.0-1.0 range.

### Failure chain if producer changes to 0-100

1. Week 3 extractor multiplies `confidence` by 100 and writes values like `87.4`.
2. Week 4 cartographer ingests those facts and treats high confidence as near-certainty; threshold filters and prioritization logic become meaningless.
3. Downstream consumers using lineage-informed risk scoring overstate certainty, creating false trust in extracted facts.
4. No crash occurs because field is still numeric. This is a silent corruption class, not a parser failure.

### Bitol-compatible clause to prevent this

```yaml
kind: DataContract
apiVersion: v3.0.0
id: week3-document-refinery-extractions
schema:
  fact_confidence:
    type: number
    required: true
    minimum: 0.0
    maximum: 1.0
    description: "Confidence must remain normalized probability scale."
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - min(fact_confidence) >= 0.0
      - max(fact_confidence) <= 1.0
```

This catches both explicit range violations and accidental scaling changes before propagation.

## 3) How lineage graph is used for blame chain attribution

The Week 4 lineage snapshot is the structural map that connects contract violations to likely upstream code owners.

Step-by-step process:

1. **Take failing check from ValidationRunner**  
   Example: `week3-document-refinery-extractions.fact_confidence.range` status FAIL.

2. **Map failing field to system/table context**  
   The attributor resolves `fact_confidence` as generated from Week 3 extraction output.

3. **Load latest lineage snapshot**  
   Read `outputs/week4/lineage_snapshots.jsonl` last record, then inspect `nodes[]` and `edges[]`.

4. **Find producer candidates**  
   Traverse upstream from consumer node (or from contract lineage metadata) using BFS over `READS/PRODUCES/CONSUMES`-style edges until file nodes are found.

5. **Rank by lineage distance**  
   Fewer hops means stronger causal likelihood. Hop penalty reduces confidence by `0.2` per edge.

6. **Overlay git history recency**  
   For candidate files run:
   `git log --follow --since="14 days ago" --format='%H|%an|%ae|%ai|%s' -- <file>`
   Newer commits near violation timestamp are weighted higher.

7. **Compute confidence score**  
   Base score decreases with days from violation detection and lineage distance.

8. **Write structured violation record**  
   Output includes ranked `blame_chain[]`, then `blast_radius` from contract lineage downstream list and `records_failing` from validation report.

This method avoids “guessing by intuition”; it combines topology (lineage) and chronology (git timestamps) into reproducible attribution.

## 4) LangSmith trace contract (structural + statistical + AI-specific)

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-runs
info:
  title: LangSmith Trace Export Contract
  version: 1.0.0
schema:
  id:
    type: string
    format: uuid
    required: true
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
  prompt_tokens:
    type: integer
    minimum: 0
    required: true
  completion_tokens:
    type: integer
    minimum: 0
    required: true
  total_tokens:
    type: integer
    minimum: 0
    required: true
  total_cost:
    type: number
    minimum: 0.0
    required: true
quality:
  type: SodaChecks
  specification:
    checks for traces:
      - missing_count(id) = 0
      - invalid_count(run_type not in ['llm','chain','tool','retriever','embedding']) = 0
      - invalid_count(end_time <= start_time) = 0
      - invalid_count(total_tokens != prompt_tokens + completion_tokens) = 0
ai_extensions:
  llm_output_schema_violation_rate:
    type: metric
    warn_threshold: 0.02
    fail_threshold: 0.05
  embedding_drift:
    type: cosine_distance
    warn_threshold: 0.10
    fail_threshold: 0.15
```

Structural: required fields + enum + types.  
Statistical: token/cost and threshold checks over numeric distributions.  
AI-specific: drift and schema-violation-rate operational metrics.

## 5) Why contracts get stale in production and how this architecture prevents it

Most contract systems fail from **organizational drift**, not parser bugs. Teams update producers fast, but contracts live as static docs or stale YAML that nobody reruns. Common failure patterns:

1. Contracts are manually authored once, then never regenerated from real data.
2. Validation runs are optional (best-effort) and not in CI/pipeline gates.
3. Violations are detected but not routed to ownership (no attribution path).
4. Schema change discussions happen in chat, not in machine-readable snapshots.
5. AI-specific failure signals (embedding drift, output schema degradation) are not connected to core data quality workflows.

This implementation reduces staleness with four controls:

- **Automatic generation from observed data** (`contracts/generator.py`) so contracts reflect reality each run.
- **Snapshot discipline** (`schema_snapshots/<contract_id>/<timestamp>.yaml`) enabling temporal diff and drift/evolution analysis.
- **Executable runner output** (`validation_reports/*.json`) with explicit PASS/FAIL/WARN/ERROR and machine-parseable results.
- **Lineage-aware blast radius hooks** embedded in contracts, so when violation occurs, downstream impact is computable and ownership is actionable.

In practice, the anti-staleness mechanism is repetition and visibility: every run updates evidence artifacts, and each artifact is tied to a command evaluators can reproduce.

## Interim readiness notes

- Week 3 and Week 5 source datasets currently include >= 50 records each in `outputs/week3/extractions.jsonl` and `outputs/week5/events.jsonl`.
- Contracts are auto-generated and include range/type/required clauses.
- First validation run report is generated from real local data (not fabricated).
- Next step for Sunday scope is to add `attributor.py`, `schema_analyzer.py`, AI extensions, and report generator.
