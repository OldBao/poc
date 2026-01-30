# Hybrid Pipeline Architecture — Design Doc

**Date**: 2026-01-30
**Author**: Andy Zhang
**Status**: Approved

## Problem

The current prompt-only approach (LLM generates SQL end-to-end) has three accuracy failure modes:

1. **Wrong tables/columns** — LLM hallucinates or picks the wrong source table
2. **Wrong join logic** — complex multi-table queries break
3. **Wrong filter values** — LLM doesn't know exact enum values for dimensions (markets, entry points, module names)

## Solution

Hybrid pipeline with two SQL generation paths:

- **Simple metrics** (~10 of 15): deterministic Jinja2 template assembly. No LLM in the SQL path — 100% accurate by construction.
- **Complex metrics** (~5 of 15): LLM adapts pre-built snippets with rich context (value index, schemas, snippets), validated before output.

A **value index** (SQLite) stores distinct dimension values + counts, queried from AlloyDB on a schedule or on demand.

## Architecture

```
User Question (natural language)
        |
        v
+-------------------------+
|   LLM Intent Extraction  |  <- LLM (GPT/Claude API)
|   - intent: query/compare/trend/breakdown
|   - metrics: ["dau", "take_rate"]
|   - dimensions: {market: "ID", date: "2025-11"}
+------------+------------+
             |  structured JSON
             v
+-------------------------+
|   Metric Resolver        |  <- YAML Registry + Value Index
|   - match metric by name/alias
|   - select source (simple vs complex)
|   - validate dimensions exist
+------------+------------+
             |
     +-------+-------+
     |               |
  simple          complex
     |               |
     v               v
+----------+   +-----------+
| Template |   | LLM SQL   |  <- snippets + value index + schema as context
| Assembly |   | Generation|
| (Jinja2) |   +-----------+
+----------+        |
     |               v
     |         +-----------+
     |         | Validator |  <- check tables, columns, filter values, syntax
     |         +-----------+
     |               |
     |          pass / fail(retry once) / fail(surface error)
     |               |
     +-------+-------+
             |
             v
         Output SQL
```

## Value Index

SQLite database storing distinct values and counts for every dimension column in the YAML registry.

### Schema

```sql
CREATE TABLE dimension_values (
    table_name  TEXT,
    column_name TEXT,
    value       TEXT,
    count       INTEGER,
    updated_at  TIMESTAMP,
    PRIMARY KEY (table_name, column_name, value)
);
```

### Population

A reindex job queries AlloyDB for each dimension column defined in the YAML registry:

```sql
SELECT grass_region AS value, COUNT(*) AS count
FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
GROUP BY grass_region
```

The reindex job auto-discovers which columns to index by scanning all `columns` and `filters` fields in the YAML metric definitions.

### Refresh Modes

- **Scheduled**: nightly cron runs `agent reindex --all`
- **On-demand**: user runs `agent reindex` or `agent reindex --dimension grass_region`

### Usage

- **LLM prompt context**: before LLM generates complex SQL, inject relevant dimension values (e.g., "Valid markets: ID, TH, VN, PH, MY, SG, TW, BR, MX, CL, CO, AR")
- **Validator**: after SQL is generated, check that filter values like `grass_region = 'ID'` exist in the index

## Simple Metric Path (Deterministic)

No LLM involvement in SQL generation:

1. **Metric Resolver** matches the extracted metric name/alias to a YAML definition
2. **Source Selector** picks the right source based on `use_when` conditions (e.g., platform-level vs module-level DAU)
3. **Jinja2 Template Engine** renders the SQL using the metric definition + resolved dimensions

Templates: `simple_metric.sql.j2`, `compare.sql.j2`, `trend.sql.j2` (existing).

The value index validates dimension values before injecting them into the template. If a dimension value isn't in the index (e.g., user says "market XY"), the agent surfaces a helpful error: "Unknown market 'XY'. Valid markets: ID, TH, VN, ..." — no SQL is generated.

**Coverage**: all `type: simple` metrics (~10 of 15 POC metrics), plus derived metrics like `take_rate = ads_rev / gmv`.

## Complex Metric Path (LLM-Assisted)

For `type: complex` metrics (ads rev by channel, net ads rev, order % by channel).

### LLM Prompt Includes

- The pre-built SQL snippet from `snippets/` as a reference example
- Table schemas (column names and types) from the YAML registry
- Relevant dimension values from the value index
- The user's parsed intent and dimensions from the extraction step

The LLM's job: adapt the snippet to the user's specific question (filter to a market, adjust date range, select specific sub-metrics). It is not writing SQL from scratch — it is modifying a known-good snippet.

### Validation Pipeline (3 Checks)

1. **Table/column check** — every table and column in the SQL exists in the YAML registry
2. **Filter value check** — every literal value in WHERE clauses exists in the value index
3. **Syntax check** — parse the SQL (e.g., `sqlglot`) to catch syntax errors

### On Failure

- First failure: feed the validation error back to the LLM, retry once
- Second failure: surface the error to the user with details on what failed

**Coverage**: ~5 of 15 POC metrics. Multi-table joins with business rules (BR SCS adjustments, old vs new source table cutoffs, channel breakdowns).

## Project Structure

```
sqlpoc/
├── metrics/                    # Semantic registry (YAML) — unchanged
├── snippets/                   # Pre-built SQL for complex metrics — unchanged
├── templates/                  # Jinja2 SQL templates — unchanged
├── src/
│   ├── agent.py                # Main CLI orchestrator (rewrite)
│   ├── llm_client.py           # LLM API wrapper (exists)
│   ├── prompt_builder.py       # LLM prompt assembly (update)
│   ├── registry.py             # Load & index YAML metrics (exists)
│   ├── models.py               # Data models (exists)
│   ├── extractor.py            # LLM intent/entity extraction (NEW)
│   ├── resolver.py             # Match intent -> metric + source (NEW)
│   ├── assembler.py            # Jinja2 SQL assembly for simple metrics (NEW)
│   ├── validator.py            # SQL validation against registry + index (NEW)
│   ├── value_index.py          # SQLite value index management (NEW)
│   └── importer/               # Existing importer — unchanged
├── value_index.db              # SQLite value index (generated)
├── tests/
│   ├── test_cases.yaml         # Regression test cases
│   ├── test_agent.py           # End-to-end tests
│   ├── test_extractor.py       # (NEW)
│   ├── test_resolver.py        # (NEW)
│   ├── test_assembler.py       # (NEW)
│   └── test_validator.py       # (NEW)
└── docs/plans/
```

### Key Changes From Current State

- `agent.py` becomes an orchestrator that wires the pipeline, instead of directly calling LLM
- `prompt_builder.py` builds two different prompts: one for intent extraction, one for complex SQL generation
- Five new modules: `extractor`, `resolver`, `assembler`, `validator`, `value_index`

## Implementation Phases

### Phase 1: Value Index

`value_index.py` + `agent reindex` CLI command. SQLite schema, AlloyDB queries, refresh logic.

Testable standalone: run reindex, inspect the DB.

### Phase 2: Extractor

`extractor.py`. LLM intent/entity extraction with strict JSON output. Reuse existing `llm_client.py`.

Testable with existing `test_cases.yaml` — verify parsed intents match expected.

### Phase 3: Resolver

`resolver.py`. Match extracted metrics to YAML definitions, pick source, validate dimensions against value index. Pure logic, no LLM.

Unit testable.

### Phase 4: Assembler + Validator

`assembler.py` for Jinja2 rendering (simple path). `validator.py` for SQL validation (complex path). Update `prompt_builder.py` to inject snippets + value index + schemas.

Testable: generate SQL for every metric, compare against expected output.

### Phase 5: Agent Orchestration

Rewire `agent.py` to use the full pipeline. Add retry logic for complex path. End-to-end regression tests.

Each phase produces a working, testable component before the next begins.
