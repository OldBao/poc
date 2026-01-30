# LLM-Based SQL Generation for S&R&A Metrics — Design Doc

**Date**: 2026-01-30
**Author**: Andy Zhang
**Status**: POC Design

## Goal

Build a system that takes natural language questions about S&R&A metrics and generates accurate SQL using an LLM (OpenAI GPT-4o). The LLM handles both understanding the request and generating SQL, guided by a structured knowledge base of metric definitions and reference SQL examples.

This replaces the previous deterministic pipeline (Jinja2 templates + pre-built snippets) with a single LLM call, using the metric definitions as prompt context rather than code that runs.

## Architecture

```
User Question (single turn)
        |
        v
+-------------------+
|   OpenAI GPT-4o   |  <-- System prompt:
|                   |      - All metric YAML defs
|   Single request  |      - Reference SQL snippets
|   Single response |      - Output format rules
+--------+----------+
         |
    JSON response
         |
    +----+----+
    |         |
    v         v
  SQL     Ambiguity
output    candidates
```

Three layers:

1. **Knowledge Base (static context)** — All YAML metric definitions + per-metric reference SQL snippets, assembled into a structured system prompt. This is the ground truth the LLM works from.

2. **LLM Agent (OpenAI GPT-4o)** — Single-turn. Receives the full KB in the system prompt plus the user's question. Returns either SQL or candidate interpretations if the request is ambiguous.

3. **Regression Test Suite** — Known question -> expected SQL fragment pairs. The accuracy gate.

The LLM is responsible for both intent understanding and SQL generation. No deterministic SQL assembly pipeline.

## Knowledge Base & Prompt Design

The system prompt is assembled automatically at startup from project files. Three sections:

### A. Metric Definitions

All ~15 YAML metrics serialized into the prompt. Each metric includes:
- Name and aliases
- Type (simple / complex)
- Formula and aggregation
- Source tables, column mappings, filters
- Dimensions (required / optional)
- Business rule notes

Example YAML (already exists in `metrics/`):

```yaml
metric:
  name: DAU
  aliases: ["daily active users", "platform DAU"]
  type: simple
  formula: "avg(dau)"
  sources:
    - id: platform_dau
      layer: dws
      table: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      columns:
        value: a1
        date: grass_date
        region: grass_region
      filters: ["tz_type = 'local'"]
      use_when:
        granularity: [platform]
  dimensions:
    required: [market, date_range]
    optional: [module]
```

### B. Reference SQL Snippets

5-10 known-good SQL snippets covering the main query patterns:
- Simple metric query (e.g., DAU for one market/month)
- Derived metric (e.g., take rate = ads_rev / gmv)
- Complex multi-table query (e.g., ads rev by channel)
- Comparison query (MoM with change rate calculation)
- Multi-metric query

Each snippet is 10-30 lines, isolated per metric. These are the few-shot examples that anchor the LLM's SQL output style and correctness.

### C. Output Format Instructions

The LLM must return JSON:

```json
{
  "type": "sql",
  "sql": "SELECT ..."
}
```

Or if ambiguous:

```json
{
  "type": "ambiguous",
  "candidates": [
    "Gross Ads Revenue (total ads revenue before deductions)",
    "Net Ads Revenue (after SCS credits and rebates)"
  ]
}
```

### Prompt Size

~15 metrics + 5-10 SQL snippets = roughly 5-8K tokens. Well within GPT-4o context window.

The prompt is assembled by reading YAML files and snippet files from disk. No manual prompt maintenance — add a YAML metric file and a snippet, and the prompt updates automatically.

## Analyzer & Importer Pipeline

The importer converts raw materials into structured KB entries. It runs offline, not at query time.

### Inputs

- **SQL files** — ETL queries like `monthly_core_metrics_tracker.sql`
- **Docs** — Confluence pages, text notes describing business rules and metric definitions

### Process

```
Raw SQL / Docs
      |
      v
+------------------+
|  Analyzer (LLM)  |  <-- Prompt: YAML schema + examples
|  Parse & extract |
+--------+---------+
         |
         v
  Draft YAML files + SQL snippets
         |
         v
  Human review & commit
         |
         v
  KB updated -> query prompt auto-refreshes
```

1. **Analyzer** — Uses the OpenAI API to parse the raw input. For SQL: identifies metrics, extracts table names, columns, filters, aggregations, join patterns. For docs: extracts metric names, business rules, edge cases, notes.

2. **Generator** — Produces two outputs per metric:
   - Draft YAML metric definition (name, aliases, type, sources, dimensions)
   - Reference SQL snippet (the isolated query fragment for that metric, 10-30 lines)

3. **Review** — Outputs are written to `metrics/` and `snippets/` as drafts. Human reviews, edits, and commits.

The analyzer prompt includes the YAML schema and a couple of example YAML files so the LLM knows the target format.

## Regression Test Suite

The accuracy gate. Lives in `tests/test_cases.yaml`.

### Test Case Format

```yaml
- question: "ID market DAU in November 2025"
  expected_type: sql
  expected_sql_contains:
    - "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live"
    - "grass_region = 'ID'"
    - "BETWEEN"
    - "2025-11-01"
    - "2025-11-30"
    - "avg(a1)"
  expected_sql_not_contains:
    - "dwd_module"

- question: "Compare ID take rate between Oct and Nov 2025"
  expected_type: sql
  expected_sql_contains:
    - "current_period"
    - "previous_period"
    - "change_rate"

- question: "What's the revenue?"
  expected_type: ambiguous
  expected_candidates_contain:
    - "ads"
```

### How Testing Works

- `test_agent.py` loads each case, sends the question to the LLM, checks the response against expected fragments.
- Uses substring matching — not exact SQL comparison. Gives the LLM flexibility on formatting while enforcing correctness on tables, columns, filters, and logic.
- `expected_sql_not_contains` catches common mistakes (wrong source table, missing required filter).
- Ambiguity cases verify the LLM correctly identifies unclear requests instead of guessing.

### When to Run

Before any change to YAML definitions, prompt template, reference SQL snippets, or LLM model version. The suite is the accuracy contract.

## Project Structure

```
sqlpoc/
├── src/
│   ├── agent.py            # CLI entry point: question -> SQL or ambiguity
│   ├── prompt_builder.py   # Assembles system prompt from YAML + snippets
│   ├── llm_client.py       # OpenAI API wrapper (GPT-4o)
│   ├── importer/
│   │   ├── analyzer.py     # Parses raw SQL/docs via LLM
│   │   └── generator.py    # Produces draft YAML + snippet files
│   └── registry.py         # Loads YAML metrics (reused)
├── metrics/                # YAML metric definitions (KB)
├── snippets/               # Per-metric reference SQL snippets
├── tests/
│   ├── test_cases.yaml     # Regression suite
│   └── test_agent.py       # Test runner
├── docs/
│   └── plans/
└── raw/                    # Raw input SQL files and docs for importer
```

### Changes from Previous Design

| Removed | Replaced by |
|---------|-------------|
| `assembler.py` (Jinja2 SQL assembly) | LLM generates SQL directly |
| `extractor.py` (LLM intent extraction) | LLM handles both intent + SQL |
| `templates/` directory | Reference SQL snippets in prompt |

| Added | Purpose |
|-------|---------|
| `prompt_builder.py` | Assembles KB into system prompt |
| `llm_client.py` | OpenAI API wrapper |
| `importer/analyzer.py` | Parses raw SQL/docs into KB format |
| `importer/generator.py` | Produces draft YAML + snippets |
| `raw/` directory | Stores input materials for importer |

## Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| LLM | OpenAI GPT-4o | User preference, strong SQL generation |
| KB format | YAML + SQL snippets | Version control, human readable, CR friendly |
| Prompt assembly | Python (prompt_builder.py) | Auto-generates from files on disk |
| Testing | Substring matching on SQL fragments | Flexible enough for LLM output variation |
| Language | Python 3.10+ | Team familiarity |

## Implementation Phases

### Phase 1: Importer Pipeline
- Build `analyzer.py` and `generator.py`
- Feed `monthly_core_metrics_tracker.sql` and any existing docs
- Review and refine generated YAML + snippets
- Deliverable: complete KB for all ~15 metrics

### Phase 2: Prompt Builder & LLM Client
- Build `prompt_builder.py` to assemble system prompt from YAML + snippets
- Build `llm_client.py` as OpenAI API wrapper
- Test prompt with a few manual questions
- Deliverable: working single-turn question -> SQL pipeline

### Phase 3: Regression Test Suite
- Expand `test_cases.yaml` to cover all metrics and query patterns
- Build `test_agent.py` runner
- Iterate on prompt and KB until all tests pass
- Deliverable: green test suite, accuracy validated

### Phase 4: CLI Agent
- Wire everything together in `agent.py`
- Handle ambiguity response display
- Clean CLI output formatting
- Deliverable: usable CLI tool

## Key Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| LLM generates wrong SQL (wrong table, missing filter) | Regression test suite catches this; `expected_sql_not_contains` for common mistakes |
| LLM output varies between runs | Substring matching tolerates formatting differences; use low temperature |
| Prompt gets too large as metrics grow | ~15 metrics = ~5-8K tokens, plenty of headroom; selective retrieval is a future option |
| Importer produces bad YAML drafts | Human review step before committing to KB |
| OpenAI API cost | Single call per question, ~6-10K input tokens; negligible for internal tool |
| Business rules change but KB not updated | Same risk as any documentation; importer makes updates easier |
