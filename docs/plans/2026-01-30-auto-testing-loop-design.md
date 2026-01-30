# Auto-Testing Loop Design

## Overview

A self-improving test loop that evaluates the SQL agent against benchmark cases and automatically repairs the knowledge base (metrics YAML, snippets) when generation fails. Runs as a CLI command with structural comparison, result validation via Shopee's internal query service, and an LLM-driven repair loop with retry caps and regression protection.

## Architecture

Three phases per test run:

### Phase 1 — Generate & Compare (structural gate)

- Load benchmark cases from YAML (question + expected SQL)
- Run the agent to generate SQL for each question
- Compare generated vs expected SQL structurally using the LLM
- Extracts and compares: tables, joins, filters, aggregations, group-by/order-by
- LLM returns structured JSON diff: `{"match": true/false, "differences": [...]}`
- LLM-based comparison handles Presto-specific syntax, aliasing, and semantic equivalence
- Pass → Phase 2; Fail → Phase 3 (repair)

### Phase 2 — Execute & Validate (result gate)

- Wrap both SQLs with `LIMIT 100`, submit to Shopee's internal query service
- Compare results:
  - Schema check: column names and types (order-insensitive)
  - Data check: sort both result sets, compare row-by-row
  - Numeric tolerance: 0.01% for floating-point differences
- Edge cases:
  - Generated SQL execution error → automatic failure, error feeds into repair
  - Expected SQL execution error → flag benchmark case as broken, skip
  - Query timeout (configurable, default 60s) → mark inconclusive
- Pass → done; Fail → Phase 3 (repair)

### Phase 3 — Repair Loop

- Send the LLM a repair prompt with:
  - Original question and expected SQL
  - Generated SQL and failure context (structural diff, result mismatch, or execution error)
  - Current relevant metric YAML and snippet files
  - Full list of available metrics/snippets
- LLM responds with a repair plan as structured JSON:
  ```json
  {
    "actions": [
      {"type": "edit_snippet", "file": "snippets/dau.sql", "content": "..."},
      {"type": "create_metric", "file": "metrics/new_metric.yaml", "content": "..."},
      {"type": "edit_metric", "file": "metrics/dau.yaml", "content": "..."}
    ],
    "reasoning": "The snippet was missing the tz_type filter..."
  }
  ```
- Apply file changes, re-run the failing case through Phase 1+2
- After each repair, re-run all previously-passing cases as regression check
- If a repair breaks a passing case, revert and flag for human review

### Guardrails

- Per-case retry cap (default: 3)
- Global LLM call budget (default: 50 across entire run)
- Each repair action is logged for the summary
- Human confirmation required before committing any KB changes

## Benchmark File Format

File: `tests/benchmark.yaml`

```yaml
cases:
  - id: dau_id_monthly
    question: "ID market DAU in November 2025"
    expected_sql: |
      SELECT grass_region, substr(cast(grass_date as varchar), 1, 7) AS period,
             avg(a1) AS dau
      FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30'
        AND tz_type = 'local' AND grass_region = 'ID'
      GROUP BY 1, 2 ORDER BY 2 DESC
    tags: [traffic, dau]

  - id: gross_ads_revenue
    question: "Gross ads revenue for TH in Q4 2025"
    expected_sql: |
      ...
    tags: [ads, revenue]
```

Fields:
- **id** — unique identifier for reporting
- **question** — natural language input to the agent
- **expected_sql** — reference SQL to compare against
- **tags** — optional, for filtering subsets

## CLI Interface

```bash
python -m src.autotest [options]
```

Options:
- `--benchmark FILE` — path to benchmark YAML (default: `tests/benchmark.yaml`)
- `--tags TAG,...` — run only cases with matching tags
- `--id CASE_ID` — run a single case
- `--max-retries N` — per-case retry cap (default: 3)
- `--max-llm-calls N` — global LLM budget (default: 50)
- `--no-repair` — run Phase 1+2 only, skip repair loop (evaluation-only)
- `--dry-run` — show repair plans without applying file changes

## Output

```
=== Auto-Test Results ===
Total: 12 | Passed: 9 | Repaired: 2 | Failed: 1

Repaired:
  ✓ dau_id_monthly (2 retries)
    - edited snippets/dau.sql: added tz_type filter
  ✓ gross_ads_revenue (1 retry)
    - created metrics/gross_ads_revenue.yaml

Failed (needs human review):
  ✗ net_revenue_breakdown (3 retries exhausted)
    - last error: result mismatch, 12 rows differ

KB changes pending confirmation.
Commit these changes? [y/n]
```

## New Files

| File | Purpose |
|------|---------|
| `src/autotest/__init__.py` | Package init |
| `src/autotest/runner.py` | Main loop orchestration (Phase 1→2→3) |
| `src/autotest/comparator.py` | Structural + result comparison |
| `src/autotest/repairer.py` | Repair loop logic |
| `src/query_service.py` | Query execution client via internal API (stub) |
| `tests/benchmark.yaml` | Benchmark test cases |

## Query Service

- Thin client class `QueryService` in `src/query_service.py`
- Methods: `execute(sql, limit=100) -> QueryResult`
- Wraps Shopee's internal query service API
- Initially a stub — endpoint/auth specifics to be filled in
