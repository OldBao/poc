# Semantic Layer + Agent for S&R&A Metrics — Design Doc

**Date**: 2026-01-30
**Author**: Andy Zhang
**Status**: POC Design

## Goal

Build a semantic layer and CLI agent that takes natural language questions about S&R&A metrics and generates 100%-accurate SQL. Starting scope: the S&R&A Monthly Metrics Tracker (all markets, ~15 metrics).

The system solves two problems equally:
1. **Self-serve for non-SQL users** — leaders/PMs ask data questions, get SQL without writing it
2. **SQL correctness enforcement** — semantic layer defines canonical metric definitions, preventing wrong joins/filters/口径

## Architecture

```
User Question (natural language)
        |
        v
+-------------------------+
|   Intent & Entity       |  <-- LLM (Claude/GPT API)
|   Extraction            |
|   - intent: query / compare / trend / breakdown
|   - metrics: ["ads_rev", "take_rate"]
|   - dimensions: {market: "ID", date: "2025-11"}
+------------+------------+
             |  structured JSON
             v
+-------------------------+
|   Metric Resolver       |  <-- Semantic Registry (YAML files)
|   - match metric name   |      + keyword/embedding index
|     against aliases     |
|   - select source       |
|   - validate dimensions |
+------------+------------+
             |  metric definition(s)
             v
+-------------------------+
|   SQL Assembly Engine   |  <-- Jinja2 templates + SQL snippets
|   - simple: template    |
|   - complex: snippet    |
|   - inject filters      |
+------------+------------+
             |  SQL string
             v
         Output SQL
```

Four components. The LLM **never touches SQL** — it only parses the user question into structured JSON. All SQL logic lives in YAML definitions + Jinja2 templates + pre-built snippets.

## Semantic Registry (YAML Metric Definitions)

### Two-Tier Design

**Simple metrics** (`type: simple`): Direct aggregation from one source table. SQL generated from Jinja2 template.

**Complex metrics** (`type: complex`): Multi-table joins, CASE WHEN logic, or special business rules. SQL from pre-built, tested snippets with dimension placeholders.

### Multi-Source Support

Each metric can have multiple `sources`, each tagged with a data warehouse layer (`dws`/`dwd`/`ods`) and `use_when` conditions. The Metric Resolver picks the right source based on the user's query context.

### Simple Metric Example

```yaml
# metrics/dau.yaml
metric:
  name: DAU
  aliases: ["daily active users", "platform DAU"]
  type: simple
  formula: "avg(dau)"
  unit: count
  format: "#,##0"
  aggregation: avg

  sources:
    - id: platform_dau
      description: "Platform-level DAU from DWS layer"
      layer: dws
      table: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      columns:
        value: a1
        date: grass_date
        region: grass_region
      filters: ["tz_type = 'local'"]
      use_when:
        granularity: [platform]

    - id: module_dau
      description: "Module-level DAU from DWD layer"
      layer: dwd
      table: traffic.dwd_module_active_user_di__reg_s0_live
      columns:
        value: active_users
        date: grass_date
        region: grass_region
        module: module_name
      filters: ["tz_type = 'local'"]
      use_when:
        granularity: [module]

  dimensions:
    required: [market, date_range]
    optional: [module]
  owner: renliang.dang
  tracker_ref: "S&R&A Monthly Metrics Tracker"
```

### Complex Metric Example

```yaml
# metrics/ads_rev_by_channel.yaml
metric:
  name: Ads Rev by Channel
  aliases: ["ads revenue breakdown", "search ads rev", "DD ads rev", "RCMD ads rev"]
  type: complex
  sub_metrics:
    - search_ads_rev_usd
    - dd_ads_rev_usd
    - rcmd_ads_rev_usd
    - game_ads_rev_usd
    - brand_ads_rev_usd
    - live_ads_rev_usd
    - video_ads_rev_usd
  snippet_file: snippets/ads_rev_by_channel.sql
  dimensions:
    required: [market, date_range]
    optional: []
  notes: |
    - Old data (before 2024-01) uses mkplpaidads_analytics.ads_take_rate_dashboard_v2
      with different entry_point names (e.g., 'Search' vs 'Global Search')
    - New data uses mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
    - BR market has special SCS credit adjustment for net_ads_rev_excl_1p
```

### Full Metric List (POC Scope)

| Metric | Type | Source Count |
|--------|------|-------------|
| DAU | simple | 2 (platform, module) |
| Buyer UV | simple | 1 |
| Buyer UV Rate | simple | 1 (derived: buyer_uv / dau) |
| Orders | simple | 1 |
| Order per User | simple | 1 (derived: orders / dau) |
| GMV | simple | 1 |
| Ads Gross Rev | complex | 2 (old/new source tables) |
| Gross Take Rate | simple | 1 (derived: ads_rev / gmv) |
| Ads Direct ROI | simple | 1 (derived: ads_gmv / ads_rev) |
| Net Ads Rev | complex | 2 (with BR SCS adjustment) |
| Net Take Rate | simple | 1 (derived: net_ads_rev / gmv) |
| Commission Fee | simple | 1 |
| Rebate | simple | 1 |
| Order% by Channel | complex | 1 (8 channels) |
| Ads Rev% by Channel | complex | 1 (8 channels) |

## SQL Assembly Engine

### Simple Metrics — Jinja2 Template

```sql
-- templates/simple_metric.sql.j2
SELECT
    substr(cast({{ source.columns.date }} as varchar), 1, 7) AS period
    {% if group_by_region %}, {{ source.columns.region }} AS market{% endif %}
    {% if group_by_module and source.columns.module %}, {{ source.columns.module }} AS module{% endif %}
    , {{ metric.aggregation }}({{ source.columns.value }}) AS {{ metric.name | lower }}
FROM {{ source.table }}
WHERE {{ source.columns.date }} BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    {% for f in source.filters %} AND {{ f }}{% endfor %}
    {% if market %} AND {{ source.columns.region }} = '{{ market }}'{% endif %}
GROUP BY 1 {% if group_by_region %}, 2{% endif %} {% if group_by_module %}, 3{% endif %}
ORDER BY 1 DESC
```

### Complex Metrics — Pre-built Snippets

Stored in `snippets/` directory. Each snippet contains the full multi-table join logic with Jinja2 placeholders for dimension filters (`{{ date_start }}`, `{{ date_end }}`, `{{ market }}`).

### Comparison Queries — Wrapper Template

```sql
-- templates/compare.sql.j2
WITH current_period AS (
    {{ base_query | indent(4) }}
),
previous_period AS (
    {{ base_query_prev | indent(4) }}
)
SELECT
    c.period AS current_period,
    p.period AS previous_period,
    c.{{ metric_col }} AS current_value,
    p.{{ metric_col }} AS previous_value,
    (c.{{ metric_col }} - p.{{ metric_col }}) / NULLIF(p.{{ metric_col }}, 0) AS change_rate
FROM current_period c
LEFT JOIN previous_period p ON c.market = p.market
```

## LLM Intent & Entity Extraction

The LLM receives:
1. **System prompt** with available metric names + aliases (auto-generated from YAML registry) and dimension values
2. **User question**
3. **Strict JSON output schema**

The LLM sees metric names and aliases only — no SQL, no table names, no column names.

### Output Schema

```json
{
  "intent": "query | compare | trend | breakdown",
  "metrics": ["metric_name"],
  "dimensions": {
    "market": "XX" | null,
    "date_range": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"},
    "compare_to": {"type": "MoM|YoY|custom", "start": "...", "end": "..."} | null,
    "module": "XX" | null
  },
  "clarification_needed": null | "string explaining what's ambiguous"
}
```

If the question is ambiguous (e.g., "revenue" could mean multiple metrics), the LLM sets `clarification_needed` and the agent asks the user to specify.

## Project Structure

```
sqlpoc/
+-- metrics/                    # Semantic registry (YAML)
|   +-- dau.yaml
|   +-- buyer_uv.yaml
|   +-- gmv.yaml
|   +-- ads_gross_rev.yaml
|   +-- take_rate.yaml
|   +-- ads_roi.yaml
|   +-- net_ads_rev.yaml
|   +-- net_take_rate.yaml
|   +-- commission_fee.yaml
|   +-- rebate.yaml
|   +-- order_pct_by_channel.yaml
|   +-- ads_rev_pct_by_channel.yaml
+-- snippets/                   # Pre-built SQL for complex metrics
|   +-- ads_rev_by_channel.sql
|   +-- net_ads_rev.sql
|   +-- order_by_channel.sql
+-- templates/                  # Jinja2 SQL templates
|   +-- simple_metric.sql.j2
|   +-- compare.sql.j2
|   +-- trend.sql.j2
+-- src/
|   +-- registry.py             # Load & index YAML metrics
|   +-- extractor.py            # LLM intent/entity extraction
|   +-- resolver.py             # Match user intent -> metric definitions
|   +-- assembler.py            # SQL assembly engine (Jinja2)
|   +-- validator.py            # Optional SQL validation
|   +-- agent.py                # Main CLI entry point
+-- tests/
|   +-- test_cases.yaml         # Known Q&A pairs for regression
|   +-- test_agent.py
+-- docs/
|   +-- plans/
+-- output.sql                  # Reference output query
+-- monthly_core_metrics_tracker.sql  # Reference ETL
+-- excel.xlsx                  # Reference tracker
```

## Implementation Phases

### Phase 1: Metric Registry
- Write ~12 YAML files for S&R&A tracker metrics, extracting definitions from existing SQL
- Build `registry.py` to load, validate, and index YAML files
- Deliverable: all metrics defined, loadable, with correct source tables and formulas

### Phase 2: SQL Assembly
- Build `assembler.py` with Jinja2 templates for simple metrics
- Extract complex SQL snippets from `monthly_core_metrics_tracker.sql` into `snippets/`
- Build comparison query wrapper
- Write unit tests that verify generated SQL matches expected output for each metric

### Phase 3: LLM Extraction
- Build `extractor.py` with prompt design
- Auto-generate metric list section of the prompt from YAML registry
- Test with 10-15 sample questions covering query, compare, trend intents
- Handle ambiguity detection and clarification flow

### Phase 4: Agent CLI
- Wire all components together in `agent.py`
- Build `test_cases.yaml` with known-good Q&A pairs
- Regression test: input question -> generated SQL must match expected SQL
- CLI interface: user types question, gets formatted SQL back

## Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Semantic registry | YAML + Git | Version control, CR friendly, human readable |
| SQL templates | Jinja2 | Mature, flexible, Python native |
| LLM | Claude API (or any available) | POC stage, API is fine |
| Metric index | Keyword matching (POC) | FAISS overkill for ~15 metrics, add later if needed |
| Language | Python 3.10+ | Team familiarity, rich ecosystem |

## Testing Strategy

For 100% accuracy, every metric + dimension combination has a stored expected SQL output:

```yaml
# tests/test_cases.yaml
- question: "ID market DAU in November 2025"
  expected_intent: query
  expected_metrics: [dau]
  expected_sql_contains:
    - "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live"
    - "grass_region = 'ID'"
    - "BETWEEN date '2025-11-01' AND date '2025-11-30'"
    - "avg(a1)"

- question: "Compare ID take rate between Oct and Nov 2025"
  expected_intent: compare
  expected_metrics: [take_rate]
  expected_sql_contains:
    - "current_period"
    - "previous_period"
    - "change_rate"
```

## Key Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| SQL snippets diverge from real ETL | Store snippets alongside ETL code, include in code review |
| LLM misparses ambiguous questions | `clarification_needed` field forces explicit user disambiguation |
| New metrics added but agent doesn't know | Auto-generate LLM prompt from YAML registry — add YAML = agent knows |
| Complex business rules (BR SCS credits, old vs new entry points) | Encode in snippet notes + use_when conditions |
