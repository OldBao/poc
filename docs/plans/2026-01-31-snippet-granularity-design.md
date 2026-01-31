# 3-Layer Snippet Architecture Design

Date: 2026-01-31
Status: Design

## Problem

Current snippets are full SQL query blobs (25-36 lines each). When the LLM composes derived metrics (take rate = ads_rev / gmv), it must mentally extract sub-patterns from these blobs and recombine them into CTEs. This is error-prone and doesn't scale as metrics grow.

Additionally, the same metric can exist in multiple source tables (dws vs dwd) with slightly different numbers. The system needs to handle source selection for consistency.

## Design: 3-Layer Snippet Architecture

### Layer 1 — Source Fragments (many, grows with metrics)

Raw daily-grain queries from a specific table. One file per metric-source combination. No period rollup, no composition — just SELECT, FROM, WHERE, GROUP BY at the (region, date) grain.

```
snippets/layer1/dau.sql
snippets/layer1/gmv__mp_paidads.sql
snippets/layer1/gmv__mp_order.sql
snippets/layer1/ads_gross_rev.sql
snippets/layer1/net_ads_rev.sql
snippets/layer1/ads_rev_by_channel.sql
snippets/layer1/order_by_channel.sql
snippets/layer1/buyer_uv.sql
snippets/layer1/commission_fee.sql
snippets/layer1/rebate.sql
```

Example `layer1/ads_gross_rev.sql`:
```sql
-- Source: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
-- Grain: grass_region, grass_date
SELECT
    grass_region
    , grass_date
    , sum(ads_rev_usd) AS ads_rev_usd
    , sum(ads_gmv_usd) AS ads_gmv_usd
FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'regional'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
```

Comment header declares the grain so Layer 2 knows what to wrap.

#### Multi-source metrics

When a metric can be computed from multiple tables, each source gets its own Layer 1 file with a `__tablename` suffix. The YAML declares which is golden.

Naming: `{metric_slug}__{table_short_name}.sql`

Example:
```
snippets/layer1/gmv__mp_paidads.sql    # from ads table
snippets/layer1/gmv__mp_order.sql      # from order table (golden)
```

#### Source selection rules

1. When composing a derived metric, prefer sources from the same table as the other sub-metrics (consistency)
2. For standalone queries, use the golden source
3. If no golden marked, use the first source

### Layer 2 — Aggregation Templates (few, reusable)

Patterns for rolling up daily-grain data into periods. These are generic — they wrap any Layer 1 fragment.

```
snippets/layer2/avg_rollup.sql
snippets/layer2/sum_rollup.sql
snippets/layer2/count_distinct_rollup.sql
```

Example `layer2/avg_rollup.sql`:
```sql
-- Aggregation: monthly average rollup
-- Wraps a Layer 1 fragment that outputs (grass_region, grass_date, value_columns)
SELECT
    substr(cast(grass_date as varchar), 1, 7) AS period
    , grass_region AS market
    , avg({{ value_expr }}) AS {{ value_alias }}
FROM (
    {{ inner_query }}
) _inner
GROUP BY 1, 2
ORDER BY 1 DESC
```

Each metric YAML declares a default aggregation template. The LLM uses the default for standard queries but can adapt for ad-hoc requests (e.g., "daily trend" → skip the monthly rollup).

### Layer 3 — Composition Templates (few, reusable)

Patterns for combining multiple metrics via CTEs. These are metric-agnostic.

```
snippets/layer3/ratio.sql
snippets/layer3/comparison_mom.sql
snippets/layer3/multi_metric.sql
```

Example `layer3/ratio.sql`:
```sql
-- Composition: ratio of two metrics
WITH {{ numerator_alias }} AS (
    {{ numerator_query }}
),
{{ denominator_alias }} AS (
    {{ denominator_query }}
)
SELECT
    a.period
    , a.market
    , a.{{ numerator_value }} AS {{ numerator_alias }}
    , b.{{ denominator_value }} AS {{ denominator_alias }}
    , a.{{ numerator_value }} / NULLIF(b.{{ denominator_value }}, 0) AS {{ ratio_alias }}
FROM {{ numerator_alias }} a
JOIN {{ denominator_alias }} b ON a.period = b.period AND a.market = b.market
```

Example `layer3/comparison_mom.sql`:
```sql
-- Composition: month-over-month comparison
WITH current_period AS (
    {{ current_query }}
),
previous_period AS (
    {{ previous_query }}
)
SELECT
    c.period AS current_period
    , p.period AS previous_period
    , c.market
    , c.{{ value_col }} AS current_value
    , p.{{ value_col }} AS previous_value
    , (c.{{ value_col }} - p.{{ value_col }}) / NULLIF(p.{{ value_col }}, 0) AS change_rate
FROM current_period c
LEFT JOIN previous_period p ON c.market = p.market
```

**For derived metrics:** Composition is explicitly declared in YAML (template, operands, roles).
**For ad-hoc questions:** LLM uses the templates as reference examples and composes freely (e.g., "compare DAU and GMV side by side").

## YAML Metric Definition Changes

### Simple metric (before → after)

Before:
```yaml
metric:
  name: DAU
  type: simple
  aggregation: avg
  sources:
    - table: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      columns: { value: a1, date: grass_date, region: grass_region }
      filters: ["tz_type = 'local'"]
```

After:
```yaml
metric:
  name: DAU
  type: simple
  aggregation_template: avg_rollup
  sources:
    - id: platform_dau
      table: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      golden: true
      snippet: snippets/layer1/dau.sql
      columns: { value: a1, date: grass_date, region: grass_region }
      filters: ["tz_type = 'local'"]
```

### Multi-source metric

```yaml
metric:
  name: GMV
  type: simple
  aggregation_template: avg_rollup
  sources:
    - id: gmv_order
      table: mp_order.dwd_order_item_all_ent_df__reg_s0_live
      golden: true
      snippet: snippets/layer1/gmv__mp_order.sql
    - id: gmv_paidads
      table: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
      golden: false
      snippet: snippets/layer1/gmv__mp_paidads.sql
```

### Derived metric with explicit composition

```yaml
metric:
  name: Gross Take Rate
  type: derived
  formula: "ads_gross_rev / gmv"
  depends_on: ["Ads Gross Rev", "GMV"]
  composition:
    template: ratio
    numerator: Ads Gross Rev
    denominator: GMV
```

### Complex derived metric

```yaml
metric:
  name: Net Revenue After Costs
  type: derived
  formula: "(ads_gross_rev - rebate - commission) / gmv"
  depends_on: ["Ads Gross Rev", "Rebate", "Commission Fee", "GMV"]
  composition:
    template: ratio
    numerator:
      operation: subtract
      operands: ["Ads Gross Rev", "Rebate", "Commission Fee"]
    denominator: GMV
```

## How the LLM Uses This

The system prompt includes:
1. All Layer 1 fragments (metric-specific SQL knowledge)
2. All Layer 2 templates (aggregation patterns)
3. All Layer 3 templates (composition patterns)
4. Metric YAML definitions with source selection and composition declarations

For a query like "ID gross take rate in Nov 2025":
1. Resolve: derived metric → composition template `ratio`, numerator = Ads Gross Rev, denominator = GMV
2. Source selection: both sub-metrics have sources from `mp_paidads` → pick those for consistency
3. Wrap each Layer 1 fragment with its Layer 2 aggregation template (avg_rollup)
4. Compose using Layer 3 ratio template with the two wrapped queries as CTEs

## Directory Structure

```
snippets/
├── layer1/                         # Source fragments (grows with metrics)
│   ├── dau.sql
│   ├── gmv__mp_paidads.sql
│   ├── gmv__mp_order.sql
│   ├── ads_gross_rev.sql
│   ├── net_ads_rev.sql
│   ├── ads_rev_by_channel.sql
│   ├── order_by_channel.sql
│   ├── buyer_uv.sql
│   ├── commission_fee.sql
│   └── rebate.sql
├── layer2/                         # Aggregation templates (few)
│   ├── avg_rollup.sql
│   ├── sum_rollup.sql
│   └── count_distinct_rollup.sql
├── layer3/                         # Composition templates (few)
│   ├── ratio.sql
│   ├── comparison_mom.sql
│   └── multi_metric.sql
└── adjustments/                    # Market-specific adjustments (unchanged)
    └── br_scs_credit.sql
```

Old flat snippets (`snippets/ads_gross_rev.sql`, etc.) are deleted and replaced by the layered structure.

## Migration

Full replacement — decompose the existing 4 snippets into Layer 1 fragments, create the Layer 2 and Layer 3 templates, update all YAML metric definitions. This is a prototype; clean breaks are fine.

## Impact on Other Components

- `prompt_builder.py` — Must walk `layer1/`, `layer2/`, `layer3/` subdirectories separately and label them in the system prompt
- `validator.py` — Table extraction from snippets must handle the new paths
- `agent.py` — No changes (still uses the system prompt)
- `test_cases.yaml` / `benchmark.yaml` — Expected SQL stays the same; the LLM output shouldn't change, only how it gets there
