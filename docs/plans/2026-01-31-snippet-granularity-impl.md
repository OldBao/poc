# 3-Layer Snippet Architecture Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Decompose monolithic SQL snippets into a 3-layer composable architecture (source fragments → aggregation templates → composition templates), update YAML metric definitions with source selection metadata, and adapt prompt_builder + validator to the new structure.

**Architecture:** Layer 1 = metric-specific daily-grain SQL fragments. Layer 2 = reusable aggregation rollup templates. Layer 3 = reusable composition templates (ratio, MoM, multi-metric). YAML declares `aggregation_template`, `golden` source, `snippet` path, and `composition` for derived metrics.

**Tech Stack:** Python, PyYAML, sqlglot, existing `src/` modules (PromptBuilder, SQLValidator, MetricRegistry)

**Design doc:** [docs/plans/2026-01-31-snippet-granularity-design.md](2026-01-31-snippet-granularity-design.md)

---

### Task 1: Create Layer 1 Source Fragments

**Files:**
- Create: `snippets/layer1/ads_gross_rev.sql`
- Create: `snippets/layer1/gmv__mp_paidads.sql`
- Create: `snippets/layer1/gmv__mp_order.sql` (placeholder — no current order-table snippet exists)
- Create: `snippets/layer1/net_ads_rev.sql`
- Create: `snippets/layer1/dau.sql`
- Create: `snippets/layer1/ads_rev_by_channel.sql`
- Create: `snippets/layer1/order_by_channel.sql`
- Create: `snippets/layer1/buyer_uv.sql`
- Create: `snippets/layer1/commission_fee.sql`
- Create: `snippets/layer1/rebate.sql`

**Step 1: Create directory structure**

```bash
mkdir -p snippets/layer1
```

**Step 2: Decompose existing snippets into daily-grain Layer 1 fragments**

Extract the inner subquery from each existing snippet (the daily-grain `SELECT ... GROUP BY region, date` part). Strip the outer `avg()` / `substr()` rollup — that moves to Layer 2.

Example: `snippets/ads_gross_rev.sql` currently has:
```sql
SELECT grass_region, substr(...) AS period, avg(ads_rev_usd) AS ads_gross_rev
FROM (
    SELECT grass_region, grass_date, sum(ads_rev_usd) AS ads_rev_usd, ...
    FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
    WHERE ... GROUP BY 1, 2
) n1
GROUP BY 1, 2
```

Layer 1 keeps only the inner query:
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

**Step 3: Create Layer 1 fragments for simple metrics (DAU, buyer_uv, etc.)**

These metrics don't have existing snippet files — create Layer 1 fragments from their YAML `sources` definitions.

Example `layer1/dau.sql`:
```sql
-- Source: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
-- Grain: grass_region, grass_date
SELECT
    grass_region
    , grass_date
    , a1
FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'local'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
```

**Step 4: Verify all Layer 1 fragments have the standard comment header**

Every file must have `-- Source:` and `-- Grain:` headers so Layer 2 knows what to wrap.

**Step 5: Commit**

```bash
git add snippets/layer1/
git commit -m "feat(snippets): add Layer 1 source fragments"
```

---

### Task 2: Create Layer 2 Aggregation Templates

**Files:**
- Create: `snippets/layer2/avg_rollup.sql`
- Create: `snippets/layer2/sum_rollup.sql`
- Create: `snippets/layer2/count_distinct_rollup.sql`

**Step 1: Create directory**

```bash
mkdir -p snippets/layer2
```

**Step 2: Write avg_rollup.sql**

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

**Step 3: Write sum_rollup.sql**

```sql
-- Aggregation: monthly sum rollup
SELECT
    substr(cast(grass_date as varchar), 1, 7) AS period
    , grass_region AS market
    , sum({{ value_expr }}) AS {{ value_alias }}
FROM (
    {{ inner_query }}
) _inner
GROUP BY 1, 2
ORDER BY 1 DESC
```

**Step 4: Write count_distinct_rollup.sql**

```sql
-- Aggregation: monthly count distinct rollup
SELECT
    substr(cast(grass_date as varchar), 1, 7) AS period
    , grass_region AS market
    , count(distinct {{ value_expr }}) AS {{ value_alias }}
FROM (
    {{ inner_query }}
) _inner
GROUP BY 1, 2
ORDER BY 1 DESC
```

**Step 5: Commit**

```bash
git add snippets/layer2/
git commit -m "feat(snippets): add Layer 2 aggregation templates"
```

---

### Task 3: Create Layer 3 Composition Templates

**Files:**
- Create: `snippets/layer3/ratio.sql`
- Create: `snippets/layer3/comparison_mom.sql`
- Create: `snippets/layer3/multi_metric.sql`

**Step 1: Create directory**

```bash
mkdir -p snippets/layer3
```

**Step 2: Write ratio.sql**

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

**Step 3: Write comparison_mom.sql**

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

**Step 4: Write multi_metric.sql**

```sql
-- Composition: side-by-side comparison of multiple metrics
WITH {{ metric_a_alias }} AS (
    {{ metric_a_query }}
),
{{ metric_b_alias }} AS (
    {{ metric_b_query }}
)
SELECT
    COALESCE(a.period, b.period) AS period
    , COALESCE(a.market, b.market) AS market
    , a.{{ metric_a_value }} AS {{ metric_a_alias }}
    , b.{{ metric_b_value }} AS {{ metric_b_alias }}
FROM {{ metric_a_alias }} a
FULL OUTER JOIN {{ metric_b_alias }} b ON a.period = b.period AND a.market = b.market
ORDER BY 1 DESC
```

**Step 5: Commit**

```bash
git add snippets/layer3/
git commit -m "feat(snippets): add Layer 3 composition templates"
```

---

### Task 4: Update YAML Metric Definitions

**Files:**
- Edit: all 13 files in `metrics/`

**Step 1: Update simple metrics with `aggregation_template` and `snippet` per source**

For each simple metric (DAU, GMV, Buyer UV, Commission Fee, Rebate):
- Add `aggregation_template: avg_rollup` (or `sum_rollup` where appropriate)
- Add `snippet: snippets/layer1/<metric>.sql` to each source
- Add `golden: true` to the primary source

Example — `metrics/dau.yaml` after:
```yaml
metric:
  name: DAU
  aliases: ["daily active users", "platform DAU", "日活"]
  tags: [volume]
  type: simple
  aggregation_template: avg_rollup
  unit: count
  sources:
    - id: platform_dau
      layer: dws
      table: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      golden: true
      snippet: snippets/layer1/dau.sql
      columns:
        value: a1
        date: grass_date
        region: grass_region
      filters:
        - "tz_type = 'local'"
      use_when:
        granularity: [platform]
  dimensions:
    required: [market, date_range]
    optional: [module]
  owner: renliang.dang
```

**Step 2: Update multi-source metrics (GMV)**

GMV has two sources — add `golden: true` to the primary and `snippet` paths with `__tablename` suffixes:
```yaml
sources:
  - id: gmv_from_ads
    table: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
    golden: false
    snippet: snippets/layer1/gmv__mp_paidads.sql
    ...
  - id: gmv_from_order
    table: mp_order.dwd_order_item_all_ent_df__reg_s0_live
    golden: true
    snippet: snippets/layer1/gmv__mp_order.sql
    ...
```

**Step 3: Update complex metrics (Ads Gross Rev, Net Ads Rev)**

Convert from `snippet_file` → structured `sources` with `snippet`:
```yaml
metric:
  name: Ads Gross Rev
  type: simple          # was "complex", now simple with Layer 1 + Layer 2
  aggregation_template: avg_rollup
  sources:
    - id: ads_gross_rev_paidads
      table: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
      golden: true
      snippet: snippets/layer1/ads_gross_rev.sql
      columns:
        value: ads_rev_usd
        date: grass_date
        region: grass_region
      filters:
        - "tz_type = 'regional'"
  ...
```

Remove the old `snippet_file` field.

**Step 4: Update derived metrics with `composition`**

For Gross Take Rate, Net Take Rate, Ads ROI, and Net Ads Rev Atomic:
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
  ...
```

**Step 5: Update channel breakdown metrics**

`ads_rev_pct_by_channel.yaml` and `order_pct_by_channel.yaml` — add `snippet` paths pointing to their Layer 1 fragments.

**Step 6: Run metric loading to verify YAML is valid**

```bash
python -c "from src.registry import MetricRegistry; r = MetricRegistry(); r.load(); print(f'{len(r.metrics)} metrics loaded')"
```

**Step 7: Commit**

```bash
git add metrics/
git commit -m "feat(metrics): add snippet paths, aggregation templates, composition declarations"
```

---

### Task 5: Update MetricRegistry / Models for New Fields

**Files:**
- Edit: `src/models.py` — add `golden`, `snippet`, `aggregation_template`, `composition` fields
- Edit: `src/registry.py` — parse new fields
- Edit: `tests/test_registry.py` — add tests for new fields

**Step 1: Write failing tests**

```python
# Add to tests/test_registry.py

def test_source_has_golden_and_snippet(tmp_path):
    yaml_content = """
metric:
  name: Test
  type: simple
  aggregation_template: avg_rollup
  sources:
    - id: test_src
      table: db.table
      golden: true
      snippet: snippets/layer1/test.sql
      columns: {value: v, date: d, region: r}
  dimensions:
    required: [market]
    optional: []
"""
    (tmp_path / "test.yaml").write_text(yaml_content)
    registry = MetricRegistry(metrics_dir=str(tmp_path))
    registry.load()
    assert registry.metrics[0].sources[0].golden is True
    assert registry.metrics[0].sources[0].snippet == "snippets/layer1/test.sql"
    assert registry.metrics[0].aggregation_template == "avg_rollup"


def test_derived_metric_has_composition(tmp_path):
    yaml_content = """
metric:
  name: Ratio
  type: derived
  formula: "a / b"
  depends_on: ["A", "B"]
  composition:
    template: ratio
    numerator: A
    denominator: B
  dimensions:
    required: [market]
    optional: []
"""
    (tmp_path / "ratio.yaml").write_text(yaml_content)
    registry = MetricRegistry(metrics_dir=str(tmp_path))
    registry.load()
    assert registry.metrics[0].composition["template"] == "ratio"
```

**Step 2: Update `src/models.py`**

Add fields to `MetricSource`:
- `golden: bool = False`
- `snippet: str | None = None`

Add fields to `Metric`:
- `aggregation_template: str | None = None`
- `composition: dict | None = None`

**Step 3: Update `src/registry.py`**

Parse the new fields from YAML.

**Step 4: Run tests**

```bash
pytest tests/test_registry.py -v
```

**Step 5: Commit**

```bash
git add src/models.py src/registry.py tests/test_registry.py
git commit -m "feat(models): add golden, snippet, aggregation_template, composition fields"
```

---

### Task 6: Update PromptBuilder for Layered Snippets

**Files:**
- Edit: `src/prompt_builder.py`
- Create: `tests/test_prompt_builder_layers.py`

**Step 1: Write failing tests**

```python
# tests/test_prompt_builder_layers.py
import os
import pytest
from src.prompt_builder import PromptBuilder


def test_build_includes_layer_sections(tmp_path):
    """Prompt includes separate sections for Layer 1, Layer 2, Layer 3."""
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    snippets_dir = tmp_path / "snippets"
    (snippets_dir / "layer1").mkdir(parents=True)
    (snippets_dir / "layer2").mkdir(parents=True)
    (snippets_dir / "layer3").mkdir(parents=True)

    (snippets_dir / "layer1" / "dau.sql").write_text("SELECT a1 FROM t")
    (snippets_dir / "layer2" / "avg_rollup.sql").write_text("SELECT avg({{ value_expr }})")
    (snippets_dir / "layer3" / "ratio.sql").write_text("WITH a AS (...)")

    builder = PromptBuilder(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        rules_dir=str(tmp_path / "rules"),
    )
    prompt = builder.build()

    assert "## Source Fragments (Layer 1)" in prompt
    assert "## Aggregation Templates (Layer 2)" in prompt
    assert "## Composition Templates (Layer 3)" in prompt
    assert "SELECT a1 FROM t" in prompt
    assert "avg({{ value_expr }})" in prompt


def test_build_falls_back_to_flat_snippets(tmp_path):
    """If no layer subdirs exist, falls back to flat snippet listing."""
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    snippets_dir = tmp_path / "snippets"
    snippets_dir.mkdir()
    (snippets_dir / "test.sql").write_text("SELECT 1")

    builder = PromptBuilder(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        rules_dir=str(tmp_path / "rules"),
    )
    prompt = builder.build()

    assert "Reference SQL Examples" in prompt
    assert "SELECT 1" in prompt
```

**Step 2: Update `_build_snippets_section()` in prompt_builder.py**

Change from flat `os.walk` to layer-aware logic:
1. Check if `layer1/`, `layer2/`, `layer3/` subdirectories exist
2. If yes, build separate labeled sections for each layer
3. If no, fall back to the current flat listing (backwards compatibility)

```python
def _build_snippets_section(self) -> str:
    if not os.path.isdir(self.snippets_dir):
        return ""

    layer1_dir = os.path.join(self.snippets_dir, "layer1")
    layer2_dir = os.path.join(self.snippets_dir, "layer2")
    layer3_dir = os.path.join(self.snippets_dir, "layer3")

    # If layered structure exists, use it
    if os.path.isdir(layer1_dir):
        sections = []
        sections.append(self._build_layer_section(
            layer1_dir, "Source Fragments (Layer 1)",
            "Daily-grain queries from specific tables. Use these as building blocks."
        ))
        if os.path.isdir(layer2_dir):
            sections.append(self._build_layer_section(
                layer2_dir, "Aggregation Templates (Layer 2)",
                "Patterns for rolling up daily-grain data into periods. Wrap Layer 1 fragments."
            ))
        if os.path.isdir(layer3_dir):
            sections.append(self._build_layer_section(
                layer3_dir, "Composition Templates (Layer 3)",
                "Patterns for combining multiple metrics via CTEs."
            ))
        # Also include adjustments if present
        adj_dir = os.path.join(self.snippets_dir, "adjustments")
        if os.path.isdir(adj_dir):
            sections.append(self._build_layer_section(
                adj_dir, "Adjustment Snippets",
                "Market-specific adjustments to apply when conditions match."
            ))
        return "\n\n".join(s for s in sections if s)

    # Fallback: flat snippet listing (backwards compat)
    return self._build_flat_snippets()
```

**Step 3: Add `_build_layer_section()` and `_build_flat_snippets()` helper methods**

**Step 4: Update `_format_metric()` to show `aggregation_template` and `composition`**

When a metric has `aggregation_template`, display it. When it has `composition`, display the template and operands.

**Step 5: Run tests**

```bash
pytest tests/test_prompt_builder_layers.py -v
pytest tests/test_prompt_builder.py -v  # existing tests still pass
```

**Step 6: Commit**

```bash
git add src/prompt_builder.py tests/test_prompt_builder_layers.py
git commit -m "feat(prompt_builder): support 3-layer snippet structure in system prompt"
```

---

### Task 7: Update Validator for New Snippet Paths

**Files:**
- Edit: `src/validator.py`
- Edit: `tests/test_validator.py`

**Step 1: Write failing test**

```python
# Add to tests/test_validator.py

def test_validator_extracts_tables_from_layered_snippets(tmp_path):
    """Validator finds tables from snippets/layer1/*.sql files."""
    # Create a metric with snippet in layer1/
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    (metrics_dir / "test.yaml").write_text("""
metric:
  name: Test
  type: simple
  sources:
    - id: src1
      table: db.known_table
      snippet: snippets/layer1/test.sql
      columns: {value: v, date: d, region: r}
  dimensions:
    required: []
    optional: []
""")

    snippets_dir = tmp_path / "snippets" / "layer1"
    snippets_dir.mkdir(parents=True)
    (snippets_dir / "test.sql").write_text(
        "SELECT v FROM db.snippet_table WHERE d BETWEEN date '2025-01-01' AND date '2025-01-31'"
    )

    registry = MetricRegistry(metrics_dir=str(metrics_dir))
    registry.load()
    value_index = MagicMock()
    value_index.get_all_values_for_column.return_value = set()

    validator = SQLValidator(registry=registry, value_index=value_index)

    # Table from YAML source
    assert "db.known_table" in validator._known_tables
    # Table from snippet file
    assert "db.snippet_table" in validator._known_tables
```

**Step 2: Update `_build_table_set()` in validator.py**

Currently reads `m.snippet_file`. Update to also read `source.snippet` for each source:

```python
def _build_table_set(self) -> set[str]:
    tables = set()
    for m in self.registry.metrics:
        for s in m.sources:
            tables.add(s.table.lower())
            # Extract tables from source-level snippet files
            if s.snippet:
                tables.update(self._tables_from_snippet(s.snippet))
        # Legacy: also check metric-level snippet_file
        if m.snippet_file:
            tables.update(self._tables_from_snippet(m.snippet_file))
    return tables
```

**Step 3: Run tests**

```bash
pytest tests/test_validator.py -v
```

**Step 4: Commit**

```bash
git add src/validator.py tests/test_validator.py
git commit -m "feat(validator): extract tables from source-level snippet paths"
```

---

### Task 8: Delete Old Flat Snippets & Run Regression Tests

**Files:**
- Delete: `snippets/ads_gross_rev.sql`
- Delete: `snippets/net_ads_rev.sql`
- Delete: `snippets/ads_rev_by_channel.sql`
- Delete: `snippets/order_by_channel.sql`
- Keep: `snippets/adjustments/` (unchanged)

**Step 1: Delete old flat snippets**

```bash
rm snippets/ads_gross_rev.sql snippets/net_ads_rev.sql snippets/ads_rev_by_channel.sql snippets/order_by_channel.sql
```

**Step 2: Remove `snippet_file` from YAMLs that still reference old paths**

Check all metric YAMLs — remove any `snippet_file: snippets/xxx.sql` fields that point to deleted files. These should already be replaced by source-level `snippet:` in Task 4, but verify.

**Step 3: Run unit tests**

```bash
pytest tests/ --ignore=tests/test_regression.py -v
```

All tests should pass.

**Step 4: Run regression tests (if LLM available)**

```bash
pytest tests/test_regression.py -v
```

The LLM-generated SQL should remain the same quality — the snippets are the same content, just reorganized.

**Step 5: Print the system prompt and review**

```bash
python -c "
from src.prompt_builder import PromptBuilder
pb = PromptBuilder()
prompt = pb.build()
print(prompt[:3000])
print('...')
print(f'Total prompt length: {len(prompt)} chars')
"
```

Verify:
- Layer 1, 2, 3 sections are present and labeled
- Metrics show `aggregation_template` and `composition` where applicable
- No references to deleted snippet files remain

**Step 6: Commit**

```bash
git add -A
git commit -m "refactor(snippets): complete migration to 3-layer architecture, delete old flat snippets"
```

---

## Summary

| Task | What | Tests |
|------|------|-------|
| 1 | Layer 1 source fragments (10 files) | Manual review |
| 2 | Layer 2 aggregation templates (3 files) | Manual review |
| 3 | Layer 3 composition templates (3 files) | Manual review |
| 4 | Update 13 metric YAMLs | YAML load check |
| 5 | Models + Registry new fields | 2+ tests |
| 6 | PromptBuilder layered sections | 2+ tests |
| 7 | Validator new snippet paths | 1+ tests |
| 8 | Delete old snippets + regression | Full suite |
