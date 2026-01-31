# Semantic Layer + Agent Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a CLI agent that takes natural language questions about S&R&A metrics and generates 100%-accurate SQL via a YAML semantic registry + Jinja2 SQL templates.

**Architecture:** LLM extracts intent/entities from user question → Metric Resolver matches against YAML registry → SQL Assembly Engine renders Jinja2 templates or injects filters into pre-built snippets → outputs SQL string. LLM never writes SQL.

**Tech Stack:** Python 3.10+, PyYAML, Jinja2, Anthropic SDK (Claude API), pytest

---

### Task 1: Project Scaffolding

**Files:**
- Create: `src/__init__.py`
- Create: `tests/__init__.py`
- Create: `requirements.txt`
- Create: `metrics/.gitkeep`
- Create: `snippets/.gitkeep`
- Create: `templates/.gitkeep`

**Step 1: Create directory structure and requirements**

```
mkdir -p src tests metrics snippets templates
```

`requirements.txt`:
```
pyyaml>=6.0
jinja2>=3.1
anthropic>=0.40
pytest>=8.0
```

**Step 2: Create empty init files**

```python
# src/__init__.py
# tests/__init__.py
```

**Step 3: Install dependencies**

Run: `pip install -r requirements.txt`
Expected: All packages install successfully

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: scaffold project structure"
```

---

### Task 2: Metric Data Model

**Files:**
- Create: `src/models.py`
- Create: `tests/test_models.py`

**Step 1: Write the failing test**

`tests/test_models.py`:
```python
import pytest
from src.models import MetricDefinition, MetricSource


def test_simple_metric_from_dict():
    data = {
        "metric": {
            "name": "DAU",
            "aliases": ["daily active users", "platform DAU"],
            "type": "simple",
            "aggregation": "avg",
            "unit": "count",
            "sources": [
                {
                    "id": "platform_dau",
                    "layer": "dws",
                    "table": "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
                    "columns": {"value": "a1", "date": "grass_date", "region": "grass_region"},
                    "filters": ["tz_type = 'local'"],
                    "use_when": {"granularity": ["platform"]},
                }
            ],
            "dimensions": {"required": ["market", "date_range"], "optional": ["module"]},
        }
    }
    m = MetricDefinition.from_dict(data)
    assert m.name == "DAU"
    assert m.type == "simple"
    assert "daily active users" in m.aliases
    assert len(m.sources) == 1
    assert m.sources[0].table == "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live"


def test_complex_metric_from_dict():
    data = {
        "metric": {
            "name": "Ads Rev by Channel",
            "aliases": ["ads revenue breakdown"],
            "type": "complex",
            "snippet_file": "snippets/ads_rev_by_channel.sql",
            "sub_metrics": ["search_ads_rev_usd", "dd_ads_rev_usd"],
            "dimensions": {"required": ["market", "date_range"], "optional": []},
        }
    }
    m = MetricDefinition.from_dict(data)
    assert m.type == "complex"
    assert m.snippet_file == "snippets/ads_rev_by_channel.sql"
    assert len(m.sub_metrics) == 2


def test_source_select_by_granularity():
    s1 = MetricSource(
        id="platform_dau", layer="dws",
        table="t1", columns={"value": "a1", "date": "d", "region": "r"},
        filters=[], use_when={"granularity": ["platform"]},
    )
    s2 = MetricSource(
        id="module_dau", layer="dwd",
        table="t2", columns={"value": "a2", "date": "d", "region": "r", "module": "m"},
        filters=[], use_when={"granularity": ["module"]},
    )
    m = MetricDefinition(
        name="DAU", aliases=[], type="simple", aggregation="avg",
        unit="count", sources=[s1, s2],
        dimensions={"required": ["market", "date_range"], "optional": ["module"]},
    )
    assert m.select_source(granularity="platform") == s1
    assert m.select_source(granularity="module") == s2


def test_source_select_default_first():
    s1 = MetricSource(
        id="default", layer="dws", table="t1",
        columns={"value": "v", "date": "d", "region": "r"},
        filters=[], use_when={},
    )
    m = MetricDefinition(
        name="X", aliases=[], type="simple", aggregation="avg",
        unit="count", sources=[s1],
        dimensions={"required": ["market", "date_range"], "optional": []},
    )
    assert m.select_source() == s1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.models'`

**Step 3: Write minimal implementation**

`src/models.py`:
```python
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MetricSource:
    id: str
    layer: str
    table: str
    columns: dict
    filters: list[str] = field(default_factory=list)
    use_when: dict = field(default_factory=dict)


@dataclass
class MetricDefinition:
    name: str
    aliases: list[str]
    type: str  # "simple" or "complex"
    dimensions: dict
    sources: list[MetricSource] = field(default_factory=list)
    aggregation: Optional[str] = None
    unit: Optional[str] = None
    snippet_file: Optional[str] = None
    sub_metrics: list[str] = field(default_factory=list)
    owner: Optional[str] = None
    notes: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "MetricDefinition":
        m = data["metric"]
        sources = [MetricSource(**s) for s in m.get("sources", [])]
        return cls(
            name=m["name"],
            aliases=m.get("aliases", []),
            type=m["type"],
            dimensions=m["dimensions"],
            sources=sources,
            aggregation=m.get("aggregation"),
            unit=m.get("unit"),
            snippet_file=m.get("snippet_file"),
            sub_metrics=m.get("sub_metrics", []),
            owner=m.get("owner"),
            notes=m.get("notes"),
        )

    def select_source(self, granularity: Optional[str] = None) -> MetricSource:
        if granularity:
            for s in self.sources:
                if granularity in s.use_when.get("granularity", []):
                    return s
        return self.sources[0]
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_models.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/models.py tests/test_models.py
git commit -m "feat: add MetricDefinition and MetricSource data models"
```

---

### Task 3: YAML Registry Loader

**Files:**
- Create: `src/registry.py`
- Create: `tests/test_registry.py`
- Create: `metrics/dau.yaml` (first real metric)

**Step 1: Write the first metric YAML**

`metrics/dau.yaml`:
```yaml
metric:
  name: DAU
  aliases: ["daily active users", "platform DAU", "日活"]
  type: simple
  aggregation: avg
  unit: count

  sources:
    - id: platform_dau
      layer: dws
      table: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
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

**Step 2: Write the failing test**

`tests/test_registry.py`:
```python
import os
import pytest
from src.registry import MetricRegistry

METRICS_DIR = os.path.join(os.path.dirname(__file__), "..", "metrics")


def test_load_all_metrics():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    assert len(registry.metrics) >= 1
    assert "DAU" in [m.name for m in registry.metrics]


def test_find_by_name():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("DAU")
    assert result is not None
    assert result.name == "DAU"


def test_find_by_alias():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("daily active users")
    assert result is not None
    assert result.name == "DAU"


def test_find_case_insensitive():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("dau")
    assert result is not None
    assert result.name == "DAU"


def test_find_not_found():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("nonexistent metric")
    assert result is None


def test_list_metric_names():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    names = registry.list_names_and_aliases()
    assert any("DAU" in entry for entry in names)
```

**Step 3: Run test to verify it fails**

Run: `pytest tests/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.registry'`

**Step 4: Write minimal implementation**

`src/registry.py`:
```python
import os
import yaml
from typing import Optional
from src.models import MetricDefinition


class MetricRegistry:
    def __init__(self, metrics_dir: str):
        self.metrics_dir = metrics_dir
        self.metrics: list[MetricDefinition] = []
        self._name_index: dict[str, MetricDefinition] = {}

    def load(self):
        self.metrics = []
        self._name_index = {}
        for fname in os.listdir(self.metrics_dir):
            if not fname.endswith(".yaml") and not fname.endswith(".yml"):
                continue
            path = os.path.join(self.metrics_dir, fname)
            with open(path) as f:
                data = yaml.safe_load(f)
            if data and "metric" in data:
                m = MetricDefinition.from_dict(data)
                self.metrics.append(m)
                self._name_index[m.name.lower()] = m
                for alias in m.aliases:
                    self._name_index[alias.lower()] = m

    def find(self, name: str) -> Optional[MetricDefinition]:
        return self._name_index.get(name.lower())

    def list_names_and_aliases(self) -> list[str]:
        result = []
        for m in self.metrics:
            aliases_str = ", ".join(m.aliases)
            result.append(f"{m.name} ({aliases_str})")
        return result
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/test_registry.py -v`
Expected: All 6 tests PASS

**Step 6: Commit**

```bash
git add src/registry.py tests/test_registry.py metrics/dau.yaml
git commit -m "feat: add MetricRegistry YAML loader with name/alias lookup"
```

---

### Task 4: Write All S&R&A Metric YAMLs

**Files:**
- Create: `metrics/buyer_uv.yaml`
- Create: `metrics/gmv.yaml`
- Create: `metrics/ads_gross_rev.yaml`
- Create: `metrics/take_rate.yaml`
- Create: `metrics/ads_roi.yaml`
- Create: `metrics/net_ads_rev.yaml`
- Create: `metrics/net_take_rate.yaml`
- Create: `metrics/commission_fee.yaml`
- Create: `metrics/rebate.yaml`
- Create: `metrics/order_pct_by_channel.yaml`
- Create: `metrics/ads_rev_pct_by_channel.yaml`

**Step 1: Write all simple metric YAMLs**

Extract definitions from `monthly_core_metrics_tracker.sql`. Each YAML follows the same structure as `dau.yaml`. Key source tables per metric:

| Metric | Source Table | Value Column | Aggregation |
|--------|-------------|-------------|-------------|
| Buyer UV | traffic_omni_oa.dwd_order_item_atc_journey_di__reg_sensitive_live | count distinct user_id | avg |
| GMV | mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live | platform_gmv_excl_testorder | avg |
| Take Rate | derived: ads_rev_usd / gmv_usd_1d | — | — |
| Ads ROI | derived: ads_gmv_usd / ads_rev_usd | — | — |
| Net Take Rate | derived: net_ads_rev / gmv_usd_1d | — | — |
| Commission Fee | mp_order.dwd_order_item_all_ent_df__reg_s0_live | commission_fee_usd | avg |
| Rebate | mp_order.dwd_order_item_all_ent_df__reg_s0_live | (sum of 7 rebate columns) | avg |

For derived metrics (take_rate, ads_roi, net_take_rate), use `type: derived` with `formula` referencing other metrics:

`metrics/take_rate.yaml`:
```yaml
metric:
  name: Gross Take Rate
  aliases: ["take rate", "货币化率", "gross take rate"]
  type: derived
  formula: "ads_gross_rev / gmv"
  depends_on: ["Ads Gross Rev", "GMV"]
  unit: ratio
  dimensions:
    required: [market, date_range]
    optional: []
```

`metrics/buyer_uv.yaml`:
```yaml
metric:
  name: Buyer UV
  aliases: ["buyer count", "买家数", "buyer uv"]
  type: simple
  aggregation: avg
  unit: count

  sources:
    - id: buyer_uv
      layer: dwd
      table: traffic_omni_oa.dwd_order_item_atc_journey_di__reg_sensitive_live
      columns:
        value: "count(distinct case when gmv_usd * atc_prorate * first_touchpoint_item > 0 then user_id end)"
        date: grass_date
        region: grass_region
      filters:
        - "tz_type = 'local'"
        - "first_touchpoint_item = 1"
        - "user_id > 0"
        - "user_id is not null"
        - "order_item_id is not null"
      use_when:
        granularity: [platform]

  dimensions:
    required: [market, date_range]
    optional: []
```

`metrics/gmv.yaml`:
```yaml
metric:
  name: GMV
  aliases: ["gross merchandise value", "交易额", "gmv"]
  type: simple
  aggregation: avg
  unit: usd

  sources:
    - id: gmv_from_ads
      layer: dws
      table: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
      columns:
        value: "sum(distinct platform_gmv_excl_testorder)"
        date: grass_date
        region: grass_region
      filters:
        - "tz_type = 'regional'"
      use_when:
        granularity: [platform]

  dimensions:
    required: [market, date_range]
    optional: []
```

`metrics/commission_fee.yaml`:
```yaml
metric:
  name: Commission Fee
  aliases: ["commission", "佣金", "commission fee"]
  type: simple
  aggregation: avg
  unit: usd

  sources:
    - id: commission
      layer: dwd
      table: mp_order.dwd_order_item_all_ent_df__reg_s0_live
      columns:
        value: "sum(commission_fee_usd)"
        date: "cast(create_datetime as date)"
        region: grass_region
      filters:
        - "tz_type = 'local'"
        - "(grass_date >= cast(create_datetime as date) or grass_date = date '9999-01-01')"
      use_when:
        granularity: [platform]

  dimensions:
    required: [market, date_range]
    optional: []
```

`metrics/rebate.yaml`:
```yaml
metric:
  name: Rebate
  aliases: ["total rebate", "返利", "rebate"]
  type: simple
  aggregation: avg
  unit: usd

  sources:
    - id: rebate
      layer: dwd
      table: mp_order.dwd_order_item_all_ent_df__reg_s0_live
      columns:
        value: "sum(sv_coin_earn_by_shopee_amt_usd) + sum(pv_coin_earn_by_shopee_amt_usd) + sum(actual_shipping_rebate_by_shopee_amt_usd) + sum(pv_rebate_by_shopee_amt_usd) + sum(sv_rebate_by_shopee_amt_usd) + sum(item_rebate_by_shopee_amt_usd) + sum(card_rebate_by_shopee_amt_usd)"
        date: "cast(create_datetime as date)"
        region: grass_region
      filters:
        - "tz_type = 'local'"
        - "(grass_date >= cast(create_datetime as date) or grass_date = date '9999-01-01')"
      use_when:
        granularity: [platform]

  dimensions:
    required: [market, date_range]
    optional: []
```

`metrics/ads_gross_rev.yaml`:
```yaml
metric:
  name: Ads Gross Rev
  aliases: ["ads revenue", "广告收入", "ads gross rev", "ads rev"]
  type: complex
  snippet_file: snippets/ads_gross_rev.sql
  unit: usd
  dimensions:
    required: [market, date_range]
    optional: []
  notes: |
    New data from mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live.
    Entry points differ between old and new tables.
```

`metrics/net_ads_rev.yaml`:
```yaml
metric:
  name: Net Ads Rev
  aliases: ["net ads revenue", "净广告收入", "net ads rev"]
  type: complex
  snippet_file: snippets/net_ads_rev.sql
  unit: usd
  dimensions:
    required: [market, date_range]
    optional: []
  notes: |
    BR market has special SCS credit adjustment (2025_0034_BR_AD_SAS_CREDITS).
    Uses net_ads_rev_usd from mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live.
```

`metrics/ads_roi.yaml`:
```yaml
metric:
  name: Ads Direct ROI
  aliases: ["ads roi", "广告ROI", "ads direct roi"]
  type: derived
  formula: "ads_gmv / ads_gross_rev"
  depends_on: ["Ads Gross Rev"]
  unit: ratio
  dimensions:
    required: [market, date_range]
    optional: []
```

`metrics/net_take_rate.yaml`:
```yaml
metric:
  name: Net Take Rate
  aliases: ["net take rate", "净货币化率"]
  type: derived
  formula: "net_ads_rev / gmv"
  depends_on: ["Net Ads Rev", "GMV"]
  unit: ratio
  dimensions:
    required: [market, date_range]
    optional: []
```

`metrics/order_pct_by_channel.yaml`:
```yaml
metric:
  name: Order Pct by Channel
  aliases: ["order ratio by channel", "订单占比按渠道", "order% by channel",
            "search order ratio", "DD order ratio", "YMAL order ratio"]
  type: complex
  snippet_file: snippets/order_by_channel.sql
  sub_metrics:
    - global_search_order_ratio
    - dd_order_ratio
    - ymal_order_ratio
    - post_purchase_order_ratio
    - private_domain_order_ratio
    - live_order_ratio
    - video_order_ratio
    - other_order_ratio
  unit: ratio
  dimensions:
    required: [market, date_range]
    optional: []
  notes: |
    Source: dev_video_bi.sr_okr_table_metric_dws
    Jan-Apr 2023 uses temp_video_ls_202301_202304 for live/video orders.
```

`metrics/ads_rev_pct_by_channel.yaml`:
```yaml
metric:
  name: Ads Rev Pct by Channel
  aliases: ["ads revenue ratio by channel", "广告收入占比按渠道", "ads rev% by channel",
            "search ads ratio", "DD ads ratio"]
  type: complex
  snippet_file: snippets/ads_rev_by_channel.sql
  sub_metrics:
    - search_ads_rev_ratio
    - dd_ads_rev_ratio
    - rcmd_ads_rev_ratio
    - game_ads_rev_ratio
    - brand_ads_rev_ratio
    - live_ads_rev_ratio
    - video_ads_rev_ratio
    - undefined_rev_ratio
  unit: ratio
  dimensions:
    required: [market, date_range]
    optional: []
```

**Step 2: Write a test that verifies all YAMLs load**

Add to `tests/test_registry.py`:
```python
def test_load_all_sra_metrics():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    expected_names = [
        "DAU", "Buyer UV", "GMV", "Ads Gross Rev", "Gross Take Rate",
        "Ads Direct ROI", "Net Ads Rev", "Net Take Rate",
        "Commission Fee", "Rebate", "Order Pct by Channel", "Ads Rev Pct by Channel",
    ]
    loaded_names = [m.name for m in registry.metrics]
    for name in expected_names:
        assert name in loaded_names, f"Missing metric: {name}"
```

**Step 3: Run test**

Run: `pytest tests/test_registry.py::test_load_all_sra_metrics -v`
Expected: PASS — all 12 metrics load correctly

**Step 4: Commit**

```bash
git add metrics/ tests/test_registry.py
git commit -m "feat: add all S&R&A metric YAML definitions (12 metrics)"
```

---

### Task 5: SQL Assembly — Simple Metric Template

**Files:**
- Create: `templates/simple_metric.sql.j2`
- Create: `src/assembler.py`
- Create: `tests/test_assembler.py`

**Step 1: Write the Jinja2 template**

`templates/simple_metric.sql.j2`:
```
SELECT
    substr(cast({{ source.columns.date }} as varchar), 1, 7) AS period
{%- if market %}
    , {{ source.columns.region }} AS market
{%- endif %}
    , {{ metric.aggregation }}({{ source.columns.value }}) AS {{ metric.name | lower | replace(' ', '_') }}
FROM
    (
        SELECT
            {{ source.columns.region }}
            , {{ source.columns.date }}
            , {{ source.columns.value }} AS {{ metric.name | lower | replace(' ', '_') }}_raw
        FROM {{ source.table }}
        WHERE {{ source.columns.date }} BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
{%- for f in source.filters %}
            AND {{ f }}
{%- endfor %}
{%- if market %}
            AND {{ source.columns.region }} = '{{ market }}'
{%- endif %}
        GROUP BY 1, 2
    ) daily
GROUP BY 1{% if market %}, 2{% endif %}
ORDER BY 1 DESC
```

**Step 2: Write the failing test**

`tests/test_assembler.py`:
```python
import pytest
from src.assembler import SQLAssembler
from src.models import MetricDefinition, MetricSource


@pytest.fixture
def assembler():
    return SQLAssembler(templates_dir="templates", snippets_dir="snippets")


@pytest.fixture
def dau_metric():
    source = MetricSource(
        id="platform_dau", layer="dws",
        table="traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
        columns={"value": "a1", "date": "grass_date", "region": "grass_region"},
        filters=["tz_type = 'local'"],
        use_when={"granularity": ["platform"]},
    )
    return MetricDefinition(
        name="DAU", aliases=[], type="simple", aggregation="avg",
        unit="count", sources=[source],
        dimensions={"required": ["market", "date_range"], "optional": []},
    )


def test_simple_metric_sql(assembler, dau_metric):
    sql = assembler.assemble(
        metric=dau_metric,
        market="ID",
        date_start="2025-11-01",
        date_end="2025-11-30",
    )
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in sql
    assert "grass_region = 'ID'" in sql
    assert "BETWEEN date '2025-11-01' AND date '2025-11-30'" in sql
    assert "avg(" in sql
    assert "tz_type = 'local'" in sql
    assert "ORDER BY 1 DESC" in sql


def test_simple_metric_no_market(assembler, dau_metric):
    sql = assembler.assemble(
        metric=dau_metric,
        market=None,
        date_start="2025-11-01",
        date_end="2025-11-30",
    )
    assert "grass_region = " not in sql
    assert "AS market" not in sql
```

**Step 3: Run test to verify it fails**

Run: `pytest tests/test_assembler.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.assembler'`

**Step 4: Write minimal implementation**

`src/assembler.py`:
```python
import os
from jinja2 import Environment, FileSystemLoader
from src.models import MetricDefinition


class SQLAssembler:
    def __init__(self, templates_dir: str = "templates", snippets_dir: str = "snippets"):
        self.templates_dir = templates_dir
        self.snippets_dir = snippets_dir
        self.env = Environment(
            loader=FileSystemLoader(templates_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def assemble(
        self,
        metric: MetricDefinition,
        market: str | None = None,
        date_start: str = "",
        date_end: str = "",
        granularity: str = "platform",
    ) -> str:
        if metric.type == "simple":
            return self._assemble_simple(metric, market, date_start, date_end, granularity)
        elif metric.type == "complex":
            return self._assemble_complex(metric, market, date_start, date_end)
        elif metric.type == "derived":
            return self._assemble_derived(metric, market, date_start, date_end)
        raise ValueError(f"Unknown metric type: {metric.type}")

    def _assemble_simple(self, metric, market, date_start, date_end, granularity):
        source = metric.select_source(granularity=granularity)
        template = self.env.get_template("simple_metric.sql.j2")
        return template.render(
            metric=metric,
            source=source,
            market=market,
            date_start=date_start,
            date_end=date_end,
        )

    def _assemble_complex(self, metric, market, date_start, date_end):
        snippet_path = os.path.join(self.snippets_dir, os.path.basename(metric.snippet_file))
        with open(snippet_path) as f:
            snippet_template = f.read()
        template = self.env.from_string(snippet_template)
        return template.render(
            market=market,
            date_start=date_start,
            date_end=date_end,
        )

    def _assemble_derived(self, metric, market, date_start, date_end):
        # For POC: derived metrics return a comment explaining the formula
        return f"-- Derived metric: {metric.name}\n-- Formula: {metric.formula}\n-- Resolve depends_on metrics first, then compute ratio"
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/test_assembler.py -v`
Expected: All 2 tests PASS

**Step 6: Commit**

```bash
git add src/assembler.py tests/test_assembler.py templates/simple_metric.sql.j2
git commit -m "feat: add SQL assembler with Jinja2 template for simple metrics"
```

---

### Task 6: SQL Assembly — Complex Metric Snippets

**Files:**
- Create: `snippets/ads_gross_rev.sql`
- Create: `snippets/net_ads_rev.sql`
- Create: `snippets/order_by_channel.sql`
- Create: `snippets/ads_rev_by_channel.sql`
- Add tests to: `tests/test_assembler.py`

**Step 1: Extract ads_gross_rev snippet from monthly_core_metrics_tracker.sql**

`snippets/ads_gross_rev.sql` — extract the m2 subquery from the ETL, parameterize with Jinja2:
```sql
-- Ads Gross Revenue by entry point
-- Source: monthly_core_metrics_tracker.sql (m2 subquery)
SELECT
    grass_region
    , substr(cast(grass_date as varchar), 1, 7) AS period
    , avg(ads_rev_usd) AS ads_gross_rev
    , avg(ads_gmv_usd) AS ads_gmv
FROM
    (
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
    ) n1
GROUP BY 1, 2
ORDER BY 2 DESC
```

`snippets/order_by_channel.sql`:
```sql
-- Order % by Channel (first-lead attribution)
-- Source: monthly_core_metrics_tracker.sql (m4 subquery)
SELECT
    grass_region
    , substr(cast(grass_date as varchar), 1, 7) AS period
    , avg(CASE WHEN feature = 'Platform' THEN order_cnt_login_user_first_lead END) AS platform_order_1d
    , avg(CASE WHEN feature = 'Global Search' THEN order_cnt_login_user_first_lead END) AS global_search_order_1d
    , avg(CASE WHEN feature = 'Daily Discover' THEN order_cnt_login_user_first_lead END) AS dd_order_1d
    , avg(CASE WHEN feature = 'You May Also Like' THEN order_cnt_login_user_first_lead END) AS ymal_order_1d
    , avg(CASE WHEN feature = 'post purchase' THEN order_cnt_login_user_first_lead END) AS post_purchase_order_1d
    , avg(CASE WHEN feature = 'Private Domain Features' THEN order_cnt_login_user_first_lead END) AS private_domain_order_1d
    , avg(CASE WHEN feature = 'Live Streaming' THEN order_cnt_login_user_first_lead END) AS live_order_1d
    , avg(CASE WHEN feature = 'Video' THEN order_cnt_login_user_first_lead END) AS video_order_1d
FROM dev_video_bi.sr_okr_table_metric_dws
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'regional'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
ORDER BY 2 DESC
```

`snippets/ads_rev_by_channel.sql`:
```sql
-- Ads Revenue % by Channel (entry point breakdown)
-- Source: monthly_core_metrics_tracker.sql (m2 subquery, detailed)
SELECT
    grass_region
    , substr(cast(grass_date as varchar), 1, 7) AS period
    , avg(ads_rev_usd) AS total_ads_rev
    , avg(search_ads_rev_usd) AS search_ads_rev
    , avg(dd_ads_rev_usd) AS dd_ads_rev
    , avg(rcmd_ads_rev_usd) AS rcmd_ads_rev
    , avg(game_ads_rev_usd) AS game_ads_rev
    , avg(brand_ads_rev_usd) AS brand_ads_rev
    , avg(live_ads_rev_usd) AS live_ads_rev
    , avg(video_ads_rev_usd) AS video_ads_rev
FROM
    (
        SELECT
            grass_region, grass_date
            , sum(ads_rev_usd) AS ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Global Search', 'Image Search') THEN ads_rev_usd END) AS search_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Daily Discover External', 'Daily Discover Mix Feed Internal', 'DD External Video') THEN ads_rev_usd END) AS dd_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Cart Recommendation', 'Me You May Also Like', 'My Purchase Page Recommendation', 'Order Detail Page Recommendation', 'Order Successful Recommendation', 'You May Also Like') THEN ads_rev_usd END) AS rcmd_ads_rev_usd
            , sum(CASE WHEN entry_point = 'Game' THEN ads_rev_usd END) AS game_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Shop', 'Shop Game', 'Display') THEN ads_rev_usd END) AS brand_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Livestream Autolanding', 'Livestream PDP', 'Livestream Discovery', 'Livestream Homepage', 'Livestream Video Feed', 'Livestream For You') THEN ads_rev_usd END) AS live_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Video Trending Tab', 'HP Internal Video', 'DD Internal Video') THEN ads_rev_usd END) AS video_ads_rev_usd
        FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
        WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
            AND tz_type = 'regional'
{%- if market %}
            AND grass_region = '{{ market }}'
{%- endif %}
        GROUP BY 1, 2
    ) n
GROUP BY 1, 2
ORDER BY 2 DESC
```

`snippets/net_ads_rev.sql`:
```sql
-- Net Ads Revenue (with BR SCS credit adjustment)
-- Source: monthly_core_metrics_tracker.sql (m2 subquery, net portion)
SELECT
    n1.grass_region
    , substr(cast(n1.grass_date as varchar), 1, 7) AS period
    , avg(net_ads_rev) AS net_ads_rev
    , avg(net_ads_rev_excl_1p + coalesce(br_scs, 0)) AS net_ads_rev_excl_1p
FROM
    (
        SELECT
            grass_region, grass_date
            , sum(net_ads_rev_usd) AS net_ads_rev
            , sum(CASE WHEN seller_type_1p NOT IN ('Local SCS', 'SCS', 'Lovito') THEN net_ads_rev_excl_sip_usd_1d END) AS net_ads_rev_excl_1p
        FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
        WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
            AND tz_type = 'regional'
{%- if market %}
            AND grass_region = '{{ market }}'
{%- endif %}
        GROUP BY 1, 2
    ) n1
    LEFT JOIN (
        SELECT grass_date, grass_region, sum(free_rev) AS br_scs
        FROM (
            SELECT grass_date, grass_region
                , CASE
                    WHEN credit_topup_type_name LIKE '%free credit%' AND credit_program_name = '' THEN 'free_credit_topup_others'
                    WHEN credit_topup_type_name LIKE '%free credit%' THEN coalesce(credit_program_name, 'free_credit_topup_others')
                    WHEN credit_topup_type_name LIKE '%paid credit%' AND credit_program_name = '' THEN 'paid_credit_topup_others'
                    WHEN credit_topup_type_name LIKE '%paid credit%' THEN coalesce(credit_program_name, 'paid_credit_topup_others')
                    ELSE coalesce(credit_program_name, 'others')
                END AS credit_program_name
                , sum(free_ads_revenue_amt_usd_1d) AS free_rev
            FROM mp_paidads.dws_advertise_net_ads_revenue_1d__reg_s0_live
            WHERE grass_date >= date '{{ date_start }}'
                AND tz_type = 'regional'
                AND grass_region = 'BR'
            GROUP BY 1, 2, 3
        ) nn
        WHERE credit_program_name = '2025_0034_BR_AD_SAS_CREDITS'
        GROUP BY 1, 2
    ) n2 ON n1.grass_date = n2.grass_date AND n1.grass_region = n2.grass_region
GROUP BY 1, 2
ORDER BY 2 DESC
```

**Step 2: Write test for complex metric assembly**

Add to `tests/test_assembler.py`:
```python
def test_complex_metric_ads_gross_rev(assembler):
    from src.models import MetricDefinition
    m = MetricDefinition(
        name="Ads Gross Rev", aliases=[], type="complex",
        snippet_file="snippets/ads_gross_rev.sql",
        dimensions={"required": ["market", "date_range"], "optional": []},
    )
    sql = assembler.assemble(metric=m, market="ID", date_start="2025-11-01", date_end="2025-11-30")
    assert "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live" in sql
    assert "grass_region = 'ID'" in sql
    assert "BETWEEN date '2025-11-01' AND date '2025-11-30'" in sql


def test_complex_metric_order_by_channel(assembler):
    from src.models import MetricDefinition
    m = MetricDefinition(
        name="Order Pct by Channel", aliases=[], type="complex",
        snippet_file="snippets/order_by_channel.sql",
        dimensions={"required": ["market", "date_range"], "optional": []},
    )
    sql = assembler.assemble(metric=m, market="TH", date_start="2025-10-01", date_end="2025-10-31")
    assert "sr_okr_table_metric_dws" in sql
    assert "grass_region = 'TH'" in sql
    assert "Global Search" in sql
    assert "Daily Discover" in sql
```

**Step 3: Run test**

Run: `pytest tests/test_assembler.py -v`
Expected: All 4 tests PASS

**Step 4: Commit**

```bash
git add snippets/ tests/test_assembler.py
git commit -m "feat: add SQL snippets for complex metrics (ads rev, orders, net ads)"
```

---

### Task 7: SQL Assembly — Compare Template

**Files:**
- Create: `templates/compare.sql.j2`
- Add tests to: `tests/test_assembler.py`

**Step 1: Write the compare template**

`templates/compare.sql.j2`:
```
WITH current_period AS (
    {{ base_query | indent(4) }}
),
previous_period AS (
    {{ base_query_prev | indent(4) }}
)
SELECT
    c.period AS current_period
    , p.period AS previous_period
    , c.{{ metric_col }} AS current_value
    , p.{{ metric_col }} AS previous_value
    , (c.{{ metric_col }} - p.{{ metric_col }}) / NULLIF(p.{{ metric_col }}, 0) AS change_rate
FROM current_period c
LEFT JOIN previous_period p
    ON c.period IS NOT NULL AND p.period IS NOT NULL
{%- if has_market %}
    AND c.market = p.market
{%- endif %}
```

**Step 2: Add compare method to assembler and test**

Add to `tests/test_assembler.py`:
```python
def test_compare_sql(assembler, dau_metric):
    sql = assembler.assemble_compare(
        metric=dau_metric,
        market="ID",
        current_start="2025-11-01", current_end="2025-11-30",
        previous_start="2025-10-01", previous_end="2025-10-31",
    )
    assert "current_period" in sql
    assert "previous_period" in sql
    assert "change_rate" in sql
    assert "2025-11-01" in sql
    assert "2025-10-01" in sql
```

**Step 3: Add `assemble_compare` to `src/assembler.py`**

```python
def assemble_compare(
    self,
    metric: MetricDefinition,
    market: str | None,
    current_start: str, current_end: str,
    previous_start: str, previous_end: str,
    granularity: str = "platform",
) -> str:
    base_query = self.assemble(metric, market, current_start, current_end, granularity)
    base_query_prev = self.assemble(metric, market, previous_start, previous_end, granularity)
    metric_col = metric.name.lower().replace(" ", "_")
    template = self.env.get_template("compare.sql.j2")
    return template.render(
        base_query=base_query,
        base_query_prev=base_query_prev,
        metric_col=metric_col,
        has_market=market is not None,
    )
```

**Step 4: Run test**

Run: `pytest tests/test_assembler.py -v`
Expected: All 5 tests PASS

**Step 5: Commit**

```bash
git add templates/compare.sql.j2 src/assembler.py tests/test_assembler.py
git commit -m "feat: add MoM/comparison SQL template"
```

---

### Task 8: LLM Intent & Entity Extractor

**Files:**
- Create: `src/extractor.py`
- Create: `tests/test_extractor.py`

**Step 1: Write the failing test**

`tests/test_extractor.py`:
```python
import json
import pytest
from unittest.mock import patch, MagicMock
from src.extractor import IntentExtractor
from src.registry import MetricRegistry

METRICS_DIR = "metrics"


@pytest.fixture
def extractor():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    return IntentExtractor(registry=registry, model="claude-sonnet-4-20250514")


def test_build_system_prompt(extractor):
    prompt = extractor.build_system_prompt()
    assert "DAU" in prompt
    assert "daily active users" in prompt
    assert "market" in prompt
    assert "JSON" in prompt


def test_parse_response_valid():
    raw = json.dumps({
        "intent": "query",
        "metrics": ["DAU"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": None,
            "module": None,
        },
        "clarification_needed": None,
    })
    result = IntentExtractor.parse_response(raw)
    assert result["intent"] == "query"
    assert result["metrics"] == ["DAU"]
    assert result["dimensions"]["market"] == "ID"


def test_parse_response_with_clarification():
    raw = json.dumps({
        "intent": "query",
        "metrics": [],
        "dimensions": {"market": None, "date_range": None, "compare_to": None, "module": None},
        "clarification_needed": "Did you mean Ads Gross Rev or Net Ads Rev?",
    })
    result = IntentExtractor.parse_response(raw)
    assert result["clarification_needed"] is not None
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_extractor.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

`src/extractor.py`:
```python
import json
from typing import Optional
from src.registry import MetricRegistry

SYSTEM_PROMPT_TEMPLATE = """You are a metric query parser for the S&R&A (Search, Recommendation & Ads) team at Shopee.

Available metrics:
{metric_list}

Available dimensions:
- market: ID, VN, TH, TW, BR, MX, PH, SG, MY, CO, CL (country codes)
- date_range: supports YYYY-MM-DD dates, "last month", "November 2025", etc.
- compare_to: MoM (month-over-month), YoY (year-over-year), or specific period
- module: DD (Daily Discover), YMAL (You May Also Like), PP (Post Purchase), etc.

Parse the user question into this exact JSON format:
{{
  "intent": "query" | "compare" | "trend" | "breakdown",
  "metrics": ["metric_name"],
  "dimensions": {{
    "market": "XX" | null,
    "date_range": {{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}} | null,
    "compare_to": {{"type": "MoM" | "YoY" | "custom", "start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}} | null,
    "module": "XX" | null
  }},
  "clarification_needed": null | "string explaining what's ambiguous"
}}

Rules:
- Use exact metric names from the list above
- If the question is ambiguous (e.g., "revenue" could be multiple metrics), set clarification_needed
- For monthly data, use first and last day of month (e.g., 2025-11-01 to 2025-11-30)
- If no market specified, set market to null (means all markets)
- Return ONLY valid JSON, no other text"""


class IntentExtractor:
    def __init__(self, registry: MetricRegistry, model: str = "claude-sonnet-4-20250514"):
        self.registry = registry
        self.model = model

    def build_system_prompt(self) -> str:
        metric_list = "\n".join(
            f"- {entry}" for entry in self.registry.list_names_and_aliases()
        )
        return SYSTEM_PROMPT_TEMPLATE.format(metric_list=metric_list)

    def extract(self, question: str) -> dict:
        import anthropic
        client = anthropic.Anthropic()
        response = client.messages.create(
            model=self.model,
            max_tokens=1024,
            system=self.build_system_prompt(),
            messages=[{"role": "user", "content": question}],
        )
        raw = response.content[0].text
        return self.parse_response(raw)

    @staticmethod
    def parse_response(raw: str) -> dict:
        # Strip markdown code fences if present
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        return json.loads(text)
```

**Step 4: Run test**

Run: `pytest tests/test_extractor.py -v`
Expected: All 3 tests PASS (no API calls in these tests)

**Step 5: Commit**

```bash
git add src/extractor.py tests/test_extractor.py
git commit -m "feat: add LLM intent/entity extractor with prompt builder"
```

---

### Task 9: Agent CLI — Wire Everything Together

**Files:**
- Create: `src/agent.py`
- Create: `tests/test_agent.py`

**Step 1: Write the failing test**

`tests/test_agent.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from src.agent import Agent


@pytest.fixture
def agent():
    return Agent(metrics_dir="metrics", templates_dir="templates", snippets_dir="snippets")


def test_agent_init(agent):
    assert len(agent.registry.metrics) >= 1


@patch("src.extractor.IntentExtractor.extract")
def test_agent_simple_query(mock_extract, agent):
    mock_extract.return_value = {
        "intent": "query",
        "metrics": ["DAU"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": None,
            "module": None,
        },
        "clarification_needed": None,
    }
    result = agent.ask("What is ID DAU in November 2025?")
    assert "sql" in result
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in result["sql"]
    assert "grass_region = 'ID'" in result["sql"]


@patch("src.extractor.IntentExtractor.extract")
def test_agent_compare_query(mock_extract, agent):
    mock_extract.return_value = {
        "intent": "compare",
        "metrics": ["DAU"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": {"type": "MoM", "start": "2025-10-01", "end": "2025-10-31"},
            "module": None,
        },
        "clarification_needed": None,
    }
    result = agent.ask("Compare ID DAU between Oct and Nov 2025")
    assert "sql" in result
    assert "current_period" in result["sql"]
    assert "change_rate" in result["sql"]


@patch("src.extractor.IntentExtractor.extract")
def test_agent_clarification(mock_extract, agent):
    mock_extract.return_value = {
        "intent": "query",
        "metrics": [],
        "dimensions": {"market": None, "date_range": None, "compare_to": None, "module": None},
        "clarification_needed": "Did you mean Ads Gross Rev or Net Ads Rev?",
    }
    result = agent.ask("What is the revenue?")
    assert "clarification" in result
    assert "Ads Gross Rev" in result["clarification"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

`src/agent.py`:
```python
import sys
from src.registry import MetricRegistry
from src.extractor import IntentExtractor
from src.assembler import SQLAssembler


class Agent:
    def __init__(
        self,
        metrics_dir: str = "metrics",
        templates_dir: str = "templates",
        snippets_dir: str = "snippets",
        model: str = "claude-sonnet-4-20250514",
    ):
        self.registry = MetricRegistry(metrics_dir)
        self.registry.load()
        self.extractor = IntentExtractor(registry=self.registry, model=model)
        self.assembler = SQLAssembler(templates_dir=templates_dir, snippets_dir=snippets_dir)

    def ask(self, question: str) -> dict:
        parsed = self.extractor.extract(question)

        if parsed.get("clarification_needed"):
            return {"clarification": parsed["clarification_needed"]}

        metric_name = parsed["metrics"][0] if parsed["metrics"] else None
        if not metric_name:
            return {"error": "No metric identified in the question."}

        metric = self.registry.find(metric_name)
        if not metric:
            return {"error": f"Metric '{metric_name}' not found in registry."}

        dims = parsed["dimensions"]
        market = dims.get("market")
        date_range = dims.get("date_range") or {}
        date_start = date_range.get("start", "")
        date_end = date_range.get("end", "")
        module = dims.get("module")
        granularity = "module" if module else "platform"

        intent = parsed["intent"]

        if intent == "compare" and dims.get("compare_to"):
            comp = dims["compare_to"]
            sql = self.assembler.assemble_compare(
                metric=metric,
                market=market,
                current_start=date_start,
                current_end=date_end,
                previous_start=comp.get("start", ""),
                previous_end=comp.get("end", ""),
                granularity=granularity,
            )
        else:
            sql = self.assembler.assemble(
                metric=metric,
                market=market,
                date_start=date_start,
                date_end=date_end,
                granularity=granularity,
            )

        return {
            "intent": intent,
            "metric": metric.name,
            "market": market,
            "date_range": date_range,
            "sql": sql,
        }


def main():
    agent = Agent()
    print("S&R&A Metric Agent (type 'quit' to exit)")
    print("-" * 50)
    while True:
        try:
            question = input("\nQ: ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if question.lower() in ("quit", "exit", "q"):
            break
        if not question:
            continue

        result = agent.ask(question)

        if "clarification" in result:
            print(f"\nClarification needed: {result['clarification']}")
        elif "error" in result:
            print(f"\nError: {result['error']}")
        else:
            print(f"\nMetric: {result['metric']}")
            print(f"Market: {result.get('market', 'All')}")
            print(f"Intent: {result['intent']}")
            print(f"\n--- Generated SQL ---\n{result['sql']}")


if __name__ == "__main__":
    main()
```

**Step 4: Run test**

Run: `pytest tests/test_agent.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/agent.py tests/test_agent.py
git commit -m "feat: add Agent CLI wiring registry, extractor, and assembler"
```

---

### Task 10: Regression Test Suite

**Files:**
- Create: `tests/test_cases.yaml`
- Create: `tests/test_regression.py`

**Step 1: Write test cases**

`tests/test_cases.yaml`:
```yaml
- question: "ID market DAU in November 2025"
  mock_extract:
    intent: query
    metrics: ["DAU"]
    dimensions:
      market: ID
      date_range: {start: "2025-11-01", end: "2025-11-30"}
      compare_to: null
      module: null
    clarification_needed: null
  expected_sql_contains:
    - "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live"
    - "grass_region = 'ID'"
    - "2025-11-01"
    - "avg("

- question: "Ads gross revenue for TH in October 2025"
  mock_extract:
    intent: query
    metrics: ["Ads Gross Rev"]
    dimensions:
      market: TH
      date_range: {start: "2025-10-01", end: "2025-10-31"}
      compare_to: null
      module: null
    clarification_needed: null
  expected_sql_contains:
    - "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live"
    - "grass_region = 'TH'"
    - "2025-10-01"

- question: "Order percentage by channel for BR in Nov 2025"
  mock_extract:
    intent: query
    metrics: ["Order Pct by Channel"]
    dimensions:
      market: BR
      date_range: {start: "2025-11-01", end: "2025-11-30"}
      compare_to: null
      module: null
    clarification_needed: null
  expected_sql_contains:
    - "sr_okr_table_metric_dws"
    - "grass_region = 'BR'"
    - "Global Search"
    - "Daily Discover"
    - "Live Streaming"

- question: "Compare ID DAU between October and November 2025"
  mock_extract:
    intent: compare
    metrics: ["DAU"]
    dimensions:
      market: ID
      date_range: {start: "2025-11-01", end: "2025-11-30"}
      compare_to: {type: MoM, start: "2025-10-01", end: "2025-10-31"}
      module: null
    clarification_needed: null
  expected_sql_contains:
    - "current_period"
    - "previous_period"
    - "change_rate"
    - "2025-11-01"
    - "2025-10-01"

- question: "Net ads revenue for VN in November 2025"
  mock_extract:
    intent: query
    metrics: ["Net Ads Rev"]
    dimensions:
      market: VN
      date_range: {start: "2025-11-01", end: "2025-11-30"}
      compare_to: null
      module: null
    clarification_needed: null
  expected_sql_contains:
    - "net_ads_rev_usd"
    - "grass_region = 'VN'"
```

**Step 2: Write regression test runner**

`tests/test_regression.py`:
```python
import os
import yaml
import pytest
from unittest.mock import patch
from src.agent import Agent

TESTS_DIR = os.path.dirname(__file__)


def load_test_cases():
    path = os.path.join(TESTS_DIR, "test_cases.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


TEST_CASES = load_test_cases()


@pytest.mark.parametrize(
    "case",
    TEST_CASES,
    ids=[c["question"][:50] for c in TEST_CASES],
)
def test_regression(case):
    agent = Agent(metrics_dir="metrics", templates_dir="templates", snippets_dir="snippets")

    with patch("src.extractor.IntentExtractor.extract") as mock_extract:
        mock_extract.return_value = case["mock_extract"]
        result = agent.ask(case["question"])

    assert "sql" in result, f"Expected SQL output, got: {result}"
    sql = result["sql"]
    for fragment in case["expected_sql_contains"]:
        assert fragment in sql, f"Missing '{fragment}' in SQL:\n{sql}"
```

**Step 3: Run regression tests**

Run: `pytest tests/test_regression.py -v`
Expected: All 5 test cases PASS

**Step 4: Commit**

```bash
git add tests/test_cases.yaml tests/test_regression.py
git commit -m "feat: add regression test suite with 5 known-good test cases"
```

---

### Task 11: Manual Smoke Test

**Step 1: Run the CLI agent interactively (requires ANTHROPIC_API_KEY)**

Run: `python -m src.agent`

Test these questions:
- "What is ID DAU in November 2025?"
- "Show me ads revenue breakdown by channel for TH, October 2025"
- "Compare BR take rate between September and October 2025"

**Step 2: Verify output SQL looks correct by comparing to `output.sql` reference**

**Step 3: Final commit**

```bash
git add -A
git commit -m "chore: complete POC implementation of semantic layer agent"
```
