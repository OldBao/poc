import pytest
import yaml

from src.atomic_assembler import AtomicAssembler
from src.models import (
    AtomicColumn,
    AtomicSource,
    MetricDefinition,
    QueryIntent,
)


@pytest.fixture
def atomic_metric():
    return MetricDefinition(
        name="Net Ads Rev",
        aliases=["net ads revenue"],
        type="atomic",
        tags=["revenue", "net", "ads"],
        dimensions={"required": ["market", "date_range"]},
        atomic_source=AtomicSource(
            table="mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live",
            grain="daily",
            date_column="grass_date",
            region_column="grass_region",
            base_filters=["tz_type = 'regional'"],
        ),
        atomic_columns={
            "net_ads_rev": AtomicColumn(
                expr="sum(net_ads_rev_usd)",
                agg_across_days="sum",
            ),
            "net_ads_rev_excl_1p": AtomicColumn(
                expr="sum(CASE WHEN seller_type_1p NOT IN ('Local SCS', 'SCS', 'Lovito') THEN net_ads_rev_excl_sip_usd_1d END)",
                agg_across_days="sum",
                variant="excl_1p",
            ),
        },
    )


@pytest.fixture
def assembler():
    return AtomicAssembler()


# --- Granularity tests ---


def test_daily_granularity(assembler, atomic_metric):
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-11-01",
        date_end="2025-11-30",
        granularity="daily",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live" in sql
    assert "BETWEEN date '2025-11-01' AND date '2025-11-30'" in sql
    assert "tz_type = 'regional'" in sql
    assert "grass_region = 'ID'" in sql
    assert "grass_date AS period" in sql
    assert "sum(net_ads_rev)" in sql
    assert "GROUP BY" in sql


def test_monthly_granularity(assembler, atomic_metric):
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-01-01",
        date_end="2025-12-31",
        granularity="monthly",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "substr(cast(n1.grass_date as varchar), 1, 7) AS period" in sql
    assert "GROUP BY n1.grass_region, substr(" in sql


def test_yearly_granularity(assembler, atomic_metric):
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-01-01",
        date_end="2025-12-31",
        granularity="yearly",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "substr(cast(n1.grass_date as varchar), 1, 4) AS period" in sql
    assert "GROUP BY n1.grass_region, substr(" in sql


def test_total_granularity(assembler, atomic_metric):
    """Total granularity: one row per region, no period column."""
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-01-01",
        date_end="2025-12-31",
        granularity="total",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "GROUP BY n1.grass_region" in sql
    assert "period" not in sql
    assert "sum(net_ads_rev) AS net_ads_rev" in sql


def test_total_matches_design_doc(assembler, atomic_metric):
    """Verify the exact problem case from the design doc produces correct SQL."""
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-01-01",
        date_end="2025-12-31",
        granularity="total",
    )
    sql = assembler.assemble(atomic_metric, intent)

    # Inner query groups at daily grain
    assert "GROUP BY 1, 2" in sql
    # Outer query groups by region only (no period for total)
    assert "GROUP BY n1.grass_region" in sql
    # Both columns present (no variant filter)
    assert "sum(net_ads_rev) AS net_ads_rev" in sql
    assert "sum(net_ads_rev_excl_1p) AS net_ads_rev_excl_1p" in sql


# --- Variant filtering ---


def test_variant_filter(assembler, atomic_metric):
    """With variant='excl_1p', only the excl_1p column should appear."""
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-11-01",
        date_end="2025-11-30",
        granularity="monthly",
        variant="excl_1p",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "net_ads_rev_excl_1p" in sql
    # The default net_ads_rev column (no variant) should NOT appear
    lines = sql.split("\n")
    select_lines = [l for l in lines if "AS net_ads_rev" in l and "excl_1p" not in l]
    assert len(select_lines) == 0, f"Default column should not appear: {select_lines}"


def test_no_variant_includes_all_columns(assembler, atomic_metric):
    """With no variant, all columns should appear."""
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-11-01",
        date_end="2025-11-30",
        granularity="monthly",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "net_ads_rev" in sql
    assert "net_ads_rev_excl_1p" in sql


def test_unknown_variant_raises(assembler, atomic_metric):
    """Unknown variant should raise ValueError."""
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-11-01",
        date_end="2025-11-30",
        granularity="monthly",
        variant="nonexistent",
    )
    with pytest.raises(ValueError, match="No columns match variant"):
        assembler.assemble(atomic_metric, intent)


# --- Edge cases ---


def test_no_market(assembler, atomic_metric):
    """When market is None, no region filter should appear in WHERE."""
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market=None,
        date_start="2025-11-01",
        date_end="2025-11-30",
        granularity="monthly",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "grass_region = " not in sql
    # But region column should still be in SELECT and GROUP BY
    assert "grass_region" in sql


def test_no_date_range(assembler, atomic_metric):
    """When dates are None, no date filter should appear."""
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start=None,
        date_end=None,
        granularity="total",
    )
    sql = assembler.assemble(atomic_metric, intent)

    assert "BETWEEN" not in sql
    assert "tz_type = 'regional'" in sql


def test_invalid_granularity(assembler, atomic_metric):
    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-01-01",
        date_end="2025-12-31",
        granularity="weekly",
    )
    with pytest.raises(ValueError, match="Unsupported granularity"):
        assembler.assemble(atomic_metric, intent)


def test_no_atomic_source_raises(assembler):
    """Metric without atomic_source should raise ValueError."""
    metric = MetricDefinition(
        name="Plain",
        aliases=[],
        type="simple",
        dimensions={"required": []},
    )
    intent = QueryIntent(metric_name="Plain", granularity="daily")
    with pytest.raises(ValueError, match="no atomic source"):
        assembler.assemble(metric, intent)


# --- YAML parsing ---


def test_atomic_from_yaml(tmp_path):
    """Verify YAML parsing creates correct AtomicSource and AtomicColumn objects."""
    yaml_content = """\
metric:
  name: Test Atomic
  aliases: ["test"]
  type: atomic
  source:
    table: test_schema.test_table
    grain: daily
    date_column: dt
    region_column: region
    base_filters:
      - "active = 1"
  columns:
    total_val:
      expr: "sum(amount)"
      agg_across_days: "sum"
    avg_val:
      expr: "avg(score)"
      agg_across_days: "avg"
      variant: "avg_only"
  dimensions:
    required: [market, date_range]
"""
    data = yaml.safe_load(yaml_content)
    metric = MetricDefinition.from_dict(data)

    assert metric.type == "atomic"
    assert metric.atomic_source is not None
    assert metric.atomic_source.table == "test_schema.test_table"
    assert metric.atomic_source.grain == "daily"
    assert metric.atomic_source.date_column == "dt"
    assert metric.atomic_source.region_column == "region"
    assert metric.atomic_source.base_filters == ["active = 1"]

    assert len(metric.atomic_columns) == 2
    assert metric.atomic_columns["total_val"].expr == "sum(amount)"
    assert metric.atomic_columns["total_val"].agg_across_days == "sum"
    assert metric.atomic_columns["total_val"].variant is None
    assert metric.atomic_columns["avg_val"].variant == "avg_only"


def test_atomic_end_to_end_from_yaml():
    """Load the actual net_ads_rev_atomic.yaml and assemble SQL."""
    with open("metrics/net_ads_rev_atomic.yaml") as f:
        data = yaml.safe_load(f)

    metric = MetricDefinition.from_dict(data)
    assembler = AtomicAssembler()

    intent = QueryIntent(
        metric_name="Net Ads Rev",
        market="ID",
        date_start="2025-01-01",
        date_end="2025-12-31",
        granularity="total",
    )
    sql = assembler.assemble(metric, intent)

    assert "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live" in sql
    assert "sum(net_ads_rev) AS net_ads_rev" in sql
    assert "GROUP BY n1.grass_region" in sql
    assert "period" not in sql
