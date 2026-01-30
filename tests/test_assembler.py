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
