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
