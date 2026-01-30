import pytest
from src.assembler import Assembler
from src.models import MetricDefinition, MetricSource


@pytest.fixture
def dau_metric():
    return MetricDefinition(
        name="DAU",
        aliases=["daily active users"],
        type="simple",
        dimensions={"required": ["market", "date_range"], "optional": []},
        sources=[
            MetricSource(
                id="platform_dau",
                layer="dws",
                table="traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
                columns={"value": "a1", "date": "grass_date", "region": "grass_region"},
                filters=["tz_type = 'local'"],
                use_when={"granularity": ["platform"]},
            )
        ],
        aggregation="avg",
    )


def test_simple_metric_query(dau_metric):
    assembler = Assembler(templates_dir="templates")
    sql = assembler.render_simple(
        metric=dau_metric,
        source=dau_metric.sources[0],
        date_start="2025-11-01",
        date_end="2025-11-30",
        market="ID",
    )
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in sql
    assert "avg(a1)" in sql
    assert "grass_region = 'ID'" in sql
    assert "2025-11-01" in sql
    assert "tz_type = 'local'" in sql


def test_simple_metric_no_market(dau_metric):
    assembler = Assembler(templates_dir="templates")
    sql = assembler.render_simple(
        metric=dau_metric,
        source=dau_metric.sources[0],
        date_start="2025-11-01",
        date_end="2025-11-30",
        market=None,
    )
    assert "grass_region = " not in sql


def test_compare_query(dau_metric):
    assembler = Assembler(templates_dir="templates")
    sql = assembler.render_compare(
        metric=dau_metric,
        source=dau_metric.sources[0],
        current_start="2025-11-01",
        current_end="2025-11-30",
        previous_start="2025-10-01",
        previous_end="2025-10-31",
        market="ID",
    )
    assert "current_period" in sql
    assert "previous_period" in sql
    assert "change_rate" in sql
    assert "2025-11-01" in sql
    assert "2025-10-01" in sql
