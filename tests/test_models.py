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
