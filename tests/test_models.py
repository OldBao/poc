import pytest
from src.models import (
    MetricDefinition, MetricSource, Rule, JoinAdjustment, WrapAdjustment,
    AssemblyContext, AtomicSource, AtomicColumn, QueryIntent,
)


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


def test_rule_from_dict():
    data = {
        "rule": {
            "name": "BR SCS Credit",
            "description": "Adjusts net rev for BR",
            "when": {"market": "BR", "metric_tags": ["revenue", "net"]},
            "effect": {
                "type": "left_join",
                "snippet_file": "snippets/adjustments/br_scs_credit.sql",
                "join_keys": ["grass_date", "grass_region"],
            },
            "valid_from": "2025-01-01",
        }
    }
    rule = Rule.from_dict(data)
    assert rule.name == "BR SCS Credit"
    assert rule.when == {"market": "BR", "metric_tags": ["revenue", "net"]}
    assert rule.effect_type == "left_join"
    assert rule.snippet_file == "snippets/adjustments/br_scs_credit.sql"
    assert rule.join_keys == ["grass_date", "grass_region"]
    assert rule.valid_from == "2025-01-01"


def test_rule_from_dict_minimal():
    data = {
        "rule": {
            "name": "Simple filter",
            "description": "Adds a filter",
            "when": {"metric_tags": ["revenue"]},
            "effect": {"type": "filter", "clause": "AND seller_type != '1P'"},
        }
    }
    rule = Rule.from_dict(data)
    assert rule.name == "Simple filter"
    assert rule.effect_type == "filter"
    assert rule.clause == "AND seller_type != '1P'"
    assert rule.snippet_file is None
    assert rule.valid_from is None


def test_assembly_context_defaults():
    ctx = AssemblyContext(base_snippet="SELECT 1")
    assert ctx.joins == []
    assert ctx.filters == []
    assert ctx.columns == []
    assert ctx.wrappers == []


def test_metric_definition_with_tags():
    data = {
        "metric": {
            "name": "Net Ads Rev",
            "aliases": ["net ads revenue"],
            "type": "complex",
            "tags": ["revenue", "net", "ads"],
            "dimensions": {"required": ["market", "date_range"], "optional": []},
        }
    }
    m = MetricDefinition.from_dict(data)
    assert m.tags == ["revenue", "net", "ads"]


def test_metric_definition_without_tags():
    data = {
        "metric": {
            "name": "DAU",
            "aliases": [],
            "type": "simple",
            "dimensions": {"required": ["market", "date_range"], "optional": []},
        }
    }
    m = MetricDefinition.from_dict(data)
    assert m.tags == []


def test_atomic_metric_from_dict():
    data = {
        "metric": {
            "name": "Net Ads Rev",
            "aliases": ["net ads revenue"],
            "type": "atomic",
            "tags": ["revenue", "net", "ads"],
            "source": {
                "table": "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live",
                "grain": "daily",
                "date_column": "grass_date",
                "region_column": "grass_region",
                "base_filters": ["tz_type = 'regional'"],
            },
            "columns": {
                "net_ads_rev": {
                    "expr": "sum(net_ads_rev_usd)",
                    "agg_across_days": "sum",
                },
                "net_ads_rev_excl_1p": {
                    "expr": "sum(CASE WHEN seller_type_1p NOT IN ('Local SCS') THEN net_ads_rev_excl_sip_usd_1d END)",
                    "agg_across_days": "sum",
                    "variant": "excl_1p",
                },
            },
            "dimensions": {"required": ["market", "date_range"]},
        }
    }
    m = MetricDefinition.from_dict(data)
    assert m.type == "atomic"
    assert m.atomic_source is not None
    assert m.atomic_source.table == "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live"
    assert m.atomic_source.grain == "daily"
    assert m.atomic_source.date_column == "grass_date"
    assert m.atomic_source.region_column == "grass_region"
    assert m.atomic_source.base_filters == ["tz_type = 'regional'"]
    assert len(m.atomic_columns) == 2
    assert m.atomic_columns["net_ads_rev"].expr == "sum(net_ads_rev_usd)"
    assert m.atomic_columns["net_ads_rev"].agg_across_days == "sum"
    assert m.atomic_columns["net_ads_rev"].variant is None
    assert m.atomic_columns["net_ads_rev_excl_1p"].variant == "excl_1p"


def test_atomic_metric_from_dict_no_source():
    """Non-atomic metrics should have atomic_source=None."""
    data = {
        "metric": {
            "name": "DAU",
            "aliases": [],
            "type": "simple",
            "dimensions": {"required": ["market", "date_range"]},
        }
    }
    m = MetricDefinition.from_dict(data)
    assert m.atomic_source is None
    assert m.atomic_columns == {}


def test_query_intent_defaults():
    intent = QueryIntent(metric_name="Test")
    assert intent.granularity == "monthly"
    assert intent.market is None
    assert intent.variant is None
