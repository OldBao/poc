import os
import pytest
import yaml
from src.rule_engine import RuleEngine
from src.models import AssemblyContext, JoinAdjustment, WrapAdjustment


@pytest.fixture
def rules_dir(tmp_path):
    rule1 = {
        "rule": {
            "name": "BR SCS Credit",
            "description": "BR credit adjustment",
            "when": {"market": "BR", "metric_tags": ["revenue", "net"]},
            "effect": {
                "type": "left_join",
                "snippet_file": "snippets/adjustments/br_scs_credit.sql",
                "join_keys": ["grass_date", "grass_region"],
            },
            "valid_from": "2025-01-01",
        }
    }
    rule2 = {
        "rule": {
            "name": "LATAM Currency",
            "description": "Currency normalization for LATAM",
            "when": {"market": ["BR", "MX", "CO", "CL"], "metric_tags": ["revenue"]},
            "effect": {
                "type": "wrap",
                "snippet_file": "snippets/adjustments/latam_currency.sql",
                "priority": 10,
            },
        }
    }
    rule3 = {
        "rule": {
            "name": "1P Exclusion",
            "description": "Exclude 1P sellers",
            "when": {"metric_tags": ["revenue"]},
            "effect": {"type": "filter", "clause": "AND seller_type_1p NOT IN ('Local SCS', 'SCS')"},
        }
    }

    for i, rule in enumerate([rule1, rule2, rule3]):
        path = tmp_path / f"rule_{i}.yaml"
        with open(path, "w") as f:
            yaml.dump(rule, f)

    return str(tmp_path)


@pytest.fixture
def engine(rules_dir):
    engine = RuleEngine(rules_dir=rules_dir)
    engine.load()
    return engine


def test_load_rules(engine):
    assert len(engine.rules) == 3


def test_match_br_net_revenue(engine):
    matched = engine.match(market="BR", metric_tags=["revenue", "net", "ads"])
    names = [r.name for r in matched]
    assert "BR SCS Credit" in names
    assert "LATAM Currency" in names
    assert "1P Exclusion" in names


def test_match_th_net_revenue(engine):
    matched = engine.match(market="TH", metric_tags=["revenue", "net", "ads"])
    names = [r.name for r in matched]
    assert "BR SCS Credit" not in names
    assert "LATAM Currency" not in names
    assert "1P Exclusion" in names


def test_match_br_volume_metric(engine):
    matched = engine.match(market="BR", metric_tags=["volume"])
    assert len(matched) == 0


def test_match_with_date_filter(engine):
    matched = engine.match(
        market="BR",
        metric_tags=["revenue", "net"],
        query_date_start="2024-06-01",
    )
    names = [r.name for r in matched]
    # valid_from is 2025-01-01, query starts 2024-06 -> rule should NOT match
    assert "BR SCS Credit" not in names


def test_match_with_date_after_valid_from(engine):
    matched = engine.match(
        market="BR",
        metric_tags=["revenue", "net"],
        query_date_start="2025-03-01",
    )
    names = [r.name for r in matched]
    assert "BR SCS Credit" in names


@pytest.fixture
def snippets_dir(tmp_path):
    adj_dir = tmp_path / "snippets" / "adjustments"
    adj_dir.mkdir(parents=True)
    (adj_dir / "br_scs_credit.sql").write_text(
        "SELECT grass_date, grass_region, sum(free_rev) AS br_scs\n"
        "FROM mp_paidads.dws_advertise_net_ads_revenue_1d__reg_s0_live\n"
        "WHERE grass_date >= date '{{ date_start }}'\n"
        "GROUP BY 1, 2"
    )
    (adj_dir / "latam_currency.sql").write_text(
        "SELECT *, amount * fx_rate AS amount_usd FROM base"
    )
    return str(tmp_path / "snippets")


def test_build_context_with_join(engine, snippets_dir):
    # Override snippet paths to use temp dir
    for r in engine.rules:
        if r.snippet_file:
            r.snippet_file = os.path.join(
                snippets_dir, os.path.relpath(r.snippet_file, "snippets")
            )

    matched = engine.match(market="BR", metric_tags=["revenue", "net"])
    ctx = engine.build_context(
        base_snippet="SELECT * FROM base_table",
        matched_rules=matched,
    )

    assert ctx.base_snippet == "SELECT * FROM base_table"
    assert len(ctx.joins) >= 1
    assert ctx.joins[0].name == "BR SCS Credit"
    assert "br_scs" in ctx.joins[0].snippet


def test_build_context_no_rules():
    engine = RuleEngine(rules_dir="/nonexistent")
    ctx = engine.build_context(
        base_snippet="SELECT 1",
        matched_rules=[],
    )
    assert ctx.base_snippet == "SELECT 1"
    assert ctx.joins == []
    assert ctx.filters == []
    assert ctx.wrappers == []


def test_build_context_filter_rule(engine):
    matched = engine.match(market="TH", metric_tags=["revenue", "net"])
    ctx = engine.build_context(
        base_snippet="SELECT * FROM t",
        matched_rules=matched,
    )
    assert len(ctx.filters) == 1
    assert "seller_type_1p" in ctx.filters[0]
