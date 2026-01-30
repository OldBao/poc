import os
import pytest
import yaml
from src.rule_engine import RuleEngine


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
