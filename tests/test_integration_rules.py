"""Integration test: Rules are included in system prompt."""
import yaml
import pytest
from unittest.mock import MagicMock
from src.agent import Agent


@pytest.fixture
def agent_with_rules(tmp_path):
    # Metrics
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    metric = {
        "metric": {
            "name": "Net Ads Rev",
            "aliases": ["net ads revenue"],
            "type": "complex",
            "tags": ["revenue", "net", "ads"],
            "snippet_file": str(tmp_path / "snippets" / "net_ads_rev.sql"),
            "dimensions": {"required": ["market", "date_range"], "optional": []},
            "sources": [
                {
                    "id": "reg",
                    "layer": "reg",
                    "table": "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live",
                    "columns": {"net_ads_rev": "net_ads_rev_usd"},
                }
            ],
        }
    }
    with open(metrics_dir / "net_ads_rev.yaml", "w") as f:
        yaml.dump(metric, f)

    # Snippets
    snippets_dir = tmp_path / "snippets"
    snippets_dir.mkdir()
    adj_dir = snippets_dir / "adjustments"
    adj_dir.mkdir()
    (snippets_dir / "net_ads_rev.sql").write_text(
        "SELECT grass_region, sum(net_ads_rev_usd) AS net_ads_rev\n"
        "FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live\n"
        "WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'\n"
        "GROUP BY 1"
    )
    (adj_dir / "br_scs_credit.sql").write_text(
        "SELECT grass_date, grass_region, sum(free_rev) AS br_scs\n"
        "FROM mp_paidads.dws_advertise_net_ads_revenue_1d__reg_s0_live\n"
        "GROUP BY 1, 2"
    )

    # Rules
    rules_dir = tmp_path / "rules"
    rules_dir.mkdir()
    rule = {
        "rule": {
            "name": "BR SCS Credit",
            "description": "BR adjustment",
            "when": {"market": "BR", "metric_tags": ["revenue", "net"]},
            "effect": {
                "type": "left_join",
                "snippet_file": str(adj_dir / "br_scs_credit.sql"),
                "join_keys": ["grass_date", "grass_region"],
            },
            "valid_from": "2025-01-01",
        }
    }
    with open(rules_dir / "br_scs.yaml", "w") as f:
        yaml.dump(rule, f)

    mock_llm = MagicMock()

    agent = Agent(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        rules_dir=str(rules_dir),
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )
    return agent, mock_llm


def test_system_prompt_includes_rule(agent_with_rules):
    """The system prompt should contain the BR SCS Credit rule."""
    agent, _ = agent_with_rules
    system_prompt = agent.messages[0]["content"]
    assert "BR SCS Credit" in system_prompt
    assert "market = BR" in system_prompt
    assert "revenue" in system_prompt
    assert "left_join" in system_prompt


def test_system_prompt_includes_adjustment_sql(agent_with_rules):
    """The system prompt should contain the adjustment snippet SQL."""
    agent, _ = agent_with_rules
    system_prompt = agent.messages[0]["content"]
    assert "br_scs" in system_prompt
    assert "free_rev" in system_prompt


def test_system_prompt_includes_snippet(agent_with_rules):
    """The system prompt should contain the base snippet SQL."""
    agent, _ = agent_with_rules
    system_prompt = agent.messages[0]["content"]
    assert "net_ads_rev_usd" in system_prompt
