"""Integration test: Rule Engine end-to-end with mocked LLM."""
import os
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

    # Templates (empty, needed by Agent constructor)
    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()

    # Mock LLM
    mock_llm = MagicMock()

    agent = Agent(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        templates_dir=str(templates_dir),
        rules_dir=str(rules_dir),
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )
    return agent, mock_llm


def test_br_query_includes_scs_adjustment(agent_with_rules):
    """When querying BR, the LLM prompt should include BR SCS Credit adjustment."""
    agent, mock_llm = agent_with_rules

    # Mock extractor to return BR + Nov 2025
    mock_llm.call.side_effect = [
        # First call: extractor
        {
            "intent": "query",
            "metrics": ["Net Ads Rev"],
            "dimensions": {
                "market": "BR",
                "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            },
            "clarification_needed": None,
        },
        # Second call: complex SQL generation
        {"sql": "SELECT 1 FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live"},
    ]

    result = agent.ask("Net Ads Rev BR Nov 2025")

    # Check the second LLM call (SQL generation) received the adjustment context
    calls = mock_llm.call.call_args_list
    assert len(calls) >= 2
    sql_gen_call = calls[1]
    # Get system_prompt from the call (could be positional or keyword)
    system_prompt = sql_gen_call.kwargs.get("system_prompt", "")
    if not system_prompt and sql_gen_call.args:
        system_prompt = sql_gen_call.args[0]
    # The prompt should mention BR SCS Credit
    assert "BR SCS Credit" in system_prompt, f"Expected 'BR SCS Credit' in prompt, got: {system_prompt[:200]}"


def test_th_query_excludes_scs_adjustment(agent_with_rules):
    """When querying TH, the LLM prompt should NOT include BR SCS Credit adjustment."""
    agent, mock_llm = agent_with_rules

    mock_llm.call.side_effect = [
        {
            "intent": "query",
            "metrics": ["Net Ads Rev"],
            "dimensions": {
                "market": "TH",
                "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            },
            "clarification_needed": None,
        },
        {"sql": "SELECT 1 FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live"},
    ]

    result = agent.ask("Net Ads Rev TH Nov 2025")

    calls = mock_llm.call.call_args_list
    assert len(calls) >= 2
    sql_gen_call = calls[1]
    system_prompt = sql_gen_call.kwargs.get("system_prompt", "")
    if not system_prompt and sql_gen_call.args:
        system_prompt = sql_gen_call.args[0]
    assert "BR SCS Credit" not in system_prompt, f"Did not expect 'BR SCS Credit' in prompt for TH query"
