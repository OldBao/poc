"""
Offline regression tests for the single-conversation agent.
Uses mock LLM to test the agent's response parsing and validation.
"""
import json
import pytest
from unittest.mock import MagicMock
from src.agent import Agent


@pytest.fixture
def agent_factory(tmp_path):
    """Create an agent with a mock LLM."""
    def _make(chat_return_value):
        mock_llm = MagicMock()
        mock_llm.chat.return_value = chat_return_value
        agent = Agent(
            metrics_dir="metrics",
            snippets_dir="snippets",
            rules_dir="rules",
            value_index_path=str(tmp_path / "test.db"),
            llm_client=mock_llm,
        )
        return agent
    return _make


def test_simple_dau_query(agent_factory):
    """SQL response with valid table passes validation."""
    sql = (
        "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, "
        "grass_region AS market, avg(a1) AS dau "
        "FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live "
        "WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' "
        "AND tz_type = 'local' AND grass_region = 'ID' "
        "GROUP BY 1, 2 ORDER BY 1 DESC"
    )
    agent = agent_factory(json.dumps({"type": "sql", "sql": sql}))
    result = agent.ask("ID market DAU in November 2025")
    assert result["type"] == "sql"
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in result["sql"]
    assert "grass_region = 'ID'" in result["sql"]
    assert "avg(a1)" in result["sql"]


def test_compare_dau_query(agent_factory):
    """Comparison SQL with CTEs passes validation."""
    sql = (
        "WITH current_period AS ("
        "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, "
        "grass_region AS market, avg(a1) AS dau "
        "FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live "
        "WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' "
        "AND tz_type = 'local' AND grass_region = 'ID' GROUP BY 1, 2), "
        "previous_period AS ("
        "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, "
        "grass_region AS market, avg(a1) AS dau "
        "FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live "
        "WHERE grass_date BETWEEN date '2025-10-01' AND date '2025-10-31' "
        "AND tz_type = 'local' AND grass_region = 'ID' GROUP BY 1, 2) "
        "SELECT c.period, p.period AS prev_period, c.market, c.dau, p.dau AS prev_dau, "
        "(c.dau - p.dau) / NULLIF(p.dau, 0) AS change_rate "
        "FROM current_period c LEFT JOIN previous_period p ON c.market = p.market"
    )
    agent = agent_factory(json.dumps({"type": "sql", "sql": sql}))
    result = agent.ask("Compare ID DAU between October and November 2025")
    assert result["type"] == "sql"
    assert "current_period" in result["sql"]
    assert "previous_period" in result["sql"]
    assert "change_rate" in result["sql"]


def test_ambiguous_query(agent_factory):
    """Ambiguous response returns clarification with candidates."""
    agent = agent_factory(json.dumps({
        "type": "ambiguous",
        "candidates": ["Ads Gross Rev", "Net Ads Rev"],
    }))
    result = agent.ask("What's the revenue?")
    assert result["type"] == "clarification"
    assert "Ads Gross Rev" in result["candidates"]


def test_clarification_plain_text(agent_factory):
    """Plain text response returns clarification."""
    agent = agent_factory("Which market and date range are you looking for?")
    result = agent.ask("Show me the metrics")
    assert result["type"] == "clarification"
    assert "market" in result["message"].lower()


def test_validation_rejects_unknown_table(agent_factory):
    """SQL with unknown table fails validation."""
    agent = agent_factory(json.dumps({
        "type": "sql",
        "sql": "SELECT 1 FROM nonexistent.fake_table WHERE 1=1",
    }))
    result = agent.ask("Show me something")
    assert result["type"] == "error"


def test_no_market_query(agent_factory):
    """SQL without market filter is valid for all-market queries."""
    sql = (
        "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, "
        "grass_region AS market, avg(a1) AS dau "
        "FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live "
        "WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' "
        "AND tz_type = 'local' GROUP BY 1, 2 ORDER BY 1 DESC"
    )
    agent = agent_factory(json.dumps({"type": "sql", "sql": sql}))
    result = agent.ask("DAU in November 2025")
    assert result["type"] == "sql"
    assert "grass_region = " not in result["sql"]
