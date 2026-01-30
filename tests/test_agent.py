import pytest
from unittest.mock import MagicMock
from src.agent import Agent


@pytest.fixture
def mock_llm():
    llm = MagicMock()
    llm.call.return_value = {
        "intent": "query",
        "metrics": ["dau"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
        },
        "clarification_needed": None,
    }
    return llm


def test_simple_metric_uses_template(mock_llm, tmp_path):
    agent = Agent(
        metrics_dir="metrics",
        snippets_dir="snippets",
        templates_dir="templates",
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )
    agent.value_index.upsert(
        "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
        "grass_region",
        [("ID", 1000)],
    )
    result = agent.ask("ID market DAU in November 2025")
    assert result["type"] == "sql"
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in result["sql"]
    assert "avg(a1)" in result["sql"]
    assert "grass_region = 'ID'" in result["sql"]
    # LLM should only be called once (for extraction)
    assert mock_llm.call.call_count == 1


def test_ambiguous_query_returns_clarification(mock_llm, tmp_path):
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": [],
        "dimensions": {},
        "clarification_needed": "Did you mean Ads Gross Rev or Net Ads Rev?",
    }
    agent = Agent(
        metrics_dir="metrics",
        snippets_dir="snippets",
        templates_dir="templates",
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )
    result = agent.ask("What's the revenue?")
    assert result["type"] == "clarification"
    assert "Did you mean" in result["message"]


def test_invalid_market_returns_error(mock_llm, tmp_path):
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": ["dau"],
        "dimensions": {
            "market": "XX",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
        },
        "clarification_needed": None,
    }
    agent = Agent(
        metrics_dir="metrics",
        snippets_dir="snippets",
        templates_dir="templates",
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )
    agent.value_index.upsert(
        "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
        "grass_region",
        [("ID", 1000), ("TH", 500)],
    )
    result = agent.ask("XX market DAU in November 2025")
    assert result["type"] == "error"
    assert "XX" in result["message"]
