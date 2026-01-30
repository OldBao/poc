import pytest
from unittest.mock import patch, MagicMock
from src.agent import Agent


@pytest.fixture
def agent():
    return Agent(metrics_dir="metrics", templates_dir="templates", snippets_dir="snippets")


def test_agent_init(agent):
    assert len(agent.registry.metrics) >= 1


@patch("src.extractor.IntentExtractor.extract")
def test_agent_simple_query(mock_extract, agent):
    mock_extract.return_value = {
        "intent": "query",
        "metrics": ["DAU"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": None,
            "module": None,
        },
        "clarification_needed": None,
    }
    result = agent.ask("What is ID DAU in November 2025?")
    assert "sql" in result
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in result["sql"]
    assert "grass_region = 'ID'" in result["sql"]


@patch("src.extractor.IntentExtractor.extract")
def test_agent_compare_query(mock_extract, agent):
    mock_extract.return_value = {
        "intent": "compare",
        "metrics": ["DAU"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": {"type": "MoM", "start": "2025-10-01", "end": "2025-10-31"},
            "module": None,
        },
        "clarification_needed": None,
    }
    result = agent.ask("Compare ID DAU between Oct and Nov 2025")
    assert "sql" in result
    assert "current_period" in result["sql"]
    assert "change_rate" in result["sql"]


@patch("src.extractor.IntentExtractor.extract")
def test_agent_clarification(mock_extract, agent):
    mock_extract.return_value = {
        "intent": "query",
        "metrics": [],
        "dimensions": {"market": None, "date_range": None, "compare_to": None, "module": None},
        "clarification_needed": "Did you mean Ads Gross Rev or Net Ads Rev?",
    }
    result = agent.ask("What is the revenue?")
    assert "clarification" in result
    assert "Ads Gross Rev" in result["clarification"]
