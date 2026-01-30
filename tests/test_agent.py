# tests/test_agent.py
from unittest.mock import patch, MagicMock
from src.agent import Agent


def test_agent_returns_sql_for_clear_question():
    expected_sql = "SELECT avg(a1) AS dau FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' AND grass_region = 'ID'"

    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.call.return_value = {"type": "sql", "sql": expected_sql}

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        result = agent.ask("ID market DAU in November 2025")

    assert result["type"] == "sql"
    assert "avg(a1)" in result["sql"]
    mock_llm.call.assert_called_once()


def test_agent_returns_ambiguous_for_vague_question():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.call.return_value = {
            "type": "ambiguous",
            "candidates": ["Ads Gross Rev (total ads revenue)", "Net Ads Rev (after deductions)"],
        }

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        result = agent.ask("What's the revenue?")

    assert result["type"] == "ambiguous"
    assert len(result["candidates"]) == 2
