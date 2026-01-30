# tests/test_agent.py
from unittest.mock import patch, MagicMock, call
from src.agent import Agent, _handle_result


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


def test_handle_result_ambiguous_numeric_selection():
    """Selecting a number from ambiguous candidates re-asks with that candidate."""
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        # Re-asking with candidate returns SQL
        mock_llm.call.return_value = {"type": "sql", "sql": "SELECT avg(ads_rev) ..."}
        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")

    ambiguous_result = {"type": "ambiguous", "candidates": ["Ads Gross Rev", "Net Ads Rev"]}

    with patch("src.agent._read_input", return_value="1"):
        cont = _handle_result(agent, ambiguous_result)

    assert cont is True
    # Should have re-asked with the selected candidate
    agent.llm.call.assert_called_once()
    call_args = agent.llm.call.call_args
    user_msg = call_args[1].get("user_message") or call_args[0][1]
    assert user_msg == "Ads Gross Rev"


def test_handle_result_need_info_provides_details():
    """When LLM needs more info, user's answer is combined with metric and re-asked."""
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.call.return_value = {"type": "sql", "sql": "SELECT avg(ads_rev) ..."}
        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")

    need_info_result = {
        "type": "need_info",
        "metric": "Ads Gross Rev",
        "missing": ["market", "date_range"],
        "message": "Which market and date range?",
    }

    with patch("src.agent._read_input", return_value="ID November 2025"):
        cont = _handle_result(agent, need_info_result)

    assert cont is True
    agent.llm.call.assert_called_once()
    call_args = agent.llm.call.call_args
    user_msg = call_args[1].get("user_message") or call_args[0][1]
    assert "Ads Gross Rev" in user_msg
    assert "ID November 2025" in user_msg


def test_handle_result_ambiguous_then_quit():
    """Typing quit during ambiguous follow-up exits."""
    ambiguous_result = {"type": "ambiguous", "candidates": ["A", "B"]}

    with patch("src.agent._read_input", return_value="quit"):
        cont = _handle_result(MagicMock(), ambiguous_result)

    assert cont is False


def test_handle_result_sql_returns_immediately():
    """SQL result prints and returns without prompting."""
    sql_result = {"type": "sql", "sql": "SELECT 1"}
    cont = _handle_result(MagicMock(), sql_result)
    assert cont is True
