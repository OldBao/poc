import json
import pytest
import yaml
from unittest.mock import MagicMock
from src.agent import Agent


@pytest.fixture
def mock_llm():
    return MagicMock()


def _make_agent(mock_llm, tmp_path, metrics_dir="metrics", snippets_dir="snippets", rules_dir="rules"):
    return Agent(
        metrics_dir=metrics_dir,
        snippets_dir=snippets_dir,
        rules_dir=rules_dir,
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )


def test_sql_response(mock_llm, tmp_path):
    """LLM returns JSON with type=sql → agent returns sql result."""
    mock_llm.chat.return_value = json.dumps({
        "type": "sql",
        "sql": "SELECT avg(a1) AS dau FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' AND tz_type = 'local' AND grass_region = 'ID' GROUP BY 1",
    })
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("ID market DAU in November 2025")
    assert result["type"] == "sql"
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in result["sql"]
    assert mock_llm.chat.call_count == 1


def test_ambiguous_response(mock_llm, tmp_path):
    """LLM returns JSON with type=ambiguous → agent returns clarification with candidates."""
    mock_llm.chat.return_value = json.dumps({
        "type": "ambiguous",
        "candidates": ["Ads Gross Rev", "Net Ads Rev"],
    })
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("What's the revenue?")
    assert result["type"] == "clarification"
    assert "candidates" in result
    assert "Ads Gross Rev" in result["candidates"]


def test_clarification_response(mock_llm, tmp_path):
    """LLM returns plain text → agent returns clarification."""
    mock_llm.chat.return_value = "Which market and date range are you interested in?"
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("Show me DAU")
    assert result["type"] == "clarification"
    assert "market" in result["message"].lower()


def test_sql_list_response(mock_llm, tmp_path):
    """LLM returns JSON with type=sql_list → agent returns multiple queries."""
    mock_llm.chat.return_value = json.dumps({
        "type": "sql_list",
        "queries": [
            {"metric": "DAU", "sql": "SELECT avg(a1) FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE 1=1"},
            {"metric": "GMV", "sql": "SELECT sum(gmv_usd) FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live WHERE 1=1"},
        ],
    })
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("DAU and GMV for ID Nov 2025")
    assert result["type"] == "sql_list"
    assert len(result["queries"]) == 2


def test_validation_error(mock_llm, tmp_path):
    """SQL referencing unknown table → agent returns error."""
    mock_llm.chat.return_value = json.dumps({
        "type": "sql",
        "sql": "SELECT 1 FROM nonexistent_schema.fake_table",
    })
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("Something")
    assert result["type"] == "error"


def test_conversation_history(mock_llm, tmp_path):
    """Messages accumulate across ask() calls."""
    mock_llm.chat.return_value = "Which market?"
    agent = _make_agent(mock_llm, tmp_path)

    agent.ask("Show me DAU")
    assert len(agent.messages) == 3  # system + user + assistant

    agent.ask("ID market")
    assert len(agent.messages) == 5  # + user + assistant

    # Verify chat() receives full history each time
    last_call_messages = mock_llm.chat.call_args[0][0]
    assert len(last_call_messages) == 5


def test_reset(mock_llm, tmp_path):
    """reset() clears conversation but keeps system prompt."""
    mock_llm.chat.return_value = "Which market?"
    agent = _make_agent(mock_llm, tmp_path)

    agent.ask("Show me DAU")
    assert len(agent.messages) == 3

    agent.reset()
    assert len(agent.messages) == 1
    assert agent.messages[0]["role"] == "system"


def test_system_prompt_contains_metrics(mock_llm, tmp_path):
    """System prompt includes metric definitions from YAML."""
    agent = _make_agent(mock_llm, tmp_path)
    system_prompt = agent.messages[0]["content"]
    assert "DAU" in system_prompt
    assert "GMV" in system_prompt
    assert "Ads Gross Rev" in system_prompt


def test_system_prompt_contains_rules(mock_llm, tmp_path):
    """System prompt includes rules from rules/ directory."""
    agent = _make_agent(mock_llm, tmp_path)
    system_prompt = agent.messages[0]["content"]
    assert "BR SCS Credit" in system_prompt


def test_system_prompt_contains_conversation_instructions(mock_llm, tmp_path):
    """System prompt includes conversation instructions."""
    agent = _make_agent(mock_llm, tmp_path)
    system_prompt = agent.messages[0]["content"]
    assert "Context Carry-Over" in system_prompt or "CONTEXT CARRY-OVER" in system_prompt
    assert "sql_list" in system_prompt
