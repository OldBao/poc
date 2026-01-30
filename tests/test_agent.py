# tests/test_agent.py
import json
from unittest.mock import patch, MagicMock
from src.agent import Agent, parse_response


def test_start_sends_system_and_user_message():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT 1"}'

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        raw = agent.start("ID DAU Nov 2025")

    assert raw == '{"type": "sql", "sql": "SELECT 1"}'
    call_args = mock_llm.chat.call_args[1]
    messages = call_args["messages"]
    assert messages[0]["role"] == "system"
    assert messages[1] == {"role": "user", "content": "ID DAU Nov 2025"}
    assert len(messages) == 2


def test_start_resets_history():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.return_value = "Which market?"

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        agent.start("rev?")
        agent.start("DAU?")

    second_call_messages = mock_llm.chat.call_args[1]["messages"]
    assert len(second_call_messages) == 2
    assert second_call_messages[1]["content"] == "DAU?"


def test_follow_up_appends_to_history():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.side_effect = [
            "Which market and date range?",
            '{"type": "sql", "sql": "SELECT 1"}',
        ]

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        agent.start("Ads Gross Rev")
        raw = agent.follow_up("ID Nov 2025")

    assert raw == '{"type": "sql", "sql": "SELECT 1"}'
    messages = mock_llm.chat.call_args[1]["messages"]
    assert len(messages) == 4
    assert messages[0]["role"] == "system"
    assert messages[1] == {"role": "user", "content": "Ads Gross Rev"}
    assert messages[2] == {"role": "assistant", "content": "Which market and date range?"}
    assert messages[3] == {"role": "user", "content": "ID Nov 2025"}


def test_follow_up_accumulates_multiple_turns():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.side_effect = [
            "Which market?",
            "Which date range?",
            '{"type": "sql", "sql": "SELECT 1"}',
        ]

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        agent.start("Ads Gross Rev")
        agent.follow_up("ID")
        raw = agent.follow_up("Nov 2025")

    assert raw == '{"type": "sql", "sql": "SELECT 1"}'
    messages = mock_llm.chat.call_args[1]["messages"]
    assert len(messages) == 6


def test_parse_response_sql_json():
    rtype, data = parse_response('{"type": "sql", "sql": "SELECT 1"}')
    assert rtype == "sql"
    assert data == {"type": "sql", "sql": "SELECT 1"}


def test_parse_response_ambiguous_json():
    rtype, data = parse_response('{"type": "ambiguous", "candidates": ["A", "B"]}')
    assert rtype == "ambiguous"
    assert data == {"type": "ambiguous", "candidates": ["A", "B"]}


def test_parse_response_plain_text():
    rtype, data = parse_response("Which market do you need?")
    assert rtype == "text"
    assert data == "Which market do you need?"


def test_parse_response_json_in_markdown_fences():
    rtype, data = parse_response('```json\n{"type": "sql", "sql": "SELECT 1"}\n```')
    assert rtype == "sql"
    assert data == {"type": "sql", "sql": "SELECT 1"}
