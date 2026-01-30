from unittest.mock import patch, MagicMock
from src.llm_client import LLMClient


def test_call_returns_parsed_json():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '{"type": "sql", "sql": "SELECT 1"}'

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.call(system_prompt="You are a helper.", user_message="test")

    assert result == {"type": "sql", "sql": "SELECT 1"}


def test_call_strips_markdown_fences():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '```json\n{"type": "sql", "sql": "SELECT 1"}\n```'

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.call(system_prompt="test", user_message="test")

    assert result == {"type": "sql", "sql": "SELECT 1"}


def test_call_handles_ambiguous_response():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '{"type": "ambiguous", "candidates": ["Ads Gross Rev", "Net Ads Rev"]}'

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.call(system_prompt="test", user_message="revenue?")

    assert result["type"] == "ambiguous"
    assert len(result["candidates"]) == 2


def test_chat_returns_raw_response():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Sure, I can help with that."

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.chat(messages=[
            {"role": "system", "content": "You are a helper."},
            {"role": "user", "content": "Hello"},
        ])

    assert result == "Sure, I can help with that."
    assert isinstance(result, str)


def test_chat_returns_json_string_as_is():
    json_string = '{"type": "sql", "sql": "SELECT 1"}'
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json_string

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.chat(messages=[
            {"role": "system", "content": "test"},
            {"role": "user", "content": "test"},
        ])

    # chat() returns raw string, does NOT parse JSON
    assert result == json_string
    assert isinstance(result, str)
