import json
import sys
import pytest
from unittest.mock import patch, MagicMock
from src.llm_backend import (
    OpenAIBackend,
    strip_fences,
    create_backend,
)


# ── strip_fences ──

def test_strip_fences_json():
    assert strip_fences('```json\n{"a":1}\n```') == '{"a":1}'


def test_strip_fences_sql():
    assert strip_fences("```sql\nSELECT 1\n```") == "SELECT 1"


def test_strip_fences_plain():
    assert strip_fences("hello") == "hello"


# ── OpenAIBackend ──

def _mock_openai_response(content: str):
    resp = MagicMock()
    resp.choices = [MagicMock()]
    resp.choices[0].message.content = content
    return resp


def test_openai_generate():
    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = _mock_openai_response(
            "SELECT 1"
        )

        backend = OpenAIBackend(model="gpt-4o")
        result = backend.generate("system", "user")

    assert result == "SELECT 1"


def test_openai_generate_json():
    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = _mock_openai_response(
            '```json\n{"type": "sql", "sql": "SELECT 1"}\n```'
        )

        backend = OpenAIBackend(model="gpt-4o")
        result = backend.generate_json("system", "user")

    assert result == {"type": "sql", "sql": "SELECT 1"}


def test_openai_chat():
    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = _mock_openai_response(
            "Sure, I can help."
        )

        backend = OpenAIBackend(model="gpt-4o")
        result = backend.chat([
            {"role": "system", "content": "test"},
            {"role": "user", "content": "hello"},
        ])

    assert result == "Sure, I can help."


def test_openai_generate_json_list():
    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = _mock_openai_response(
            '```json\n[{"name": "DAU"}]\n```'
        )

        backend = OpenAIBackend(model="gpt-4o")
        result = backend.generate_json_list("system", "user")

    assert result == [{"name": "DAU"}]


# ── ClaudeCodeBackend ──

def _install_mock_claude_sdk():
    """Install a fake claude_code_sdk module into sys.modules."""
    mock_sdk = MagicMock()
    sys.modules["claude_code_sdk"] = mock_sdk
    return mock_sdk


def _cleanup_mock_claude_sdk():
    sys.modules.pop("claude_code_sdk", None)


def test_claude_code_generate():
    mock_sdk = _install_mock_claude_sdk()
    try:
        mock_msg = MagicMock()
        mock_msg.type = "text"
        mock_msg.content = '{"type": "sql", "sql": "SELECT 1"}'

        async def mock_query(prompt, options):
            yield mock_msg

        mock_sdk.query = mock_query
        mock_sdk.ClaudeCodeOptions = MagicMock()

        from src.llm_backend import ClaudeCodeBackend
        backend = ClaudeCodeBackend()
        result = backend.generate("system prompt", "user question")

        assert '"type": "sql"' in result
    finally:
        _cleanup_mock_claude_sdk()


# ── create_backend factory ──

def test_create_backend_openai():
    with patch("openai.OpenAI"):
        backend = create_backend("openai", model="gpt-4o")
    assert isinstance(backend, OpenAIBackend)


def test_create_backend_claude():
    mock_sdk = _install_mock_claude_sdk()
    try:
        mock_sdk.ClaudeCodeOptions = MagicMock()
        from src.llm_backend import ClaudeCodeBackend
        backend = create_backend("claude")
        assert isinstance(backend, ClaudeCodeBackend)
    finally:
        _cleanup_mock_claude_sdk()


def test_create_backend_unknown():
    with pytest.raises(ValueError, match="Unknown backend"):
        create_backend("gemini")
