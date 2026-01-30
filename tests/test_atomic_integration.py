"""Integration tests: atomic metrics through the full Agent pipeline."""

import pytest
from unittest.mock import MagicMock
from src.agent import Agent


@pytest.fixture
def mock_llm():
    return MagicMock()


def _make_agent(mock_llm, tmp_path):
    return Agent(
        metrics_dir="metrics",
        snippets_dir="snippets",
        templates_dir="templates",
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )


def test_atomic_metric_monthly(mock_llm, tmp_path):
    """Agent routes atomic metrics to deterministic assembly (no LLM generation call)."""
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": ["net ads revenue atomic"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "granularity": "monthly",
        },
        "clarification_needed": None,
    }
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("net ads rev atomic ID November 2025")

    assert result["type"] == "sql"
    sql = result["sql"]
    assert "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live" in sql
    assert "substr(cast(n1.grass_date as varchar), 1, 7) AS period" in sql
    assert "grass_region = 'ID'" in sql
    # LLM should only be called once (extraction), not for SQL generation
    assert mock_llm.call.call_count == 1
    assert mock_llm.chat.call_count == 0


def test_atomic_metric_total(mock_llm, tmp_path):
    """Total granularity produces no period column."""
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": ["net ads revenue atomic"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-01-01", "end": "2025-12-31"},
            "granularity": "total",
        },
        "clarification_needed": None,
    }
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("net ads rev atomic ID last year total number")

    assert result["type"] == "sql"
    assert "period" not in result["sql"]
    assert "GROUP BY n1.grass_region" in result["sql"]


def test_atomic_metric_default_granularity(mock_llm, tmp_path):
    """When no granularity specified, default to monthly."""
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": ["net ads revenue atomic"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
        },
        "clarification_needed": None,
    }
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("net ads rev atomic ID November 2025")

    assert result["type"] == "sql"
    assert "substr(cast(n1.grass_date as varchar), 1, 7) AS period" in result["sql"]


def test_atomic_metric_null_dimensions(mock_llm, tmp_path):
    """Atomic handler doesn't crash with null dimensions."""
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": ["net ads revenue atomic"],
        "dimensions": {
            "market": None,
            "date_range": None,
        },
        "clarification_needed": None,
    }
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("net ads rev atomic")

    assert result["type"] == "sql"
    assert "grass_region =" not in result["sql"]
    assert "BETWEEN" not in result["sql"]
