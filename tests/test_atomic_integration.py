"""Integration tests: atomic metrics through the single-conversation Agent.

In the new architecture, the LLM generates SQL for all metric types including
atomic. These tests verify the Agent correctly handles LLM responses for atomic
metric queries (parsing, validation, conversation flow).

The AtomicAssembler unit tests (test_atomic_assembler.py) cover the deterministic
assembly logic directly.
"""

import json
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
        rules_dir="rules",
        value_index_path=str(tmp_path / "test.db"),
        llm_client=mock_llm,
    )


def test_atomic_metric_monthly(mock_llm, tmp_path):
    """Agent returns SQL for atomic metric monthly query."""
    sql = (
        "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, "
        "grass_region AS market, "
        "sum(net_ads_rev_excl_1p) AS net_ads_rev "
        "FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live "
        "WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' "
        "AND tz_type = 'regional' AND grass_region = 'ID' "
        "GROUP BY 1, 2"
    )
    mock_llm.chat.return_value = json.dumps({"type": "sql", "sql": sql})
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("net ads rev atomic ID November 2025")

    assert result["type"] == "sql"
    assert "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live" in result["sql"]
    assert "grass_region = 'ID'" in result["sql"]


def test_atomic_metric_total(mock_llm, tmp_path):
    """Total granularity produces no period column."""
    sql = (
        "SELECT grass_region AS market, "
        "sum(net_ads_rev_excl_1p) AS net_ads_rev "
        "FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live "
        "WHERE grass_date BETWEEN date '2025-01-01' AND date '2025-12-31' "
        "AND tz_type = 'regional' AND grass_region = 'ID' "
        "GROUP BY grass_region"
    )
    mock_llm.chat.return_value = json.dumps({"type": "sql", "sql": sql})
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("net ads rev atomic ID last year total number")

    assert result["type"] == "sql"
    assert "period" not in result["sql"]


def test_atomic_metric_no_market(mock_llm, tmp_path):
    """All-market query has no grass_region filter."""
    sql = (
        "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, "
        "grass_region AS market, "
        "sum(net_ads_rev_excl_1p) AS net_ads_rev "
        "FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live "
        "WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' "
        "AND tz_type = 'regional' "
        "GROUP BY 1, 2"
    )
    mock_llm.chat.return_value = json.dumps({"type": "sql", "sql": sql})
    agent = _make_agent(mock_llm, tmp_path)
    result = agent.ask("net ads rev atomic all markets November 2025")

    assert result["type"] == "sql"
    assert "grass_region = " not in result["sql"]
