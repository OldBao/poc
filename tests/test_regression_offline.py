"""
Offline regression tests for the hybrid pipeline.
Uses mock LLM to test the full pipeline without API calls.
"""
import pytest
from unittest.mock import MagicMock
from src.agent import Agent


@pytest.fixture
def agent_factory(tmp_path):
    """Create an agent with a mock LLM that returns pre-configured responses."""
    def _make(llm_response):
        mock_llm = MagicMock()
        mock_llm.call.return_value = llm_response
        agent = Agent(
            metrics_dir="metrics",
            snippets_dir="snippets",
            templates_dir="templates",
            value_index_path=str(tmp_path / "test.db"),
            llm_client=mock_llm,
        )
        # Seed value index with known markets
        for table in [
            "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
            "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live",
            "dev_video_bi.sr_okr_table_metric_dws",
            "traffic_omni_oa.dwd_order_item_atc_journey_di__reg_sensitive_live",
        ]:
            agent.value_index.upsert(
                table, "grass_region",
                [("ID", 1000), ("TH", 500), ("VN", 300), ("BR", 200),
                 ("MX", 150), ("PH", 120), ("SG", 100), ("MY", 90),
                 ("TW", 80), ("CO", 50), ("CL", 40)],
            )
        return agent
    return _make


def test_simple_dau_query(agent_factory):
    """DAU query uses template path, produces correct SQL."""
    agent = agent_factory({
        "intent": "query",
        "metrics": ["dau"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
        },
        "clarification_needed": None,
    })
    result = agent.ask("ID market DAU in November 2025")
    assert result["type"] == "sql"
    sql = result["sql"]
    assert "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live" in sql
    assert "grass_region = 'ID'" in sql
    assert "avg(a1)" in sql
    assert "2025-11-01" in sql
    assert "2025-11-30" in sql
    assert "tz_type = 'local'" in sql


def test_compare_dau_query(agent_factory):
    """Compare query uses compare template with CTEs."""
    agent = agent_factory({
        "intent": "compare",
        "metrics": ["dau"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": {"start": "2025-10-01", "end": "2025-10-31"},
        },
        "clarification_needed": None,
    })
    result = agent.ask("Compare ID DAU between October and November 2025")
    assert result["type"] == "sql"
    sql = result["sql"]
    assert "current_period" in sql
    assert "previous_period" in sql
    assert "change_rate" in sql
    assert "2025-11-01" in sql
    assert "2025-10-01" in sql


def test_ambiguous_query(agent_factory):
    """Ambiguous query returns clarification."""
    agent = agent_factory({
        "intent": "query",
        "metrics": [],
        "dimensions": {},
        "clarification_needed": "Did you mean Ads Gross Rev or Net Ads Rev?",
    })
    result = agent.ask("What's the revenue?")
    assert result["type"] == "clarification"
    assert "Did you mean" in result["message"]


def test_unknown_metric_returns_error(agent_factory):
    """Unknown metric returns an error."""
    agent = agent_factory({
        "intent": "query",
        "metrics": ["nonexistent"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
        },
        "clarification_needed": None,
    })
    result = agent.ask("Show me the nonexistent metric")
    assert result["type"] == "error"
    assert "not found" in result["message"].lower()


def test_simple_metric_no_market(agent_factory):
    """Simple query without market filter."""
    agent = agent_factory({
        "intent": "query",
        "metrics": ["dau"],
        "dimensions": {
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
        },
        "clarification_needed": None,
    })
    result = agent.ask("DAU in November 2025")
    assert result["type"] == "sql"
    assert "grass_region = " not in result["sql"]
