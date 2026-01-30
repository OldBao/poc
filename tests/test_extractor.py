import json
import pytest
from unittest.mock import patch, MagicMock
from src.extractor import IntentExtractor
from src.registry import MetricRegistry

METRICS_DIR = "metrics"


@pytest.fixture
def extractor():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    return IntentExtractor(registry=registry, model="claude-sonnet-4-20250514")


def test_build_system_prompt(extractor):
    prompt = extractor.build_system_prompt()
    assert "DAU" in prompt
    assert "daily active users" in prompt
    assert "market" in prompt
    assert "JSON" in prompt


def test_parse_response_valid():
    raw = json.dumps({
        "intent": "query",
        "metrics": ["DAU"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": None,
            "module": None,
        },
        "clarification_needed": None,
    })
    result = IntentExtractor.parse_response(raw)
    assert result["intent"] == "query"
    assert result["metrics"] == ["DAU"]
    assert result["dimensions"]["market"] == "ID"


def test_parse_response_with_clarification():
    raw = json.dumps({
        "intent": "query",
        "metrics": [],
        "dimensions": {"market": None, "date_range": None, "compare_to": None, "module": None},
        "clarification_needed": "Did you mean Ads Gross Rev or Net Ads Rev?",
    })
    result = IntentExtractor.parse_response(raw)
    assert result["clarification_needed"] is not None
