from unittest.mock import MagicMock
from src.extractor import Extractor


def test_parse_simple_query():
    mock_llm = MagicMock()
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": ["dau"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
        },
        "clarification_needed": None,
    }
    extractor = Extractor(llm_client=mock_llm, metric_names=["DAU", "GMV"])
    result = extractor.extract("ID market DAU in November 2025")
    assert result.intent == "query"
    assert result.metrics == ["dau"]
    assert result.dimensions["market"] == "ID"
    assert result.clarification_needed is None


def test_parse_compare_query():
    mock_llm = MagicMock()
    mock_llm.call.return_value = {
        "intent": "compare",
        "metrics": ["dau"],
        "dimensions": {
            "market": "ID",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "compare_to": {"start": "2025-10-01", "end": "2025-10-31"},
        },
        "clarification_needed": None,
    }
    extractor = Extractor(llm_client=mock_llm, metric_names=["DAU"])
    result = extractor.extract("Compare ID DAU between Oct and Nov 2025")
    assert result.intent == "compare"
    assert result.dimensions["compare_to"]["start"] == "2025-10-01"


def test_ambiguous_query():
    mock_llm = MagicMock()
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": [],
        "dimensions": {},
        "clarification_needed": "Did you mean Ads Gross Rev or Net Ads Rev?",
    }
    extractor = Extractor(llm_client=mock_llm, metric_names=["Ads Gross Rev", "Net Ads Rev"])
    result = extractor.extract("What's the revenue?")
    assert result.clarification_needed is not None


def test_extraction_prompt_contains_today_date():
    mock_llm = MagicMock()
    mock_llm.call.return_value = {
        "intent": "query",
        "metrics": [],
        "dimensions": {},
        "clarification_needed": None,
    }
    extractor = Extractor(llm_client=mock_llm, metric_names=["DAU"])
    # The system prompt should contain today's date info
    assert "today" in extractor.system_prompt.lower() or "current date" in extractor.system_prompt.lower()
    # Should contain the current year
    from datetime import date
    assert str(date.today().year) in extractor.system_prompt
