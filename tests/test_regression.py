"""
Regression test suite for LLM-based SQL generation.
Runs against live OpenAI API. Mark with pytest -m live.
"""
import os
import pytest
import yaml
from src.agent import Agent, parse_response

TESTS_DIR = os.path.dirname(__file__)


def load_test_cases():
    path = os.path.join(TESTS_DIR, "test_cases.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


test_cases = load_test_cases()


@pytest.mark.live
@pytest.mark.parametrize(
    "case",
    test_cases,
    ids=[c["question"][:50] for c in test_cases],
)
def test_regression(case):
    agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
    raw = agent.start(case["question"])
    rtype, data = parse_response(raw)
    result = data if isinstance(data, dict) else {"type": "text", "message": data}

    expected_type = case["expected_type"]
    assert result.get("type") == expected_type, (
        f"Expected type '{expected_type}', got '{result.get('type')}'. "
        f"Full response: {result}"
    )

    if expected_type == "sql":
        sql = result["sql"]
        for fragment in case.get("expected_sql_contains", []):
            assert fragment in sql, (
                f"Expected SQL to contain '{fragment}'.\n"
                f"Got SQL:\n{sql}"
            )
        for fragment in case.get("expected_sql_not_contains", []):
            assert fragment not in sql, (
                f"Expected SQL to NOT contain '{fragment}'.\n"
                f"Got SQL:\n{sql}"
            )

    elif expected_type == "ambiguous":
        candidates = " ".join(result.get("candidates", []))
        for keyword in case.get("expected_candidates_contain", []):
            assert keyword.lower() in candidates.lower(), (
                f"Expected candidates to mention '{keyword}'.\n"
                f"Got: {result.get('candidates')}"
            )
