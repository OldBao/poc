import os
import yaml
import pytest
from unittest.mock import patch
from src.agent import Agent

TESTS_DIR = os.path.dirname(__file__)


def load_test_cases():
    path = os.path.join(TESTS_DIR, "test_cases.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


TEST_CASES = load_test_cases()


@pytest.mark.parametrize(
    "case",
    TEST_CASES,
    ids=[c["question"][:50] for c in TEST_CASES],
)
def test_regression(case):
    agent = Agent(metrics_dir="metrics", templates_dir="templates", snippets_dir="snippets")

    with patch("src.extractor.IntentExtractor.extract") as mock_extract:
        mock_extract.return_value = case["mock_extract"]
        result = agent.ask(case["question"])

    assert "sql" in result, f"Expected SQL output, got: {result}"
    sql = result["sql"]
    for fragment in case["expected_sql_contains"]:
        assert fragment in sql, f"Missing '{fragment}' in SQL:\n{sql}"
