import pytest
from unittest.mock import MagicMock, patch
from src.autotest.loader import BenchmarkCase
from src.autotest.comparator import CompareResult, ResultCompareResult
from src.autotest.repairer import RepairPlan, RepairAction
from src.autotest.runner import Runner, CaseResult, RunSummary


def _make_case(id="test_case", question="Q?", expected_sql="SELECT 1"):
    return BenchmarkCase(id=id, question=question, expected_sql=expected_sql, tags=[])


def test_runner_pass_on_structural_and_result_match():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT 1"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(match=True)

    result_comparator = MagicMock()
    result_comparator.compare.return_value = ResultCompareResult(match=True)

    query_service = MagicMock()
    from src.query_service import QueryResult
    query_service.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=3,
        max_llm_calls=50,
    )

    case = _make_case()
    result = runner.run_case(case)
    assert result.passed is True
    assert result.retries == 0


def test_runner_fails_after_max_retries():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT bad"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(
        match=False, differences=["Wrong table"]
    )

    result_comparator = MagicMock()
    query_service = MagicMock()
    repairer = MagicMock()
    repairer.propose.return_value = RepairPlan(
        actions=[RepairAction(type="edit_snippet", file="/tmp/x.sql", content="x")],
        reasoning="fix",
    )

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=2,
        max_llm_calls=50,
    )

    case = _make_case()
    result = runner.run_case(case)
    assert result.passed is False
    assert result.retries == 2


def test_runner_no_repair_mode():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT bad"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(
        match=False, differences=["Wrong table"]
    )

    result_comparator = MagicMock()
    query_service = MagicMock()
    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=3,
        max_llm_calls=50,
        no_repair=True,
    )

    case = _make_case()
    result = runner.run_case(case)
    assert result.passed is False
    assert result.retries == 0
    repairer.propose.assert_not_called()


def test_run_all_returns_summary():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT 1"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(match=True)

    result_comparator = MagicMock()
    result_comparator.compare.return_value = ResultCompareResult(match=True)

    query_service = MagicMock()
    from src.query_service import QueryResult
    query_service.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=3,
        max_llm_calls=50,
    )

    cases = [_make_case(id="a"), _make_case(id="b")]
    summary = runner.run_all(cases)
    assert isinstance(summary, RunSummary)
    assert summary.total == 2
    assert summary.passed == 2
    assert summary.failed == 0
