"""Integration test: runs the full pipeline with mocked LLM and query service."""
import pytest
from unittest.mock import MagicMock
from src.autotest.loader import BenchmarkCase
from src.autotest.comparator import (
    StructuralComparator,
    ResultComparator,
    CompareResult,
    ResultCompareResult,
)
from src.autotest.repairer import Repairer, RepairPlan, RepairAction
from src.autotest.runner import Runner
from src.query_service import QueryResult


def test_full_pipeline_pass():
    """Case passes both gates on first try."""
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT 1"}

    structural = MagicMock()
    structural.compare.return_value = CompareResult(match=True)

    result_comp = MagicMock()
    result_comp.compare.return_value = ResultCompareResult(match=True)

    qs = MagicMock()
    qs.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=qs,
        repairer=repairer,
    )

    cases = [BenchmarkCase(id="t1", question="Q?", expected_sql="SELECT 1")]
    summary = runner.run_all(cases)

    assert summary.total == 1
    assert summary.passed == 1
    assert summary.failed == 0


def test_full_pipeline_repair_then_pass():
    """Case fails structural, repair fixes it, passes on retry."""
    agent = MagicMock()
    agent.ask.side_effect = [
        {"type": "sql", "sql": "SELECT bad"},
        {"type": "sql", "sql": "SELECT good"},
    ]

    structural = MagicMock()
    structural.compare.side_effect = [
        CompareResult(match=False, differences=["wrong table"]),
        CompareResult(match=True),
    ]

    result_comp = MagicMock()
    result_comp.compare.return_value = ResultCompareResult(match=True)

    qs = MagicMock()
    qs.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()
    repairer.propose.return_value = RepairPlan(
        actions=[RepairAction(type="edit_snippet", file="/tmp/x.sql", content="fixed")],
        reasoning="Fixed table name",
    )

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=qs,
        repairer=repairer,
    )

    cases = [BenchmarkCase(id="t1", question="Q?", expected_sql="SELECT good")]
    summary = runner.run_all(cases)

    assert summary.total == 1
    assert summary.repaired == 1
    assert summary.failed == 0


def test_full_pipeline_exhausts_retries():
    """Case fails all retries."""
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT bad"}

    structural = MagicMock()
    structural.compare.return_value = CompareResult(
        match=False, differences=["wrong"]
    )

    result_comp = MagicMock()
    qs = MagicMock()

    repairer = MagicMock()
    repairer.propose.return_value = RepairPlan(
        actions=[RepairAction(type="edit_snippet", file="/tmp/x.sql", content="x")],
        reasoning="attempt",
    )

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=qs,
        repairer=repairer,
        max_retries=2,
    )

    cases = [BenchmarkCase(id="t1", question="Q?", expected_sql="SELECT good")]
    summary = runner.run_all(cases)

    assert summary.total == 1
    assert summary.failed == 1
    assert summary.results[0].retries == 2
