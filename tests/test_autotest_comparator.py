import json
import pytest
from unittest.mock import MagicMock
from src.autotest.comparator import StructuralComparator, CompareResult


def _make_llm(match: bool, differences: list[str] = None):
    llm = MagicMock()
    llm.call.return_value = {
        "match": match,
        "differences": differences or [],
    }
    return llm


def test_matching_sqls():
    llm = _make_llm(match=True)
    comp = StructuralComparator(llm_client=llm)
    result = comp.compare(
        expected_sql="SELECT avg(a1) FROM t WHERE region='ID'",
        generated_sql="SELECT avg(a1) FROM t WHERE region = 'ID'",
    )
    assert isinstance(result, CompareResult)
    assert result.match is True
    assert result.differences == []


def test_mismatching_sqls():
    llm = _make_llm(match=False, differences=["Missing tz_type filter"])
    comp = StructuralComparator(llm_client=llm)
    result = comp.compare(
        expected_sql="SELECT avg(a1) FROM t WHERE tz_type='local'",
        generated_sql="SELECT avg(a1) FROM t",
    )
    assert result.match is False
    assert "Missing tz_type filter" in result.differences


def test_compare_prompt_includes_both_sqls():
    llm = _make_llm(match=True)
    comp = StructuralComparator(llm_client=llm)
    comp.compare(
        expected_sql="SELECT 1",
        generated_sql="SELECT 2",
    )
    call_args = llm.call.call_args
    system_prompt = call_args[1]["system_prompt"] if "system_prompt" in call_args[1] else call_args[0][0]
    user_message = call_args[1]["user_message"] if "user_message" in call_args[1] else call_args[0][1]
    assert "SELECT 1" in user_message
    assert "SELECT 2" in user_message


from src.autotest.comparator import ResultComparator, ResultCompareResult
from src.query_service import QueryResult


def test_result_match():
    comp = ResultComparator()
    exp = QueryResult(columns=["region", "dau"], rows=[["ID", 1000], ["TH", 2000]])
    gen = QueryResult(columns=["region", "dau"], rows=[["TH", 2000], ["ID", 1000]])
    result = comp.compare(exp, gen)
    assert result.match is True


def test_result_schema_mismatch():
    comp = ResultComparator()
    exp = QueryResult(columns=["region", "dau"], rows=[])
    gen = QueryResult(columns=["region", "users"], rows=[])
    result = comp.compare(exp, gen)
    assert result.match is False
    assert len(result.schema_diff) > 0


def test_result_row_mismatch():
    comp = ResultComparator()
    exp = QueryResult(columns=["n"], rows=[[100]])
    gen = QueryResult(columns=["n"], rows=[[200]])
    result = comp.compare(exp, gen)
    assert result.match is False
    assert result.row_mismatches == 1


def test_result_numeric_tolerance():
    comp = ResultComparator(tolerance=0.01)
    exp = QueryResult(columns=["rate"], rows=[[1.0000]])
    gen = QueryResult(columns=["rate"], rows=[[1.005]])
    result = comp.compare(exp, gen)
    assert result.match is True


def test_result_error_handling():
    comp = ResultComparator()
    exp = QueryResult(columns=[], rows=[])
    gen = QueryResult(columns=[], rows=[], error="Syntax error")
    result = comp.compare(exp, gen)
    assert result.match is False
