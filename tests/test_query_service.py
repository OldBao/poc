import pytest
from src.query_service import QueryService, QueryResult


def test_execute_returns_query_result():
    """QueryService.execute returns a QueryResult with columns and rows."""
    qs = QueryService(base_url="http://fake", token="fake")
    result = qs.execute("SELECT 1 AS n", limit=10)
    assert isinstance(result, QueryResult)
    assert result.columns is not None
    assert result.rows is not None


def test_query_result_schema():
    qr = QueryResult(
        columns=["region", "dau"],
        rows=[["ID", 1000], ["TH", 2000]],
        error=None,
    )
    assert qr.columns == ["region", "dau"]
    assert len(qr.rows) == 2
    assert qr.error is None


def test_query_result_error():
    qr = QueryResult(columns=[], rows=[], error="Syntax error at line 1")
    assert qr.error == "Syntax error at line 1"
    assert qr.has_error


def test_limit_wrapping():
    qs = QueryService(base_url="http://fake", token="fake")
    wrapped = qs._wrap_with_limit("SELECT * FROM t", limit=100)
    assert "LIMIT 100" in wrapped
