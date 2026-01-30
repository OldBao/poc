import pytest
from src.resolver import Resolver, ResolverResult
from src.registry import MetricRegistry
from src.value_index import ValueIndex
from src.extractor import ExtractionResult


@pytest.fixture
def registry():
    reg = MetricRegistry(metrics_dir="metrics")
    reg.load()
    return reg


@pytest.fixture
def value_index(tmp_path):
    idx = ValueIndex(str(tmp_path / "test.db"))
    idx.init_db()
    idx.upsert(
        "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
        "grass_region",
        [("ID", 1000), ("TH", 500), ("VN", 300)],
    )
    return idx


def test_resolve_simple_metric(registry, value_index):
    resolver = Resolver(registry=registry, value_index=value_index)
    extraction = ExtractionResult(
        intent="query",
        metrics=["dau"],
        dimensions={"market": "ID", "date_range": {"start": "2025-11-01", "end": "2025-11-30"}},
    )
    result = resolver.resolve(extraction)
    assert result.metric.name == "DAU"
    assert result.metric.type == "simple"
    assert result.source.table == "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live"
    assert result.errors == []


def test_resolve_unknown_metric(registry, value_index):
    resolver = Resolver(registry=registry, value_index=value_index)
    extraction = ExtractionResult(
        intent="query",
        metrics=["nonexistent_metric"],
        dimensions={"market": "ID", "date_range": {"start": "2025-11-01", "end": "2025-11-30"}},
    )
    result = resolver.resolve(extraction)
    assert len(result.errors) > 0
    assert "not found" in result.errors[0].lower()


def test_resolve_invalid_market(registry, value_index):
    resolver = Resolver(registry=registry, value_index=value_index)
    extraction = ExtractionResult(
        intent="query",
        metrics=["dau"],
        dimensions={"market": "XX", "date_range": {"start": "2025-11-01", "end": "2025-11-30"}},
    )
    result = resolver.resolve(extraction)
    assert len(result.errors) > 0
    assert "XX" in result.errors[0]
