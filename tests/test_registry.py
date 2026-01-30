import os
import pytest
from src.registry import MetricRegistry

METRICS_DIR = os.path.join(os.path.dirname(__file__), "..", "metrics")


def test_load_all_metrics():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    assert len(registry.metrics) >= 1
    assert "DAU" in [m.name for m in registry.metrics]


def test_find_by_name():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("DAU")
    assert result is not None
    assert result.name == "DAU"


def test_find_by_alias():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("daily active users")
    assert result is not None
    assert result.name == "DAU"


def test_find_case_insensitive():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("dau")
    assert result is not None
    assert result.name == "DAU"


def test_find_not_found():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    result = registry.find("nonexistent metric")
    assert result is None


def test_list_metric_names():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    names = registry.list_names_and_aliases()
    assert any("DAU" in entry for entry in names)
