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


def test_source_has_golden_and_snippet(tmp_path):
    yaml_content = """
metric:
  name: Test
  type: simple
  aggregation_template: avg_rollup
  sources:
    - id: test_src
      table: db.table
      golden: true
      snippet: snippets/layer1/test.sql
      columns: {value: v, date: d, region: r}
  dimensions:
    required: [market]
    optional: []
"""
    (tmp_path / "test.yaml").write_text(yaml_content)
    registry = MetricRegistry(metrics_dir=str(tmp_path))
    registry.load()
    assert registry.metrics[0].sources[0].golden is True
    assert registry.metrics[0].sources[0].snippet == "snippets/layer1/test.sql"
    assert registry.metrics[0].aggregation_template == "avg_rollup"


def test_derived_metric_has_composition(tmp_path):
    yaml_content = """
metric:
  name: Ratio
  type: derived
  formula: "a / b"
  depends_on: ["A", "B"]
  composition:
    template: ratio
    numerator: A
    denominator: B
  dimensions:
    required: [market]
    optional: []
"""
    (tmp_path / "ratio.yaml").write_text(yaml_content)
    registry = MetricRegistry(metrics_dir=str(tmp_path))
    registry.load()
    assert registry.metrics[0].composition["template"] == "ratio"
    assert registry.metrics[0].composition["numerator"] == "A"
    assert registry.metrics[0].composition["denominator"] == "B"


def test_load_all_sra_metrics():
    registry = MetricRegistry(METRICS_DIR)
    registry.load()
    expected_names = [
        "DAU", "Buyer UV", "GMV", "Ads Gross Rev", "Gross Take Rate",
        "Ads Direct ROI", "Net Ads Rev", "Net Take Rate",
        "Commission Fee", "Rebate", "Order Pct by Channel", "Ads Rev Pct by Channel",
    ]
    loaded_names = [m.name for m in registry.metrics]
    for name in expected_names:
        assert name in loaded_names, f"Missing metric: {name}"
