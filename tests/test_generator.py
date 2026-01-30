import os
import tempfile
import yaml
from src.importer.generator import Generator


def test_generator_writes_yaml_and_snippet():
    analyzed = [{
        "name": "DAU",
        "aliases": ["daily active users"],
        "type": "simple",
        "table": "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
        "columns": {"value": "a1", "date": "grass_date", "region": "grass_region"},
        "filters": ["tz_type = 'local'"],
        "aggregation": "avg",
        "snippet": "SELECT avg(a1) AS dau FROM traffic.dau_table WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'",
        "notes": "",
    }]

    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")

        gen = Generator(metrics_dir=metrics_dir, snippets_dir=snippets_dir)
        gen.generate(analyzed)

        yaml_path = os.path.join(metrics_dir, "dau.yaml")
        assert os.path.exists(yaml_path)
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
        assert data["metric"]["name"] == "DAU"
        assert data["metric"]["type"] == "simple"

        snippet_path = os.path.join(snippets_dir, "dau.sql")
        assert os.path.exists(snippet_path)
        with open(snippet_path) as f:
            sql = f.read()
        assert "SELECT" in sql


def test_generator_skips_snippet_for_simple_without_snippet():
    analyzed = [{
        "name": "Test Metric",
        "aliases": [],
        "type": "simple",
        "table": "some.table",
        "columns": {"value": "val"},
        "filters": [],
        "aggregation": "sum",
        "notes": "test",
    }]

    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")

        gen = Generator(metrics_dir=metrics_dir, snippets_dir=snippets_dir)
        gen.generate(analyzed)

        yaml_path = os.path.join(metrics_dir, "test_metric.yaml")
        assert os.path.exists(yaml_path)

        snippet_path = os.path.join(snippets_dir, "test_metric.sql")
        assert not os.path.exists(snippet_path)
