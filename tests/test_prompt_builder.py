import os
import tempfile
import yaml
from src.prompt_builder import PromptBuilder


def _make_metric_yaml(name, aliases, table, columns, filters, metric_type="simple", snippet_file=None, notes=None):
    m = {
        "metric": {
            "name": name,
            "aliases": aliases,
            "type": metric_type,
            "dimensions": {"required": ["market", "date_range"], "optional": []},
        }
    }
    if metric_type == "simple":
        m["metric"]["aggregation"] = "avg"
        m["metric"]["sources"] = [{
            "id": "src1",
            "layer": "dws",
            "table": table,
            "columns": columns,
            "filters": filters,
            "use_when": {"granularity": ["platform"]},
        }]
    if snippet_file:
        m["metric"]["snippet_file"] = snippet_file
    if notes:
        m["metric"]["notes"] = notes
    return m


def test_prompt_includes_metric_definitions():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)

        metric = _make_metric_yaml("DAU", ["daily active users"], "traffic.dau_table", {"value": "a1", "date": "grass_date", "region": "grass_region"}, ["tz_type = 'local'"])
        with open(os.path.join(metrics_dir, "dau.yaml"), "w") as f:
            yaml.dump(metric, f)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir)
        prompt = builder.build()

        assert "DAU" in prompt
        assert "daily active users" in prompt
        assert "traffic.dau_table" in prompt


def test_prompt_includes_sql_snippets():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)

        metric = _make_metric_yaml("Ads Gross Rev", ["ads revenue"], "", {}, [], metric_type="complex", snippet_file="snippets/ads_gross_rev.sql")
        with open(os.path.join(metrics_dir, "ads.yaml"), "w") as f:
            yaml.dump(metric, f)

        with open(os.path.join(snippets_dir, "ads_gross_rev.sql"), "w") as f:
            f.write("SELECT sum(ads_rev_usd) FROM mp_paidads.table WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'")

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir)
        prompt = builder.build()

        assert "Ads Gross Rev" in prompt
        assert "SELECT sum(ads_rev_usd)" in prompt


def test_prompt_includes_output_format():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir)
        prompt = builder.build()

        assert '"type"' in prompt
        assert '"sql"' in prompt
        assert '"ambiguous"' in prompt
        assert '"candidates"' in prompt
