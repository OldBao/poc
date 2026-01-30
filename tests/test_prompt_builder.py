import os
import tempfile
from datetime import date

import yaml
from src.models import AssemblyContext, JoinAdjustment, WrapAdjustment
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


def test_complex_sql_prompt_includes_snippet():
    builder = PromptBuilder(metrics_dir="metrics", snippets_dir="snippets")
    prompt = builder.build_complex_sql_prompt(
        snippet_sql="SELECT * FROM table WHERE x = 1",
        metric_name="Ads Gross Rev",
        dimension_values={"grass_region": ["ID", "TH", "VN"]},
    )
    assert "SELECT * FROM table" in prompt
    assert "Ads Gross Rev" in prompt
    assert "ID" in prompt
    assert "TH" in prompt


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


def test_prompt_has_conversation_instructions():
    pb = PromptBuilder(metrics_dir="metrics", snippets_dir="snippets")
    prompt = pb.build()
    assert "ask in plain text" in prompt.lower() or "ask the user" in prompt.lower()
    assert "need_info" not in prompt


def test_build_assembled_prompt_with_join():
    pb = PromptBuilder()
    ctx = AssemblyContext(
        base_snippet="SELECT * FROM base WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30'",
        joins=[JoinAdjustment(
            name="BR SCS Credit",
            snippet="SELECT grass_date, sum(free_rev) AS br_scs FROM t GROUP BY 1",
            join_keys=["grass_date", "grass_region"],
        )],
    )
    prompt = pb.build_assembled_prompt(ctx, metric_name="Net Ads Rev")
    assert "Base Query" in prompt
    assert "base WHERE" in prompt
    assert "Adjustments to Apply" in prompt
    assert "BR SCS Credit" in prompt
    assert "LEFT JOIN" in prompt
    assert "grass_date, grass_region" in prompt


def test_build_assembled_prompt_with_filter():
    pb = PromptBuilder()
    ctx = AssemblyContext(
        base_snippet="SELECT 1",
        filters=["AND seller_type != '1P'"],
    )
    prompt = pb.build_assembled_prompt(ctx, metric_name="Test")
    assert "Additional filter" in prompt or "filter" in prompt.lower()
    assert "seller_type" in prompt


def test_assembled_prompt_contains_today_date():
    pb = PromptBuilder()
    ctx = AssemblyContext(base_snippet="SELECT 1")
    prompt = pb.build_assembled_prompt(ctx, metric_name="Test")
    today = date.today()
    assert today.isoformat() in prompt or str(today.year) in prompt


def test_build_assembled_prompt_no_adjustments():
    pb = PromptBuilder()
    ctx = AssemblyContext(base_snippet="SELECT 1")
    prompt = pb.build_assembled_prompt(ctx, metric_name="Test")
    assert "Base Query" in prompt
    # Should not have adjustments section when there are none
    assert "Adjustments to Apply" not in prompt
