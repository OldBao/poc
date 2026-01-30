import os
import tempfile
from datetime import date

import yaml
from src.prompt_builder import PromptBuilder


def _make_metric_yaml(name, aliases, table, columns, filters, metric_type="simple", snippet_file=None, notes=None, tags=None):
    m = {
        "metric": {
            "name": name,
            "aliases": aliases,
            "type": metric_type,
            "dimensions": {"required": ["market", "date_range"], "optional": []},
        }
    }
    if tags:
        m["metric"]["tags"] = tags
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
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        metric = _make_metric_yaml("DAU", ["daily active users"], "traffic.dau_table", {"value": "a1", "date": "grass_date", "region": "grass_region"}, ["tz_type = 'local'"])
        with open(os.path.join(metrics_dir, "dau.yaml"), "w") as f:
            yaml.dump(metric, f)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir, rules_dir=rules_dir)
        prompt = builder.build()

        assert "DAU" in prompt
        assert "daily active users" in prompt
        assert "traffic.dau_table" in prompt


def test_prompt_includes_sql_snippets():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        metric = _make_metric_yaml("Ads Gross Rev", ["ads revenue"], "", {}, [], metric_type="complex", snippet_file="snippets/ads_gross_rev.sql")
        with open(os.path.join(metrics_dir, "ads.yaml"), "w") as f:
            yaml.dump(metric, f)

        with open(os.path.join(snippets_dir, "ads_gross_rev.sql"), "w") as f:
            f.write("SELECT sum(ads_rev_usd) FROM mp_paidads.table WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'")

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir, rules_dir=rules_dir)
        prompt = builder.build()

        assert "Ads Gross Rev" in prompt
        assert "SELECT sum(ads_rev_usd)" in prompt


def test_prompt_includes_output_format():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir, rules_dir=rules_dir)
        prompt = builder.build()

        assert '"type"' in prompt
        assert '"sql"' in prompt
        assert '"ambiguous"' in prompt
        assert '"candidates"' in prompt


def test_prompt_has_conversation_instructions():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir, rules_dir=rules_dir)
        prompt = builder.build()

        assert "CONTEXT CARRY-OVER" in prompt
        assert "sql_list" in prompt
        assert "MISSING DIMENSIONS" in prompt


def test_prompt_contains_today_date():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir, rules_dir=rules_dir)
        prompt = builder.build()
        today = date.today()
        assert today.isoformat() in prompt


def test_prompt_includes_rules():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        # Create a rule with inline snippet
        adj_dir = os.path.join(snippets_dir, "adjustments")
        os.makedirs(adj_dir)
        snippet_path = os.path.join(adj_dir, "br_scs.sql")
        with open(snippet_path, "w") as f:
            f.write("SELECT grass_date, sum(free_rev) AS br_scs FROM t GROUP BY 1")

        rule = {
            "rule": {
                "name": "BR SCS Credit",
                "description": "BR market adjustment",
                "when": {"market": "BR", "metric_tags": ["revenue", "net"]},
                "effect": {
                    "type": "left_join",
                    "snippet_file": snippet_path,
                    "join_keys": ["grass_date", "grass_region"],
                },
                "valid_from": "2025-01-01",
            }
        }
        with open(os.path.join(rules_dir, "br_scs.yaml"), "w") as f:
            yaml.dump(rule, f)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir, rules_dir=rules_dir)
        prompt = builder.build()

        assert "BR SCS Credit" in prompt
        assert "market = BR" in prompt
        assert "left_join" in prompt
        assert "br_scs" in prompt


def test_prompt_includes_metric_tags():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        metric = _make_metric_yaml(
            "Net Ads Rev", ["net ads revenue"], "", {}, [],
            metric_type="complex", tags=["revenue", "net", "ads"],
        )
        with open(os.path.join(metrics_dir, "net_ads_rev.yaml"), "w") as f:
            yaml.dump(metric, f)

        builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir, rules_dir=rules_dir)
        prompt = builder.build()

        assert "Tags: revenue, net, ads" in prompt
