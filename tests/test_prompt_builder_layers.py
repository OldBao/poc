import os
import pytest
from src.prompt_builder import PromptBuilder


def test_build_includes_layer_sections(tmp_path):
    """Prompt includes separate sections for Layer 1, Layer 2, Layer 3."""
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    snippets_dir = tmp_path / "snippets"
    (snippets_dir / "layer1").mkdir(parents=True)
    (snippets_dir / "layer2").mkdir(parents=True)
    (snippets_dir / "layer3").mkdir(parents=True)

    (snippets_dir / "layer1" / "dau.sql").write_text("SELECT a1 FROM t")
    (snippets_dir / "layer2" / "avg_rollup.sql").write_text("SELECT avg({{ value_expr }})")
    (snippets_dir / "layer3" / "ratio.sql").write_text("WITH a AS (...)")

    builder = PromptBuilder(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        rules_dir=str(tmp_path / "rules"),
    )
    prompt = builder.build()

    assert "## Source Fragments (Layer 1)" in prompt
    assert "## Aggregation Templates (Layer 2)" in prompt
    assert "## Composition Templates (Layer 3)" in prompt
    assert "SELECT a1 FROM t" in prompt
    assert "avg({{ value_expr }})" in prompt


def test_build_falls_back_to_flat_snippets(tmp_path):
    """If no layer subdirs exist, falls back to flat snippet listing."""
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    snippets_dir = tmp_path / "snippets"
    snippets_dir.mkdir()
    (snippets_dir / "test.sql").write_text("SELECT 1")

    builder = PromptBuilder(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        rules_dir=str(tmp_path / "rules"),
    )
    prompt = builder.build()

    assert "Reference SQL Examples" in prompt
    assert "SELECT 1" in prompt


def test_build_includes_adjustments(tmp_path):
    """Adjustments directory is included when layered structure exists."""
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    snippets_dir = tmp_path / "snippets"
    (snippets_dir / "layer1").mkdir(parents=True)
    (snippets_dir / "adjustments").mkdir(parents=True)

    (snippets_dir / "layer1" / "dau.sql").write_text("SELECT 1")
    (snippets_dir / "adjustments" / "br_scs_credit.sql").write_text("SELECT credit FROM adj")

    builder = PromptBuilder(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        rules_dir=str(tmp_path / "rules"),
    )
    prompt = builder.build()

    assert "## Adjustment Snippets" in prompt
    assert "SELECT credit FROM adj" in prompt


def test_format_metric_shows_aggregation_template_and_composition(tmp_path):
    """Metric formatting includes aggregation_template and composition."""
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    (metrics_dir / "rate.yaml").write_text("""
metric:
  name: Test Rate
  type: derived
  formula: "a / b"
  depends_on: ["A", "B"]
  aggregation_template: avg_rollup
  composition:
    template: ratio
    numerator: A
    denominator: B
  dimensions:
    required: [market]
    optional: []
""")
    snippets_dir = tmp_path / "snippets"
    snippets_dir.mkdir()

    builder = PromptBuilder(
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
        rules_dir=str(tmp_path / "rules"),
    )
    prompt = builder.build()

    assert "Aggregation template: avg_rollup" in prompt
    assert "Composition: template=ratio" in prompt
    assert "Numerator: A" in prompt
    assert "Denominator: B" in prompt
