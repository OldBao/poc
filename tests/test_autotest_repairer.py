import json
import os
import pytest
from unittest.mock import MagicMock
from src.autotest.repairer import Repairer, RepairAction, RepairPlan


def _make_llm(actions):
    llm = MagicMock()
    llm.call.return_value = {
        "actions": actions,
        "reasoning": "Test reasoning",
    }
    return llm


def test_repairer_proposes_edit_snippet(tmp_path):
    snippet_dir = tmp_path / "snippets"
    snippet_dir.mkdir()
    (snippet_dir / "dau.sql").write_text("SELECT old FROM t")

    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()

    llm = _make_llm([{
        "type": "edit_snippet",
        "file": str(snippet_dir / "dau.sql"),
        "content": "SELECT new FROM t WHERE tz_type = 'local'",
    }])

    repairer = Repairer(
        llm_client=llm,
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippet_dir),
    )

    plan = repairer.propose(
        question="ID DAU",
        expected_sql="SELECT new FROM t WHERE tz_type = 'local'",
        generated_sql="SELECT old FROM t",
        failure_context="Missing tz_type filter",
    )

    assert len(plan.actions) == 1
    assert plan.actions[0].type == "edit_snippet"
    assert plan.reasoning == "Test reasoning"


def test_repairer_apply_writes_files(tmp_path):
    snippet_dir = tmp_path / "snippets"
    snippet_dir.mkdir()
    (snippet_dir / "dau.sql").write_text("old content")

    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()

    llm = MagicMock()
    repairer = Repairer(
        llm_client=llm,
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippet_dir),
    )

    plan = RepairPlan(
        actions=[
            RepairAction(
                type="edit_snippet",
                file=str(snippet_dir / "dau.sql"),
                content="new content",
            ),
        ],
        reasoning="Fix filter",
    )

    repairer.apply(plan)
    assert (snippet_dir / "dau.sql").read_text() == "new content"


def test_repairer_apply_creates_new_files(tmp_path):
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    snippets_dir = tmp_path / "snippets"
    snippets_dir.mkdir()

    llm = MagicMock()
    repairer = Repairer(
        llm_client=llm,
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippets_dir),
    )

    new_file = str(metrics_dir / "new_metric.yaml")
    plan = RepairPlan(
        actions=[
            RepairAction(type="create_metric", file=new_file, content="metric: new"),
        ],
        reasoning="Add missing metric",
    )

    repairer.apply(plan)
    assert os.path.exists(new_file)
    assert open(new_file).read() == "metric: new"


def test_repairer_revert(tmp_path):
    snippet_dir = tmp_path / "snippets"
    snippet_dir.mkdir()
    (snippet_dir / "dau.sql").write_text("original")

    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()

    llm = MagicMock()
    repairer = Repairer(
        llm_client=llm,
        metrics_dir=str(metrics_dir),
        snippets_dir=str(snippet_dir),
    )

    plan = RepairPlan(
        actions=[
            RepairAction(
                type="edit_snippet",
                file=str(snippet_dir / "dau.sql"),
                content="modified",
            ),
        ],
        reasoning="Fix",
    )

    repairer.apply(plan)
    assert (snippet_dir / "dau.sql").read_text() == "modified"

    repairer.revert(plan)
    assert (snippet_dir / "dau.sql").read_text() == "original"
