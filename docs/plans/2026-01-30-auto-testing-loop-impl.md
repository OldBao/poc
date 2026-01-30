# Auto-Testing Loop Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a self-improving test loop that evaluates the SQL agent against benchmark cases, compares structural + result validity, and auto-repairs the KB on failure.

**Architecture:** Three-phase pipeline (structural compare → result validate → repair loop) orchestrated by a CLI runner. LLM-based structural comparison, query execution via Shopee internal API, and LLM-driven KB repair with retry caps and regression protection.

**Tech Stack:** Python, pytest, PyYAML, openai, existing `src/` modules (Agent, LLMClient, PromptBuilder)

---

### Task 1: Benchmark Loader

**Files:**
- Create: `src/autotest/__init__.py`
- Create: `src/autotest/loader.py`
- Create: `tests/test_autotest_loader.py`
- Create: `tests/benchmark.yaml` (seed with 2 sample cases)

**Step 1: Write the failing test**

```python
# tests/test_autotest_loader.py
import pytest
from src.autotest.loader import BenchmarkLoader, BenchmarkCase


def test_load_cases_from_yaml(tmp_path):
    yaml_content = """
cases:
  - id: dau_id_monthly
    question: "ID market DAU in November 2025"
    expected_sql: |
      SELECT grass_region, avg(a1) AS dau
      FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30'
      GROUP BY 1
    tags: [traffic, dau]
  - id: gmv_th
    question: "TH GMV in December 2025"
    expected_sql: |
      SELECT sum(gmv) FROM orders WHERE market = 'TH'
    tags: [commerce, gmv]
"""
    f = tmp_path / "benchmark.yaml"
    f.write_text(yaml_content)

    loader = BenchmarkLoader(str(f))
    cases = loader.load()

    assert len(cases) == 2
    assert cases[0].id == "dau_id_monthly"
    assert cases[0].question == "ID market DAU in November 2025"
    assert "avg(a1)" in cases[0].expected_sql
    assert cases[0].tags == ["traffic", "dau"]


def test_filter_by_tags(tmp_path):
    yaml_content = """
cases:
  - id: case_a
    question: "Q A"
    expected_sql: "SELECT 1"
    tags: [ads]
  - id: case_b
    question: "Q B"
    expected_sql: "SELECT 2"
    tags: [traffic]
"""
    f = tmp_path / "benchmark.yaml"
    f.write_text(yaml_content)

    loader = BenchmarkLoader(str(f))
    cases = loader.load(tags=["ads"])
    assert len(cases) == 1
    assert cases[0].id == "case_a"


def test_filter_by_id(tmp_path):
    yaml_content = """
cases:
  - id: case_a
    question: "Q A"
    expected_sql: "SELECT 1"
    tags: []
  - id: case_b
    question: "Q B"
    expected_sql: "SELECT 2"
    tags: []
"""
    f = tmp_path / "benchmark.yaml"
    f.write_text(yaml_content)

    loader = BenchmarkLoader(str(f))
    cases = loader.load(case_id="case_b")
    assert len(cases) == 1
    assert cases[0].id == "case_b"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_autotest_loader.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.autotest'"

**Step 3: Write minimal implementation**

```python
# src/autotest/__init__.py
# (empty)
```

```python
# src/autotest/loader.py
from dataclasses import dataclass, field
from typing import Optional
import yaml


@dataclass
class BenchmarkCase:
    id: str
    question: str
    expected_sql: str
    tags: list[str] = field(default_factory=list)


class BenchmarkLoader:
    def __init__(self, path: str = "tests/benchmark.yaml"):
        self.path = path

    def load(
        self,
        tags: Optional[list[str]] = None,
        case_id: Optional[str] = None,
    ) -> list[BenchmarkCase]:
        with open(self.path) as f:
            data = yaml.safe_load(f)

        cases = [
            BenchmarkCase(
                id=c["id"],
                question=c["question"],
                expected_sql=c["expected_sql"].strip(),
                tags=c.get("tags", []),
            )
            for c in data["cases"]
        ]

        if case_id:
            cases = [c for c in cases if c.id == case_id]
        if tags:
            cases = [c for c in cases if set(tags) & set(c.tags)]

        return cases
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_autotest_loader.py -v`
Expected: PASS (3 tests)

**Step 5: Create seed benchmark file**

```yaml
# tests/benchmark.yaml
cases:
  - id: dau_id_monthly
    question: "ID market DAU in November 2025"
    expected_sql: |
      SELECT grass_region, substr(cast(grass_date as varchar), 1, 7) AS period, avg(a1) AS dau
      FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30'
        AND tz_type = 'local' AND grass_region = 'ID'
      GROUP BY 1, 2 ORDER BY 2 DESC
    tags: [traffic, dau]

  - id: gmv_th_monthly
    question: "TH GMV in December 2025"
    expected_sql: |
      SELECT grass_region, substr(cast(grass_date as varchar), 1, 7) AS period, sum(gmv) AS gmv
      FROM commerce.shopee_commerce_dws_order_metrics_nd__reg_s0_live
      WHERE grass_date BETWEEN date '2025-12-01' AND date '2025-12-31'
        AND tz_type = 'local' AND grass_region = 'TH'
      GROUP BY 1, 2 ORDER BY 2 DESC
    tags: [commerce, gmv]
```

**Step 6: Commit**

```bash
git add src/autotest/__init__.py src/autotest/loader.py tests/test_autotest_loader.py tests/benchmark.yaml
git commit -m "feat(autotest): add benchmark loader with filtering"
```

---

### Task 2: Query Service Stub

**Files:**
- Create: `src/query_service.py`
- Create: `tests/test_query_service.py`

**Step 1: Write the failing test**

```python
# tests/test_query_service.py
import pytest
from src.query_service import QueryService, QueryResult


def test_execute_returns_query_result():
    """QueryService.execute returns a QueryResult with columns and rows."""
    qs = QueryService(base_url="http://fake", token="fake")
    # Stub: should return a QueryResult even without real service
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_query_service.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.query_service'"

**Step 3: Write minimal implementation**

```python
# src/query_service.py
from dataclasses import dataclass
from typing import Optional


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list]
    error: Optional[str] = None

    @property
    def has_error(self) -> bool:
        return self.error is not None


class QueryService:
    """Thin client for Shopee's internal query service.

    TODO: Replace stub implementation with real API calls.
    """

    def __init__(self, base_url: str = "", token: str = ""):
        self.base_url = base_url
        self.token = token

    def execute(self, sql: str, limit: int = 100) -> QueryResult:
        wrapped = self._wrap_with_limit(sql, limit)
        # STUB: return empty result. Replace with real API call.
        return QueryResult(columns=[], rows=[], error=None)

    def _wrap_with_limit(self, sql: str, limit: int) -> str:
        sql = sql.strip().rstrip(";")
        return f"{sql}\nLIMIT {limit}"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_query_service.py -v`
Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add src/query_service.py tests/test_query_service.py
git commit -m "feat(autotest): add query service stub with QueryResult model"
```

---

### Task 3: Structural Comparator

**Files:**
- Create: `src/autotest/comparator.py`
- Create: `tests/test_autotest_comparator.py`

**Step 1: Write the failing test**

```python
# tests/test_autotest_comparator.py
import json
import pytest
from unittest.mock import MagicMock
from src.autotest.comparator import StructuralComparator, CompareResult


def _make_llm(match: bool, differences: list[str] = None):
    llm = MagicMock()
    llm.call.return_value = {
        "match": match,
        "differences": differences or [],
    }
    return llm


def test_matching_sqls():
    llm = _make_llm(match=True)
    comp = StructuralComparator(llm_client=llm)
    result = comp.compare(
        expected_sql="SELECT avg(a1) FROM t WHERE region='ID'",
        generated_sql="SELECT avg(a1) FROM t WHERE region = 'ID'",
    )
    assert isinstance(result, CompareResult)
    assert result.match is True
    assert result.differences == []


def test_mismatching_sqls():
    llm = _make_llm(match=False, differences=["Missing tz_type filter"])
    comp = StructuralComparator(llm_client=llm)
    result = comp.compare(
        expected_sql="SELECT avg(a1) FROM t WHERE tz_type='local'",
        generated_sql="SELECT avg(a1) FROM t",
    )
    assert result.match is False
    assert "Missing tz_type filter" in result.differences


def test_compare_prompt_includes_both_sqls():
    llm = _make_llm(match=True)
    comp = StructuralComparator(llm_client=llm)
    comp.compare(
        expected_sql="SELECT 1",
        generated_sql="SELECT 2",
    )
    call_args = llm.call.call_args
    system_prompt = call_args[1]["system_prompt"] if "system_prompt" in call_args[1] else call_args[0][0]
    user_message = call_args[1]["user_message"] if "user_message" in call_args[1] else call_args[0][1]
    assert "SELECT 1" in user_message
    assert "SELECT 2" in user_message
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_autotest_comparator.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Write minimal implementation**

```python
# src/autotest/comparator.py
from dataclasses import dataclass, field
from typing import Optional
from src.llm_client import LLMClient
from src.query_service import QueryResult


@dataclass
class CompareResult:
    match: bool
    differences: list[str] = field(default_factory=list)


STRUCTURAL_COMPARE_PROMPT = """You are an expert SQL analyst. Compare two Presto SQL queries structurally.

Extract and compare these components:
- Tables referenced
- Join conditions
- WHERE filters (date ranges, region, tz_type, etc.)
- Aggregation functions and columns
- GROUP BY / ORDER BY clauses

Two queries are structurally equivalent if they would produce the same result, even if:
- Column aliases differ
- Formatting/whitespace differs
- Equivalent date functions are used (e.g., substr(cast(...)) vs date_format)
- Column order differs

Respond with ONLY this JSON:
{
  "match": true/false,
  "differences": ["difference 1", "difference 2"]
}

If match is true, differences should be an empty list.
"""


class StructuralComparator:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client

    def compare(self, expected_sql: str, generated_sql: str) -> CompareResult:
        user_message = (
            f"## Expected SQL\n```sql\n{expected_sql}\n```\n\n"
            f"## Generated SQL\n```sql\n{generated_sql}\n```"
        )
        result = self.llm.call(
            system_prompt=STRUCTURAL_COMPARE_PROMPT,
            user_message=user_message,
        )
        return CompareResult(
            match=result.get("match", False),
            differences=result.get("differences", []),
        )


@dataclass
class ResultCompareResult:
    match: bool
    schema_diff: list[str] = field(default_factory=list)
    row_mismatches: int = 0
    sample_diffs: list[str] = field(default_factory=list)


class ResultComparator:
    def __init__(self, tolerance: float = 0.0001):
        self.tolerance = tolerance

    def compare(self, expected: QueryResult, generated: QueryResult) -> ResultCompareResult:
        if expected.has_error:
            return ResultCompareResult(
                match=False,
                schema_diff=[f"Expected SQL error: {expected.error}"],
            )
        if generated.has_error:
            return ResultCompareResult(
                match=False,
                schema_diff=[f"Generated SQL error: {generated.error}"],
            )

        # Schema check (order-insensitive)
        exp_cols = sorted(expected.columns)
        gen_cols = sorted(generated.columns)
        if exp_cols != gen_cols:
            return ResultCompareResult(
                match=False,
                schema_diff=[f"Expected columns: {exp_cols}, Got: {gen_cols}"],
            )

        # Data check: sort rows and compare
        exp_rows = sorted(expected.rows, key=str)
        gen_rows = sorted(generated.rows, key=str)

        mismatches = 0
        diffs = []
        max_rows = max(len(exp_rows), len(gen_rows))
        for i in range(max_rows):
            if i >= len(exp_rows):
                mismatches += 1
                diffs.append(f"Row {i}: missing in expected")
                continue
            if i >= len(gen_rows):
                mismatches += 1
                diffs.append(f"Row {i}: missing in generated")
                continue
            if not self._rows_equal(exp_rows[i], gen_rows[i]):
                mismatches += 1
                if len(diffs) < 5:  # sample up to 5
                    diffs.append(f"Row {i}: expected {exp_rows[i]}, got {gen_rows[i]}")

        return ResultCompareResult(
            match=mismatches == 0,
            row_mismatches=mismatches,
            sample_diffs=diffs,
        )

    def _rows_equal(self, row_a: list, row_b: list) -> bool:
        if len(row_a) != len(row_b):
            return False
        for a, b in zip(row_a, row_b):
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if abs(a) < 1e-9 and abs(b) < 1e-9:
                    continue
                if abs(a - b) / max(abs(a), abs(b)) > self.tolerance:
                    return False
            elif a != b:
                return False
        return True
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_autotest_comparator.py -v`
Expected: PASS (3 tests)

**Step 5: Add result comparator tests**

```python
# append to tests/test_autotest_comparator.py

from src.autotest.comparator import ResultComparator, ResultCompareResult
from src.query_service import QueryResult


def test_result_match():
    comp = ResultComparator()
    exp = QueryResult(columns=["region", "dau"], rows=[["ID", 1000], ["TH", 2000]])
    gen = QueryResult(columns=["region", "dau"], rows=[["TH", 2000], ["ID", 1000]])
    result = comp.compare(exp, gen)
    assert result.match is True


def test_result_schema_mismatch():
    comp = ResultComparator()
    exp = QueryResult(columns=["region", "dau"], rows=[])
    gen = QueryResult(columns=["region", "users"], rows=[])
    result = comp.compare(exp, gen)
    assert result.match is False
    assert len(result.schema_diff) > 0


def test_result_row_mismatch():
    comp = ResultComparator()
    exp = QueryResult(columns=["n"], rows=[[100]])
    gen = QueryResult(columns=["n"], rows=[[200]])
    result = comp.compare(exp, gen)
    assert result.match is False
    assert result.row_mismatches == 1


def test_result_numeric_tolerance():
    comp = ResultComparator(tolerance=0.01)
    exp = QueryResult(columns=["rate"], rows=[[1.0000]])
    gen = QueryResult(columns=["rate"], rows=[[1.005]])
    result = comp.compare(exp, gen)
    assert result.match is True


def test_result_error_handling():
    comp = ResultComparator()
    exp = QueryResult(columns=[], rows=[])
    gen = QueryResult(columns=[], rows=[], error="Syntax error")
    result = comp.compare(exp, gen)
    assert result.match is False
```

**Step 6: Run all comparator tests**

Run: `pytest tests/test_autotest_comparator.py -v`
Expected: PASS (8 tests)

**Step 7: Commit**

```bash
git add src/autotest/comparator.py tests/test_autotest_comparator.py
git commit -m "feat(autotest): add structural and result comparators"
```

---

### Task 4: Repairer

**Files:**
- Create: `src/autotest/repairer.py`
- Create: `tests/test_autotest_repairer.py`

**Step 1: Write the failing test**

```python
# tests/test_autotest_repairer.py
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_autotest_repairer.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Write minimal implementation**

```python
# src/autotest/repairer.py
import os
from dataclasses import dataclass, field
from typing import Optional
from src.llm_client import LLMClient


@dataclass
class RepairAction:
    type: str  # edit_snippet, edit_metric, create_metric, create_snippet
    file: str
    content: str
    _original: Optional[str] = field(default=None, repr=False)


@dataclass
class RepairPlan:
    actions: list[RepairAction]
    reasoning: str


REPAIR_PROMPT = """You are an expert at debugging SQL generation systems.

The system uses a knowledge base of YAML metric definitions and SQL snippet files to generate SQL from natural language questions. A test case has failed.

Your job: propose file edits/creations to fix the knowledge base so the system generates correct SQL.

## Current KB Files

### Metrics
{metrics_listing}

### Snippets
{snippets_listing}

## Failure Context

Question: {question}
Expected SQL:
```sql
{expected_sql}
```

Generated SQL:
```sql
{generated_sql}
```

Failure: {failure_context}

## Instructions

Respond with ONLY this JSON:
{{
  "actions": [
    {{"type": "edit_snippet|edit_metric|create_snippet|create_metric", "file": "exact/path", "content": "full file content"}}
  ],
  "reasoning": "Brief explanation of what was wrong and how the fix addresses it"
}}

Keep changes minimal. Only modify what's necessary to fix this specific failure.
"""


class Repairer:
    def __init__(
        self,
        llm_client: LLMClient,
        metrics_dir: str = "metrics",
        snippets_dir: str = "snippets",
    ):
        self.llm = llm_client
        self.metrics_dir = metrics_dir
        self.snippets_dir = snippets_dir

    def propose(
        self,
        question: str,
        expected_sql: str,
        generated_sql: str,
        failure_context: str,
    ) -> RepairPlan:
        metrics_listing = self._list_dir_contents(self.metrics_dir)
        snippets_listing = self._list_dir_contents(self.snippets_dir)

        prompt = REPAIR_PROMPT.format(
            metrics_listing=metrics_listing,
            snippets_listing=snippets_listing,
            question=question,
            expected_sql=expected_sql,
            generated_sql=generated_sql,
            failure_context=failure_context,
        )

        result = self.llm.call(
            system_prompt=prompt,
            user_message="Propose a repair plan.",
        )

        actions = [RepairAction(**a) for a in result.get("actions", [])]
        return RepairPlan(
            actions=actions,
            reasoning=result.get("reasoning", ""),
        )

    def apply(self, plan: RepairPlan) -> None:
        for action in plan.actions:
            # Save original for revert
            if os.path.exists(action.file):
                with open(action.file) as f:
                    action._original = f.read()
            else:
                action._original = None

            os.makedirs(os.path.dirname(action.file), exist_ok=True)
            with open(action.file, "w") as f:
                f.write(action.content)

    def revert(self, plan: RepairPlan) -> None:
        for action in plan.actions:
            if action._original is not None:
                with open(action.file, "w") as f:
                    f.write(action._original)
            elif os.path.exists(action.file):
                os.remove(action.file)

    def _list_dir_contents(self, directory: str) -> str:
        if not os.path.isdir(directory):
            return "(empty)"
        parts = []
        for fname in sorted(os.listdir(directory)):
            fpath = os.path.join(directory, fname)
            if os.path.isfile(fpath):
                with open(fpath) as f:
                    content = f.read()
                parts.append(f"#### {fname}\n```\n{content}\n```")
        return "\n".join(parts) if parts else "(empty)"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_autotest_repairer.py -v`
Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add src/autotest/repairer.py tests/test_autotest_repairer.py
git commit -m "feat(autotest): add repairer with propose/apply/revert"
```

---

### Task 5: Runner (main orchestration)

**Files:**
- Create: `src/autotest/runner.py`
- Create: `tests/test_autotest_runner.py`

**Step 1: Write the failing test**

```python
# tests/test_autotest_runner.py
import pytest
from unittest.mock import MagicMock, patch
from src.autotest.loader import BenchmarkCase
from src.autotest.comparator import CompareResult, ResultCompareResult
from src.autotest.repairer import RepairPlan, RepairAction
from src.autotest.runner import Runner, CaseResult, RunSummary


def _make_case(id="test_case", question="Q?", expected_sql="SELECT 1"):
    return BenchmarkCase(id=id, question=question, expected_sql=expected_sql, tags=[])


def test_runner_pass_on_structural_and_result_match():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT 1"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(match=True)

    result_comparator = MagicMock()
    result_comparator.compare.return_value = ResultCompareResult(match=True)

    query_service = MagicMock()
    from src.query_service import QueryResult
    query_service.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=3,
        max_llm_calls=50,
    )

    case = _make_case()
    result = runner.run_case(case)
    assert result.passed is True
    assert result.retries == 0


def test_runner_fails_after_max_retries():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT bad"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(
        match=False, differences=["Wrong table"]
    )

    result_comparator = MagicMock()
    query_service = MagicMock()
    repairer = MagicMock()
    repairer.propose.return_value = RepairPlan(
        actions=[RepairAction(type="edit_snippet", file="/tmp/x.sql", content="x")],
        reasoning="fix",
    )

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=2,
        max_llm_calls=50,
    )

    case = _make_case()
    result = runner.run_case(case)
    assert result.passed is False
    assert result.retries == 2


def test_runner_no_repair_mode():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT bad"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(
        match=False, differences=["Wrong table"]
    )

    result_comparator = MagicMock()
    query_service = MagicMock()
    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=3,
        max_llm_calls=50,
        no_repair=True,
    )

    case = _make_case()
    result = runner.run_case(case)
    assert result.passed is False
    assert result.retries == 0
    repairer.propose.assert_not_called()


def test_run_all_returns_summary():
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT 1"}

    comparator = MagicMock()
    comparator.compare.return_value = CompareResult(match=True)

    result_comparator = MagicMock()
    result_comparator.compare.return_value = ResultCompareResult(match=True)

    query_service = MagicMock()
    from src.query_service import QueryResult
    query_service.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=comparator,
        result_comparator=result_comparator,
        query_service=query_service,
        repairer=repairer,
        max_retries=3,
        max_llm_calls=50,
    )

    cases = [_make_case(id="a"), _make_case(id="b")]
    summary = runner.run_all(cases)
    assert isinstance(summary, RunSummary)
    assert summary.total == 2
    assert summary.passed == 2
    assert summary.failed == 0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_autotest_runner.py -v`
Expected: FAIL with "cannot import name 'Runner'"

**Step 3: Write minimal implementation**

```python
# src/autotest/runner.py
from dataclasses import dataclass, field
from typing import Optional

from src.agent import Agent
from src.autotest.loader import BenchmarkCase
from src.autotest.comparator import (
    StructuralComparator,
    ResultComparator,
    CompareResult,
    ResultCompareResult,
)
from src.autotest.repairer import Repairer, RepairPlan
from src.query_service import QueryService


@dataclass
class CaseResult:
    case_id: str
    passed: bool
    retries: int = 0
    structural_result: Optional[CompareResult] = None
    result_result: Optional[ResultCompareResult] = None
    repair_plans: list[RepairPlan] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class RunSummary:
    total: int
    passed: int
    repaired: int
    failed: int
    results: list[CaseResult] = field(default_factory=list)


class Runner:
    def __init__(
        self,
        agent: Agent,
        structural_comparator: StructuralComparator,
        result_comparator: ResultComparator,
        query_service: QueryService,
        repairer: Repairer,
        max_retries: int = 3,
        max_llm_calls: int = 50,
        no_repair: bool = False,
        dry_run: bool = False,
    ):
        self.agent = agent
        self.structural = structural_comparator
        self.result_comp = result_comparator
        self.query_service = query_service
        self.repairer = repairer
        self.max_retries = max_retries
        self.max_llm_calls = max_llm_calls
        self.no_repair = no_repair
        self.dry_run = dry_run
        self._llm_calls = 0

    def run_case(self, case: BenchmarkCase) -> CaseResult:
        # Phase 1: Generate SQL
        response = self.agent.ask(case.question)
        self._llm_calls += 1

        if response.get("type") != "sql":
            msg = response.get("message", "Agent did not return SQL")
            return CaseResult(case_id=case.id, passed=False, error=msg)

        generated_sql = response["sql"]

        # Phase 1b: Structural compare
        struct_result = self.structural.compare(case.expected_sql, generated_sql)
        self._llm_calls += 1

        if struct_result.match:
            # Phase 2: Result compare
            result_outcome = self._run_result_compare(case.expected_sql, generated_sql)
            if result_outcome.match:
                return CaseResult(
                    case_id=case.id,
                    passed=True,
                    structural_result=struct_result,
                    result_result=result_outcome,
                )
            failure_context = f"Result mismatch: {result_outcome.row_mismatches} rows differ. {result_outcome.sample_diffs}"
        else:
            failure_context = f"Structural mismatch: {struct_result.differences}"
            result_outcome = None

        # Phase 3: Repair loop
        if self.no_repair:
            return CaseResult(
                case_id=case.id,
                passed=False,
                structural_result=struct_result,
                result_result=result_outcome,
                error=failure_context,
            )

        repair_plans = []
        for retry in range(self.max_retries):
            if self._llm_calls >= self.max_llm_calls:
                return CaseResult(
                    case_id=case.id,
                    passed=False,
                    retries=retry,
                    repair_plans=repair_plans,
                    error="Global LLM call budget exhausted",
                )

            plan = self.repairer.propose(
                question=case.question,
                expected_sql=case.expected_sql,
                generated_sql=generated_sql,
                failure_context=failure_context,
            )
            self._llm_calls += 1
            repair_plans.append(plan)

            if not self.dry_run:
                self.repairer.apply(plan)

            # Re-run
            response = self.agent.ask(case.question)
            self._llm_calls += 1

            if response.get("type") != "sql":
                if not self.dry_run:
                    self.repairer.revert(plan)
                continue

            generated_sql = response["sql"]

            struct_result = self.structural.compare(case.expected_sql, generated_sql)
            self._llm_calls += 1

            if struct_result.match:
                result_outcome = self._run_result_compare(case.expected_sql, generated_sql)
                if result_outcome.match:
                    return CaseResult(
                        case_id=case.id,
                        passed=True,
                        retries=retry + 1,
                        structural_result=struct_result,
                        result_result=result_outcome,
                        repair_plans=repair_plans,
                    )
                failure_context = f"Result mismatch: {result_outcome.row_mismatches} rows differ. {result_outcome.sample_diffs}"
            else:
                failure_context = f"Structural mismatch: {struct_result.differences}"
                if not self.dry_run:
                    self.repairer.revert(plan)

        return CaseResult(
            case_id=case.id,
            passed=False,
            retries=self.max_retries,
            structural_result=struct_result,
            result_result=result_outcome,
            repair_plans=repair_plans,
            error=failure_context,
        )

    def _run_result_compare(
        self, expected_sql: str, generated_sql: str
    ) -> ResultCompareResult:
        exp_result = self.query_service.execute(expected_sql)
        gen_result = self.query_service.execute(generated_sql)
        return self.result_comp.compare(exp_result, gen_result)

    def run_all(self, cases: list[BenchmarkCase]) -> RunSummary:
        results = []
        for case in cases:
            result = self.run_case(case)
            results.append(result)

        passed = sum(1 for r in results if r.passed and r.retries == 0)
        repaired = sum(1 for r in results if r.passed and r.retries > 0)
        failed = sum(1 for r in results if not r.passed)

        return RunSummary(
            total=len(results),
            passed=passed,
            repaired=repaired,
            failed=failed,
            results=results,
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_autotest_runner.py -v`
Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add src/autotest/runner.py tests/test_autotest_runner.py
git commit -m "feat(autotest): add runner with 3-phase pipeline and repair loop"
```

---

### Task 6: CLI Entry Point

**Files:**
- Create: `src/autotest/__main__.py`
- Create: `tests/test_autotest_cli.py`

**Step 1: Write the failing test**

```python
# tests/test_autotest_cli.py
import pytest
from unittest.mock import patch, MagicMock
from src.autotest.__main__ import parse_args


def test_parse_default_args():
    args = parse_args([])
    assert args.benchmark == "tests/benchmark.yaml"
    assert args.max_retries == 3
    assert args.max_llm_calls == 50
    assert args.no_repair is False
    assert args.dry_run is False
    assert args.tags is None
    assert args.id is None


def test_parse_custom_args():
    args = parse_args([
        "--benchmark", "custom.yaml",
        "--tags", "ads,traffic",
        "--id", "my_case",
        "--max-retries", "5",
        "--max-llm-calls", "100",
        "--no-repair",
        "--dry-run",
    ])
    assert args.benchmark == "custom.yaml"
    assert args.tags == "ads,traffic"
    assert args.id == "my_case"
    assert args.max_retries == 5
    assert args.max_llm_calls == 100
    assert args.no_repair is True
    assert args.dry_run is True
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_autotest_cli.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Write minimal implementation**

```python
# src/autotest/__main__.py
import argparse
import sys

from src.agent import Agent
from src.llm_client import LLMClient
from src.autotest.loader import BenchmarkLoader
from src.autotest.comparator import StructuralComparator, ResultComparator
from src.autotest.repairer import Repairer
from src.autotest.runner import Runner, RunSummary
from src.query_service import QueryService


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Auto-testing loop for S&R&A Metric SQL Agent"
    )
    parser.add_argument(
        "--benchmark", default="tests/benchmark.yaml",
        help="Path to benchmark YAML (default: tests/benchmark.yaml)",
    )
    parser.add_argument("--tags", default=None, help="Comma-separated tags to filter")
    parser.add_argument("--id", default=None, help="Run a single case by ID")
    parser.add_argument(
        "--max-retries", type=int, default=3,
        help="Per-case retry cap (default: 3)",
    )
    parser.add_argument(
        "--max-llm-calls", type=int, default=50,
        help="Global LLM call budget (default: 50)",
    )
    parser.add_argument(
        "--no-repair", action="store_true",
        help="Evaluation only — skip repair loop",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show repair plans without applying changes",
    )
    return parser.parse_args(argv)


def print_summary(summary: RunSummary):
    print("\n" + "=" * 40)
    print(f"=== Auto-Test Results ===")
    print(f"Total: {summary.total} | Passed: {summary.passed} | Repaired: {summary.repaired} | Failed: {summary.failed}")
    print()

    repaired = [r for r in summary.results if r.passed and r.retries > 0]
    if repaired:
        print("Repaired:")
        for r in repaired:
            print(f"  ✓ {r.case_id} ({r.retries} retries)")
            for plan in r.repair_plans:
                for action in plan.actions:
                    print(f"    - {action.type}: {action.file}")
        print()

    failed = [r for r in summary.results if not r.passed]
    if failed:
        print("Failed (needs human review):")
        for r in failed:
            print(f"  ✗ {r.case_id} ({r.retries} retries exhausted)")
            if r.error:
                print(f"    - {r.error}")
        print()


def main():
    args = parse_args()

    # Load benchmark cases
    loader = BenchmarkLoader(args.benchmark)
    tags = args.tags.split(",") if args.tags else None
    cases = loader.load(tags=tags, case_id=args.id)

    if not cases:
        print("No benchmark cases found.")
        sys.exit(1)

    print(f"Running {len(cases)} benchmark case(s)...")

    # Set up components
    llm = LLMClient()
    agent = Agent(llm_client=llm)
    structural = StructuralComparator(llm_client=llm)
    result_comp = ResultComparator()
    query_service = QueryService()  # TODO: configure base_url and token
    repairer = Repairer(llm_client=llm)

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=query_service,
        repairer=repairer,
        max_retries=args.max_retries,
        max_llm_calls=args.max_llm_calls,
        no_repair=args.no_repair,
        dry_run=args.dry_run,
    )

    summary = runner.run_all(cases)
    print_summary(summary)

    # Prompt for commit if there were repairs
    if summary.repaired > 0 and not args.dry_run:
        print("KB changes pending confirmation.")
        answer = input("Commit these changes? [y/n] ").strip().lower()
        if answer == "y":
            import subprocess
            subprocess.run(["git", "add", "metrics/", "snippets/"], check=True)
            subprocess.run(
                ["git", "commit", "-m", "fix(kb): auto-repair from autotest loop"],
                check=True,
            )
            print("Changes committed.")
        else:
            print("Changes left unstaged. Review manually.")


if __name__ == "__main__":
    main()
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_autotest_cli.py -v`
Expected: PASS (2 tests)

**Step 5: Run full test suite**

Run: `pytest tests/ --ignore=tests/test_regression.py -v`
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/autotest/__main__.py tests/test_autotest_cli.py
git commit -m "feat(autotest): add CLI entry point with summary output"
```

---

### Task 7: Integration Test & Final Verification

**Files:**
- Create: `tests/test_autotest_integration.py`

**Step 1: Write integration test**

```python
# tests/test_autotest_integration.py
"""Integration test: runs the full pipeline with mocked LLM and query service."""
import pytest
from unittest.mock import MagicMock
from src.autotest.loader import BenchmarkCase
from src.autotest.comparator import (
    StructuralComparator,
    ResultComparator,
    CompareResult,
    ResultCompareResult,
)
from src.autotest.repairer import Repairer, RepairPlan, RepairAction
from src.autotest.runner import Runner
from src.query_service import QueryResult


def test_full_pipeline_pass():
    """Case passes both gates on first try."""
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT 1"}

    structural = MagicMock()
    structural.compare.return_value = CompareResult(match=True)

    result_comp = MagicMock()
    result_comp.compare.return_value = ResultCompareResult(match=True)

    qs = MagicMock()
    qs.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=qs,
        repairer=repairer,
    )

    cases = [BenchmarkCase(id="t1", question="Q?", expected_sql="SELECT 1")]
    summary = runner.run_all(cases)

    assert summary.total == 1
    assert summary.passed == 1
    assert summary.failed == 0


def test_full_pipeline_repair_then_pass():
    """Case fails structural, repair fixes it, passes on retry."""
    agent = MagicMock()
    # First call: bad SQL; second call (after repair): good SQL
    agent.ask.side_effect = [
        {"type": "sql", "sql": "SELECT bad"},
        {"type": "sql", "sql": "SELECT good"},
    ]

    structural = MagicMock()
    structural.compare.side_effect = [
        CompareResult(match=False, differences=["wrong table"]),
        CompareResult(match=True),
    ]

    result_comp = MagicMock()
    result_comp.compare.return_value = ResultCompareResult(match=True)

    qs = MagicMock()
    qs.execute.return_value = QueryResult(columns=["n"], rows=[[1]])

    repairer = MagicMock()
    repairer.propose.return_value = RepairPlan(
        actions=[RepairAction(type="edit_snippet", file="/tmp/x.sql", content="fixed")],
        reasoning="Fixed table name",
    )

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=qs,
        repairer=repairer,
    )

    cases = [BenchmarkCase(id="t1", question="Q?", expected_sql="SELECT good")]
    summary = runner.run_all(cases)

    assert summary.total == 1
    assert summary.repaired == 1
    assert summary.failed == 0


def test_full_pipeline_exhausts_retries():
    """Case fails all retries."""
    agent = MagicMock()
    agent.ask.return_value = {"type": "sql", "sql": "SELECT bad"}

    structural = MagicMock()
    structural.compare.return_value = CompareResult(
        match=False, differences=["wrong"]
    )

    result_comp = MagicMock()
    qs = MagicMock()

    repairer = MagicMock()
    repairer.propose.return_value = RepairPlan(
        actions=[RepairAction(type="edit_snippet", file="/tmp/x.sql", content="x")],
        reasoning="attempt",
    )

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=qs,
        repairer=repairer,
        max_retries=2,
    )

    cases = [BenchmarkCase(id="t1", question="Q?", expected_sql="SELECT good")]
    summary = runner.run_all(cases)

    assert summary.total == 1
    assert summary.failed == 1
    assert summary.results[0].retries == 2
```

**Step 2: Run integration test**

Run: `pytest tests/test_autotest_integration.py -v`
Expected: PASS (3 tests)

**Step 3: Run full suite**

Run: `pytest tests/ --ignore=tests/test_regression.py -v`
Expected: All tests pass (53 existing + new autotest tests)

**Step 4: Commit**

```bash
git add tests/test_autotest_integration.py
git commit -m "test(autotest): add integration tests for full pipeline"
```

---

### Task 8: Copy design doc into worktree

**Step 1: Copy the design doc**

```bash
cp docs/plans/2026-01-30-auto-testing-loop-design.md docs/plans/
```

Note: The design doc was written to the main worktree. Copy it into this branch.

**Step 2: Commit**

```bash
git add docs/plans/2026-01-30-auto-testing-loop-design.md
git commit -m "docs: add auto-testing loop design doc"
```
