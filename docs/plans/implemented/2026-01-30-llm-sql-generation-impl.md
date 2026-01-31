# LLM-Based SQL Generation — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the deterministic Jinja2 pipeline with LLM-generated SQL using OpenAI GPT-4o, guided by YAML metric definitions and reference SQL snippets as knowledge base context.

**Architecture:** Single-turn LLM call. System prompt contains all metric YAML definitions + per-metric SQL snippets + output format rules. User question goes in as user message. LLM returns JSON with either SQL or ambiguity candidates. Regression test suite validates accuracy.

**Tech Stack:** Python 3.10+, OpenAI API (GPT-4o), PyYAML, pytest

---

### Task 1: Add OpenAI dependency and LLM client

**Files:**
- Modify: `requirements.txt`
- Create: `src/llm_client.py`
- Test: `tests/test_llm_client.py`

**Step 1: Write the failing test**

```python
# tests/test_llm_client.py
from unittest.mock import patch, MagicMock
from src.llm_client import LLMClient


def test_call_returns_parsed_json():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '{"type": "sql", "sql": "SELECT 1"}'

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.call(system_prompt="You are a helper.", user_message="test")

    assert result == {"type": "sql", "sql": "SELECT 1"}


def test_call_strips_markdown_fences():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '```json\n{"type": "sql", "sql": "SELECT 1"}\n```'

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.call(system_prompt="test", user_message="test")

    assert result == {"type": "sql", "sql": "SELECT 1"}


def test_call_handles_ambiguous_response():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '{"type": "ambiguous", "candidates": ["Ads Gross Rev", "Net Ads Rev"]}'

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.call(system_prompt="test", user_message="revenue?")

    assert result["type"] == "ambiguous"
    assert len(result["candidates"]) == 2
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_llm_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.llm_client'`

**Step 3: Write minimal implementation**

Add to `requirements.txt`:
```
openai>=1.0
```

```python
# src/llm_client.py
import json
import openai


class LLMClient:
    def __init__(self, model: str = "gpt-4o"):
        self.model = model
        self.client = openai.OpenAI()

    def call(self, system_prompt: str, user_message: str) -> dict:
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        raw = response.choices[0].message.content.strip()
        return self._parse(raw)

    @staticmethod
    def _parse(raw: str) -> dict:
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        return json.loads(text)
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_llm_client.py -v`
Expected: 3 PASS

**Step 5: Commit**

```bash
git add requirements.txt src/llm_client.py tests/test_llm_client.py
git commit -m "feat: add OpenAI LLM client wrapper"
```

---

### Task 2: Build prompt builder

**Files:**
- Create: `src/prompt_builder.py`
- Test: `tests/test_prompt_builder.py`

**Step 1: Write the failing test**

```python
# tests/test_prompt_builder.py
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
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_prompt_builder.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.prompt_builder'`

**Step 3: Write minimal implementation**

```python
# src/prompt_builder.py
import os
import yaml

OUTPUT_FORMAT = """
You must respond with ONLY valid JSON in one of these two formats:

If the request is clear and unambiguous:
{
  "type": "sql",
  "sql": "SELECT ... (the complete SQL query)"
}

If the request is ambiguous (e.g., "revenue" could mean multiple metrics):
{
  "type": "ambiguous",
  "candidates": ["Candidate interpretation 1", "Candidate interpretation 2"]
}
"""

SYSTEM_PREAMBLE = """You are an expert SQL generator for the S&R&A (Search, Recommendation & Ads) team at Shopee.

Given a user question about S&R&A metrics, generate the exact SQL query to answer it.

RULES:
- Use ONLY the tables, columns, and filters defined in the metric definitions below.
- Follow the reference SQL examples exactly for query patterns (aggregation style, date handling, filters).
- For monthly metrics, use: substr(cast(date_col as varchar), 1, 7) AS period, and avg() aggregation.
- Always include required filters (e.g., tz_type).
- If market is specified, add a grass_region = 'XX' filter.
- Date ranges use: BETWEEN date 'YYYY-MM-DD' AND date 'YYYY-MM-DD'
- For comparison queries (MoM, YoY), use a CTE with current_period and previous_period, and compute change_rate.
- If the question is ambiguous (could refer to multiple metrics), return ambiguous candidates instead of guessing.
- Return ONLY JSON. No explanations, no markdown outside JSON.

Available markets: ID, VN, TH, TW, BR, MX, PH, SG, MY, CO, CL
"""


class PromptBuilder:
    def __init__(self, metrics_dir: str = "metrics", snippets_dir: str = "snippets"):
        self.metrics_dir = metrics_dir
        self.snippets_dir = snippets_dir

    def build(self) -> str:
        sections = [SYSTEM_PREAMBLE]

        # Section A: Metric definitions
        metrics_section = self._build_metrics_section()
        if metrics_section:
            sections.append("## Metric Definitions\n" + metrics_section)

        # Section B: Reference SQL snippets
        snippets_section = self._build_snippets_section()
        if snippets_section:
            sections.append("## Reference SQL Examples\n" + snippets_section)

        # Section C: Output format
        sections.append("## Output Format\n" + OUTPUT_FORMAT)

        return "\n\n".join(sections)

    def _build_metrics_section(self) -> str:
        if not os.path.isdir(self.metrics_dir):
            return ""
        parts = []
        for fname in sorted(os.listdir(self.metrics_dir)):
            if not fname.endswith((".yaml", ".yml")):
                continue
            path = os.path.join(self.metrics_dir, fname)
            with open(path) as f:
                data = yaml.safe_load(f)
            if data and "metric" in data:
                parts.append(self._format_metric(data["metric"]))
        return "\n".join(parts)

    def _format_metric(self, m: dict) -> str:
        lines = [f"### {m['name']}"]
        if m.get("aliases"):
            lines.append(f"Aliases: {', '.join(m['aliases'])}")
        lines.append(f"Type: {m['type']}")
        if m.get("formula"):
            lines.append(f"Formula: {m['formula']}")
        if m.get("aggregation"):
            lines.append(f"Aggregation: {m['aggregation']}")
        for source in m.get("sources", []):
            lines.append(f"Source table: {source['table']}")
            lines.append(f"  Columns: {source['columns']}")
            if source.get("filters"):
                lines.append(f"  Filters: {source['filters']}")
            if source.get("use_when"):
                lines.append(f"  Use when: {source['use_when']}")
        if m.get("snippet_file"):
            lines.append(f"Snippet: {m['snippet_file']}")
        if m.get("notes"):
            lines.append(f"Notes: {m['notes']}")
        dims = m.get("dimensions", {})
        lines.append(f"Required dimensions: {dims.get('required', [])}")
        lines.append(f"Optional dimensions: {dims.get('optional', [])}")
        lines.append("")
        return "\n".join(lines)

    def _build_snippets_section(self) -> str:
        if not os.path.isdir(self.snippets_dir):
            return ""
        parts = []
        for fname in sorted(os.listdir(self.snippets_dir)):
            if not fname.endswith(".sql"):
                continue
            path = os.path.join(self.snippets_dir, fname)
            with open(path) as f:
                sql = f.read().strip()
            name = fname.replace(".sql", "").replace("_", " ").title()
            parts.append(f"### {name}\n```sql\n{sql}\n```\n")
        return "\n".join(parts)
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_prompt_builder.py -v`
Expected: 3 PASS

**Step 5: Commit**

```bash
git add src/prompt_builder.py tests/test_prompt_builder.py
git commit -m "feat: add prompt builder for LLM system prompt assembly"
```

---

### Task 3: Rewrite agent to use LLM SQL generation

**Files:**
- Modify: `src/agent.py`
- Test: `tests/test_agent.py`

**Step 1: Write the failing test**

```python
# tests/test_agent.py
from unittest.mock import patch, MagicMock
from src.agent import Agent


def test_agent_returns_sql_for_clear_question():
    expected_sql = "SELECT avg(a1) AS dau FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30' AND grass_region = 'ID'"

    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.call.return_value = {"type": "sql", "sql": expected_sql}

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        result = agent.ask("ID market DAU in November 2025")

    assert result["type"] == "sql"
    assert "avg(a1)" in result["sql"]
    mock_llm.call.assert_called_once()


def test_agent_returns_ambiguous_for_vague_question():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.call.return_value = {
            "type": "ambiguous",
            "candidates": ["Ads Gross Rev (total ads revenue)", "Net Ads Rev (after deductions)"],
        }

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        result = agent.ask("What's the revenue?")

    assert result["type"] == "ambiguous"
    assert len(result["candidates"]) == 2
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_agent.py -v`
Expected: FAIL — old Agent signature doesn't match

**Step 3: Rewrite agent.py**

```python
# src/agent.py
import sys
from src.prompt_builder import PromptBuilder
from src.llm_client import LLMClient


class Agent:
    def __init__(
        self,
        metrics_dir: str = "metrics",
        snippets_dir: str = "snippets",
        model: str = "gpt-4o",
    ):
        self.prompt_builder = PromptBuilder(metrics_dir=metrics_dir, snippets_dir=snippets_dir)
        self.system_prompt = self.prompt_builder.build()
        self.llm = LLMClient(model=model)

    def ask(self, question: str) -> dict:
        return self.llm.call(
            system_prompt=self.system_prompt,
            user_message=question,
        )


def main():
    agent = Agent()
    print("S&R&A Metric Agent (type 'quit' to exit)")
    print("-" * 50)
    while True:
        try:
            question = input("\nQ: ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if question.lower() in ("quit", "exit", "q"):
            break
        if not question:
            continue

        result = agent.ask(question)

        if result.get("type") == "ambiguous":
            print("\nAmbiguous request. Did you mean:")
            for i, candidate in enumerate(result["candidates"], 1):
                print(f"  {i}. {candidate}")
            print("Please rephrase with a specific metric.")
        elif result.get("type") == "sql":
            print(f"\n--- Generated SQL ---\n{result['sql']}")
        else:
            print(f"\nUnexpected response: {result}")


if __name__ == "__main__":
    main()
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_agent.py -v`
Expected: 2 PASS

**Step 5: Commit**

```bash
git add src/agent.py tests/test_agent.py
git commit -m "feat: rewrite agent to use LLM-based SQL generation"
```

---

### Task 4: Build the importer analyzer

**Files:**
- Create: `src/importer/__init__.py`
- Create: `src/importer/analyzer.py`
- Test: `tests/test_analyzer.py`

**Step 1: Write the failing test**

```python
# tests/test_analyzer.py
from unittest.mock import patch, MagicMock
from src.importer.analyzer import SQLAnalyzer


def test_analyzer_extracts_metrics_from_sql():
    sample_sql = """
    select avg(a1) as dau from traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
    where grass_date between date '2025-11-01' and date '2025-11-30' and tz_type = 'local'
    """

    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '''```json
    [
        {
            "name": "DAU",
            "aliases": ["daily active users"],
            "type": "simple",
            "table": "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
            "columns": {"value": "a1", "date": "grass_date", "region": "grass_region"},
            "filters": ["tz_type = 'local'"],
            "aggregation": "avg",
            "snippet": "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, grass_region, avg(a1) AS dau FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}' AND tz_type = 'local' GROUP BY 1, 2",
            "notes": ""
        }
    ]
    ```'''

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        analyzer = SQLAnalyzer()
        results = analyzer.analyze_sql(sample_sql)

    assert len(results) >= 1
    assert results[0]["name"] == "DAU"
    assert "snippet" in results[0]


def test_analyzer_extracts_from_doc():
    sample_doc = "DAU means daily active users. We compute it as avg(a1) from the platform active churn table."

    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '''```json
    [
        {
            "name": "DAU",
            "aliases": ["daily active users"],
            "type": "simple",
            "notes": "Computed as avg(a1) from platform active churn table"
        }
    ]
    ```'''

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        analyzer = SQLAnalyzer()
        results = analyzer.analyze_doc(sample_doc)

    assert len(results) >= 1
    assert results[0]["name"] == "DAU"
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_analyzer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.importer'`

**Step 3: Write minimal implementation**

```python
# src/importer/__init__.py
```

```python
# src/importer/analyzer.py
import json
import openai

SQL_ANALYZER_PROMPT = """You are an expert SQL analyst. Given an ETL SQL query, extract all metrics defined in it.

For each metric, return a JSON array of objects with these fields:
- name: Human-readable metric name (e.g., "DAU", "Ads Gross Rev")
- aliases: List of alternative names
- type: "simple" (single table, direct aggregation) or "complex" (multi-table joins, CASE WHEN logic)
- table: Primary source table (full schema.table name)
- columns: Dict of column mappings (value, date, region, etc.)
- filters: List of required WHERE clauses (e.g., "tz_type = 'local'")
- aggregation: Aggregation function (avg, sum, count, etc.)
- snippet: The isolated SQL fragment for this metric, with {{ date_start }}, {{ date_end }}, {{ market }} as placeholders
- notes: Any business rules or edge cases

Return ONLY a JSON array. No explanations."""

DOC_ANALYZER_PROMPT = """You are an expert data analyst. Given a document describing business metrics, extract metric definitions.

For each metric, return a JSON array of objects with these fields:
- name: Human-readable metric name
- aliases: List of alternative names
- type: "simple" or "complex"
- notes: Business rules, formulas, edge cases described in the document

Include any field from the SQL schema if the document provides enough information:
- table, columns, filters, aggregation, snippet

Return ONLY a JSON array. No explanations."""


class SQLAnalyzer:
    def __init__(self, model: str = "gpt-4o"):
        self.model = model
        self.client = openai.OpenAI()

    def _call_llm(self, system_prompt: str, content: str) -> list[dict]:
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content},
            ],
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1])
        return json.loads(raw)

    def analyze_sql(self, sql: str) -> list[dict]:
        return self._call_llm(SQL_ANALYZER_PROMPT, sql)

    def analyze_doc(self, doc_text: str) -> list[dict]:
        return self._call_llm(DOC_ANALYZER_PROMPT, doc_text)
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_analyzer.py -v`
Expected: 2 PASS

**Step 5: Commit**

```bash
git add src/importer/__init__.py src/importer/analyzer.py tests/test_analyzer.py
git commit -m "feat: add importer analyzer for SQL and doc parsing"
```

---

### Task 5: Build the importer generator

**Files:**
- Create: `src/importer/generator.py`
- Test: `tests/test_generator.py`

**Step 1: Write the failing test**

```python
# tests/test_generator.py
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

        # Check YAML was written
        yaml_path = os.path.join(metrics_dir, "dau.yaml")
        assert os.path.exists(yaml_path)
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
        assert data["metric"]["name"] == "DAU"
        assert data["metric"]["type"] == "simple"

        # Check snippet was written
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

        # No snippet for simple metrics without snippet field
        snippet_path = os.path.join(snippets_dir, "test_metric.sql")
        assert not os.path.exists(snippet_path)
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_generator.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.importer.generator'`

**Step 3: Write minimal implementation**

```python
# src/importer/generator.py
import os
import yaml


class Generator:
    def __init__(self, metrics_dir: str = "metrics", snippets_dir: str = "snippets"):
        self.metrics_dir = metrics_dir
        self.snippets_dir = snippets_dir

    def generate(self, analyzed_metrics: list[dict]) -> list[str]:
        os.makedirs(self.metrics_dir, exist_ok=True)
        os.makedirs(self.snippets_dir, exist_ok=True)

        created_files = []
        for m in analyzed_metrics:
            slug = m["name"].lower().replace(" ", "_")

            # Write YAML
            yaml_data = self._build_yaml(m)
            yaml_path = os.path.join(self.metrics_dir, f"{slug}.yaml")
            with open(yaml_path, "w") as f:
                yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)
            created_files.append(yaml_path)

            # Write snippet if provided
            snippet = m.get("snippet")
            if snippet:
                snippet_path = os.path.join(self.snippets_dir, f"{slug}.sql")
                with open(snippet_path, "w") as f:
                    f.write(snippet.strip() + "\n")
                created_files.append(snippet_path)

        return created_files

    def _build_yaml(self, m: dict) -> dict:
        metric = {
            "name": m["name"],
            "aliases": m.get("aliases", []),
            "type": m["type"],
            "dimensions": {"required": ["market", "date_range"], "optional": []},
        }
        if m.get("aggregation"):
            metric["aggregation"] = m["aggregation"]
        if m.get("table"):
            source = {
                "id": m["name"].lower().replace(" ", "_"),
                "layer": "dws",
                "table": m["table"],
                "columns": m.get("columns", {}),
                "filters": m.get("filters", []),
                "use_when": {"granularity": ["platform"]},
            }
            metric["sources"] = [source]
        if m.get("snippet"):
            metric["snippet_file"] = f"snippets/{m['name'].lower().replace(' ', '_')}.sql"
        if m.get("notes"):
            metric["notes"] = m["notes"]
        return {"metric": metric}
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_generator.py -v`
Expected: 2 PASS

**Step 5: Commit**

```bash
git add src/importer/generator.py tests/test_generator.py
git commit -m "feat: add importer generator for YAML and snippet output"
```

---

### Task 6: Build regression test runner

**Files:**
- Modify: `tests/test_cases.yaml` — add `expected_type` and `expected_sql_not_contains` fields
- Create: `tests/test_regression.py`

**Step 1: Update test_cases.yaml**

Add `expected_type` to existing cases and add ambiguity test:

```yaml
# tests/test_cases.yaml
- question: "ID market DAU in November 2025"
  expected_type: sql
  expected_sql_contains:
    - "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live"
    - "grass_region = 'ID'"
    - "2025-11-01"
    - "avg("
  expected_sql_not_contains:
    - "dwd_module"

- question: "Ads gross revenue for TH in October 2025"
  expected_type: sql
  expected_sql_contains:
    - "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live"
    - "grass_region = 'TH'"
    - "2025-10-01"
  expected_sql_not_contains: []

- question: "Order percentage by channel for BR in Nov 2025"
  expected_type: sql
  expected_sql_contains:
    - "sr_okr_table_metric_dws"
    - "grass_region = 'BR'"
    - "Global Search"
    - "Daily Discover"
    - "Live Streaming"
  expected_sql_not_contains: []

- question: "Compare ID DAU between October and November 2025"
  expected_type: sql
  expected_sql_contains:
    - "current_period"
    - "previous_period"
    - "change_rate"
    - "2025-11-01"
    - "2025-10-01"
  expected_sql_not_contains: []

- question: "Net ads revenue for VN in November 2025"
  expected_type: sql
  expected_sql_contains:
    - "net_ads_rev"
    - "grass_region = 'VN'"
  expected_sql_not_contains: []

- question: "What's the revenue?"
  expected_type: ambiguous
  expected_candidates_contain:
    - "ads"
```

**Step 2: Write the test runner**

```python
# tests/test_regression.py
"""
Regression test suite for LLM-based SQL generation.

Runs against live OpenAI API. Set OPENAI_API_KEY env var.
Skip with: pytest tests/test_regression.py -k "not regression" or mark with
    pytest -m "not live"
"""
import os
import pytest
import yaml
from src.agent import Agent

TESTS_DIR = os.path.dirname(__file__)


def load_test_cases():
    path = os.path.join(TESTS_DIR, "test_cases.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


test_cases = load_test_cases()


@pytest.mark.live
@pytest.mark.parametrize(
    "case",
    test_cases,
    ids=[c["question"][:50] for c in test_cases],
)
def test_regression(case):
    agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
    result = agent.ask(case["question"])

    expected_type = case["expected_type"]
    assert result.get("type") == expected_type, (
        f"Expected type '{expected_type}', got '{result.get('type')}'. "
        f"Full response: {result}"
    )

    if expected_type == "sql":
        sql = result["sql"]
        for fragment in case.get("expected_sql_contains", []):
            assert fragment in sql, (
                f"Expected SQL to contain '{fragment}'.\n"
                f"Got SQL:\n{sql}"
            )
        for fragment in case.get("expected_sql_not_contains", []):
            assert fragment not in sql, (
                f"Expected SQL to NOT contain '{fragment}'.\n"
                f"Got SQL:\n{sql}"
            )

    elif expected_type == "ambiguous":
        candidates = " ".join(result.get("candidates", []))
        for keyword in case.get("expected_candidates_contain", []):
            assert keyword.lower() in candidates.lower(), (
                f"Expected candidates to mention '{keyword}'.\n"
                f"Got: {result.get('candidates')}"
            )
```

**Step 3: Verify test structure loads**

Run: `python -m pytest tests/test_regression.py --collect-only`
Expected: 6 test items collected

**Step 4: Commit**

```bash
git add tests/test_cases.yaml tests/test_regression.py
git commit -m "feat: add regression test suite for LLM SQL generation"
```

---

### Task 7: Create raw/ directory and importer CLI

**Files:**
- Create: `raw/` directory (move `monthly_core_metrics_tracker.sql` there as reference)
- Create: `src/importer/cli.py`
- Test: manual — run importer on the existing SQL file

**Step 1: Set up raw directory**

```bash
mkdir -p raw
cp monthly_core_metrics_tracker.sql raw/
```

**Step 2: Write importer CLI**

```python
# src/importer/cli.py
import argparse
import sys
from src.importer.analyzer import SQLAnalyzer
from src.importer.generator import Generator


def main():
    parser = argparse.ArgumentParser(description="Import raw SQL/docs into metric KB")
    parser.add_argument("input_file", help="Path to SQL file or text doc")
    parser.add_argument("--type", choices=["sql", "doc"], default="sql", help="Input type")
    parser.add_argument("--metrics-dir", default="metrics", help="Output metrics directory")
    parser.add_argument("--snippets-dir", default="snippets", help="Output snippets directory")
    parser.add_argument("--dry-run", action="store_true", help="Print analysis without writing files")
    args = parser.parse_args()

    with open(args.input_file) as f:
        content = f.read()

    analyzer = SQLAnalyzer()
    if args.type == "sql":
        results = analyzer.analyze_sql(content)
    else:
        results = analyzer.analyze_doc(content)

    if args.dry_run:
        import json
        print(json.dumps(results, indent=2))
        return

    generator = Generator(metrics_dir=args.metrics_dir, snippets_dir=args.snippets_dir)
    created = generator.generate(results)
    print(f"Created {len(created)} files:")
    for f in created:
        print(f"  {f}")


if __name__ == "__main__":
    main()
```

**Step 3: Commit**

```bash
git add raw/ src/importer/cli.py
git commit -m "feat: add importer CLI and raw input directory"
```

---

### Task 8: Clean up removed files and update __init__.py

**Files:**
- Remove: `src/extractor.py` (replaced by LLM client)
- Remove: `src/assembler.py` (replaced by LLM generation)
- Modify: `src/__init__.py` if needed
- Remove: old `tests/test_agent.py` mock-based tests (replaced by Task 3 tests)

**Step 1: Remove old files**

```bash
git rm src/extractor.py src/assembler.py
```

**Step 2: Run all unit tests**

Run: `python -m pytest tests/ -v -m "not live"`
Expected: All tests PASS (test_llm_client, test_prompt_builder, test_agent, test_analyzer, test_generator)

**Step 3: Commit**

```bash
git add -A
git commit -m "chore: remove old deterministic pipeline (extractor, assembler)"
```

---

### Task 9: End-to-end smoke test with live API

**Requires:** `OPENAI_API_KEY` environment variable set.

**Step 1: Run a manual test**

```bash
echo "ID market DAU in November 2025" | python -m src.agent
```

Expected: Should print SQL containing `traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live`, `grass_region = 'ID'`, and `avg(`.

**Step 2: Run regression suite (live)**

```bash
python -m pytest tests/test_regression.py -v -m live
```

Review output. If any tests fail, iterate on the system prompt in `src/prompt_builder.py` (adjust rules, add examples) and re-run until all pass.

**Step 3: Commit any prompt adjustments**

```bash
git add src/prompt_builder.py
git commit -m "fix: tune system prompt for regression test accuracy"
```

---

## Summary

| Task | What it builds | Tests |
|------|---------------|-------|
| 1 | OpenAI LLM client wrapper | 3 unit tests |
| 2 | Prompt builder (KB → system prompt) | 3 unit tests |
| 3 | Rewritten agent (LLM-based) | 2 unit tests |
| 4 | Importer analyzer (SQL/doc → structured data) | 2 unit tests |
| 5 | Importer generator (structured data → YAML/snippets) | 2 unit tests |
| 6 | Regression test suite | 6 parametrized tests |
| 7 | Importer CLI + raw directory | Manual |
| 8 | Cleanup old pipeline | All unit tests green |
| 9 | Live API smoke test | Regression suite (live) |
