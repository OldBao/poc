# Self-Correction Loop + Dynamic Few-shot — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add two optimizations to the SQL agent: (1) a self-correction loop that retries failed SQL with error feedback, and (2) dynamic few-shot example retrieval from a ChromaDB vector store seeded from benchmark cases and growing via auto-capture.

**Architecture:** Self-correction wraps the existing `Agent.ask()` with a 2-retry loop that feeds validator errors and DB execution errors back into the conversation. Dynamic few-shot queries ChromaDB with the user's question before each LLM call, injecting top-5 similar Q-SQL pairs into the system prompt. Auto-capture implicitly saves successful Q-SQL pairs; `/save` and `/unsave` give manual control.

**Tech Stack:** Python 3.10+, ChromaDB, sentence-transformers (all-MiniLM-L6-v2), PyHive (Presto/Hive/SparkSQL), existing `src/` modules

**Design doc:** [design](2026-01-31-self-correction-and-fewshot-design.md)

---

### Task 1: DB Executor

**Files:**
- Create: `src/db_executor.py`
- Create: `tests/test_db_executor.py`

**Step 1: Write the failing test**

```python
# tests/test_db_executor.py
import pytest
from unittest.mock import MagicMock, patch
from src.db_executor import DBExecutor


def test_rejects_write_statements():
    executor = DBExecutor(dialect="presto", connection_config={})
    for sql in [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN x INT",
    ]:
        result = executor.execute(sql)
        assert not result["success"]
        assert "read-only" in result["error"].lower()


def test_wraps_with_limit():
    executor = DBExecutor(dialect="presto", connection_config={})
    wrapped = executor._wrap_with_limit("SELECT * FROM t", limit=1)
    assert "LIMIT 1" in wrapped


def test_wraps_with_limit_strips_trailing_semicolon():
    executor = DBExecutor(dialect="presto", connection_config={})
    wrapped = executor._wrap_with_limit("SELECT * FROM t;", limit=1)
    assert wrapped.strip().endswith("LIMIT 1")
    assert ";" not in wrapped


def test_execute_returns_success_on_valid_sql():
    executor = DBExecutor(dialect="presto", connection_config={})
    mock_cursor = MagicMock()
    mock_cursor.description = [("n",), ("m",)]
    mock_cursor.fetchall.return_value = [[1, 2]]

    with patch.object(executor, "_get_cursor", return_value=mock_cursor):
        result = executor.execute("SELECT 1 AS n, 2 AS m")

    assert result["success"] is True
    assert result["columns"] == ["n", "m"]
    assert result["rows"] == 1


def test_execute_returns_error_on_exception():
    executor = DBExecutor(dialect="presto", connection_config={})
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Column 'bad' not found")

    with patch.object(executor, "_get_cursor", return_value=mock_cursor):
        result = executor.execute("SELECT bad FROM t")

    assert result["success"] is False
    assert "bad" in result["error"]


def test_test_connection_success():
    executor = DBExecutor(dialect="presto", connection_config={})
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [1]

    with patch.object(executor, "_get_cursor", return_value=mock_cursor):
        assert executor.test_connection() is True


def test_test_connection_failure():
    executor = DBExecutor(dialect="presto", connection_config={})

    with patch.object(executor, "_get_cursor", side_effect=Exception("Connection refused")):
        assert executor.test_connection() is False
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_db_executor.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.db_executor'"

**Step 3: Write minimal implementation**

```python
# src/db_executor.py
import re


WRITE_PATTERN = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE)\b",
    re.IGNORECASE,
)


class DBExecutor:
    """Read-only SQL execution for validation against Presto/Hive/SparkSQL."""

    def __init__(self, dialect: str, connection_config: dict):
        """
        dialect: 'presto' | 'sparksql' | 'hive'
        connection_config: dict with keys like host, port, catalog, schema, username
        """
        self.dialect = dialect
        self.config = connection_config

    def execute(self, sql: str, timeout: int = 30) -> dict:
        """
        Execute SQL and return:
        - {"success": True, "rows": int, "columns": [...]}
        - {"success": False, "error": "error message"}
        """
        if WRITE_PATTERN.match(sql):
            return {"success": False, "error": "Read-only mode: write statements are not allowed"}

        wrapped = self._wrap_with_limit(sql, limit=1)

        try:
            cursor = self._get_cursor()
            cursor.execute(wrapped)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return {"success": True, "rows": len(rows), "columns": columns}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def test_connection(self) -> bool:
        """Verify the connection works."""
        try:
            cursor = self._get_cursor()
            cursor.execute("SELECT 1")
            return cursor.fetchone() is not None
        except Exception:
            return False

    def _get_cursor(self):
        """Create a DB cursor based on dialect."""
        if self.dialect == "presto":
            from pyhive import presto
            conn = presto.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 8080),
                catalog=self.config.get("catalog", "hive"),
                schema=self.config.get("schema", "default"),
                username=self.config.get("username", ""),
            )
            return conn.cursor()
        elif self.dialect in ("hive", "sparksql"):
            from pyhive import hive
            conn = hive.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 10000),
                database=self.config.get("schema", "default"),
                username=self.config.get("username", ""),
            )
            return conn.cursor()
        else:
            raise ValueError(f"Unsupported dialect: {self.dialect}")

    def _wrap_with_limit(self, sql: str, limit: int = 1) -> str:
        """Wrap SQL with LIMIT to avoid fetching large result sets."""
        sql = sql.strip().rstrip(";")
        return f"SELECT * FROM ({sql}) _validation_wrapper LIMIT {limit}"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_db_executor.py -v`
Expected: PASS (7 tests)

**Step 5: Commit**

```bash
git add src/db_executor.py tests/test_db_executor.py
git commit -m "feat: add read-only DB executor for Presto/Hive/SparkSQL"
```

---

### Task 2: Example Store (ChromaDB)

**Files:**
- Create: `src/example_store.py`
- Create: `tests/test_example_store.py`

**Step 1: Write the failing test**

```python
# tests/test_example_store.py
import os
import pytest
import tempfile
import yaml
from src.example_store import ExampleStore


@pytest.fixture
def store(tmp_path):
    return ExampleStore(persist_dir=str(tmp_path / "chromadb"))


def test_add_and_search(store):
    store.add(
        question="ID market DAU in November 2025",
        sql="SELECT avg(a1) AS dau FROM traffic.t WHERE grass_region = 'ID'",
        metric_name="DAU",
    )
    results = store.search("What is ID DAU in Nov 2025?", top_k=1)
    assert len(results) == 1
    assert "DAU" in results[0]["sql"] or "dau" in results[0]["sql"]


def test_add_is_idempotent(store):
    for _ in range(3):
        store.add(
            question="ID market DAU in November 2025",
            sql="SELECT avg(a1) FROM t",
        )
    assert store.count() == 1


def test_search_returns_top_k(store):
    for i in range(10):
        store.add(
            question=f"Question variant {i} about DAU",
            sql=f"SELECT {i} FROM t",
        )
    results = store.search("DAU question", top_k=5)
    assert len(results) == 5


def test_remove(store):
    store.add(question="test question", sql="SELECT 1")
    assert store.count() == 1
    store.remove("test question")
    assert store.count() == 0


def test_seed_from_yaml(store, tmp_path):
    cases = {
        "cases": [
            {
                "id": "case1",
                "question": "ID DAU in Nov 2025",
                "expected_sql": "SELECT avg(a1) FROM t WHERE region = 'ID'",
                "tags": ["dau"],
            },
            {
                "id": "case2",
                "question": "TH GMV in Dec 2025",
                "expected_sql": "SELECT sum(gmv) FROM t WHERE region = 'TH'",
                "tags": ["gmv"],
            },
            {
                "id": "edge_case",
                "question": "What is revenue?",
                "expected_sql": "",
                "tags": ["edge"],
            },
        ]
    }
    yaml_path = tmp_path / "benchmark.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(cases, f)

    store.seed_from_yaml(str(yaml_path))
    # Edge case with empty SQL should be skipped
    assert store.count() == 2


def test_search_empty_store(store):
    results = store.search("anything", top_k=5)
    assert results == []
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_example_store.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.example_store'"

**Step 3: Write minimal implementation**

```python
# src/example_store.py
import hashlib
import yaml
import chromadb
from chromadb.utils import embedding_functions


class ExampleStore:
    """ChromaDB-backed Q-SQL example store with sentence-transformer embeddings."""

    def __init__(
        self,
        persist_dir: str = "data/chromadb",
        model_name: str = "all-MiniLM-L6-v2",
    ):
        self._client = chromadb.PersistentClient(path=persist_dir)
        self._ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=model_name,
        )
        self._collection = self._client.get_or_create_collection(
            name="examples",
            embedding_function=self._ef,
        )

    def add(
        self,
        question: str,
        sql: str,
        metric_name: str = "",
        dialect: str = "presto",
    ):
        """Upsert a Q-SQL pair. Uses hash of question as doc ID for idempotency."""
        doc_id = self._question_id(question)
        self._collection.upsert(
            ids=[doc_id],
            documents=[question],
            metadatas=[{
                "sql": sql,
                "metric_name": metric_name,
                "dialect": dialect,
            }],
        )

    def search(self, question: str, top_k: int = 5) -> list[dict]:
        """Return top-k similar examples."""
        if self._collection.count() == 0:
            return []
        actual_k = min(top_k, self._collection.count())
        results = self._collection.query(
            query_texts=[question],
            n_results=actual_k,
        )
        examples = []
        for i in range(len(results["ids"][0])):
            meta = results["metadatas"][0][i]
            examples.append({
                "question": results["documents"][0][i],
                "sql": meta.get("sql", ""),
                "metric_name": meta.get("metric_name", ""),
                "dialect": meta.get("dialect", "presto"),
            })
        return examples

    def remove(self, question: str):
        """Remove a Q-SQL pair by question text."""
        doc_id = self._question_id(question)
        self._collection.delete(ids=[doc_id])

    def seed_from_yaml(self, yaml_path: str):
        """Bulk load Q-SQL pairs from benchmark.yaml. Skips entries with empty SQL."""
        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        cases = data.get("cases", data) if isinstance(data, dict) else data
        for case in cases:
            question = case.get("question", "")
            sql = case.get("expected_sql", "").strip()
            if not question or not sql:
                continue
            self.add(
                question=question,
                sql=sql,
                metric_name=case.get("id", ""),
            )

    def count(self) -> int:
        """Return total number of examples in the store."""
        return self._collection.count()

    @staticmethod
    def _question_id(question: str) -> str:
        return hashlib.sha256(question.strip().lower().encode()).hexdigest()[:16]
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_example_store.py -v`
Expected: PASS (7 tests)

**Step 5: Commit**

```bash
git add src/example_store.py tests/test_example_store.py
git commit -m "feat: add ChromaDB example store with sentence-transformer embeddings"
```

---

### Task 3: Prompt Builder — Few-shot Injection

**Files:**
- Modify: `src/prompt_builder.py`
- Modify: `tests/test_prompt_builder.py`

**Step 1: Write the failing test**

Add to `tests/test_prompt_builder.py`:

```python
def test_build_with_examples_injects_fewshot_section():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        builder = PromptBuilder(
            metrics_dir=metrics_dir,
            snippets_dir=snippets_dir,
            rules_dir=rules_dir,
        )
        examples = [
            {"question": "ID DAU in Nov 2025", "sql": "SELECT avg(a1) FROM t"},
            {"question": "TH GMV in Dec 2025", "sql": "SELECT sum(gmv) FROM t"},
        ]
        prompt = builder.build_with_examples(examples)

        assert "## Similar Examples" in prompt
        assert "Q: ID DAU in Nov 2025" in prompt
        assert "SQL: SELECT avg(a1) FROM t" in prompt
        assert "Q: TH GMV in Dec 2025" in prompt


def test_build_with_examples_empty_list_same_as_build():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        builder = PromptBuilder(
            metrics_dir=metrics_dir,
            snippets_dir=snippets_dir,
            rules_dir=rules_dir,
        )
        prompt_no_examples = builder.build()
        prompt_empty = builder.build_with_examples([])

        assert prompt_no_examples == prompt_empty


def test_build_with_examples_appears_before_conversation_instructions():
    with tempfile.TemporaryDirectory() as tmpdir:
        metrics_dir = os.path.join(tmpdir, "metrics")
        snippets_dir = os.path.join(tmpdir, "snippets")
        rules_dir = os.path.join(tmpdir, "rules")
        os.makedirs(metrics_dir)
        os.makedirs(snippets_dir)
        os.makedirs(rules_dir)

        builder = PromptBuilder(
            metrics_dir=metrics_dir,
            snippets_dir=snippets_dir,
            rules_dir=rules_dir,
        )
        examples = [{"question": "Q1", "sql": "SELECT 1"}]
        prompt = builder.build_with_examples(examples)

        examples_pos = prompt.index("## Similar Examples")
        conversation_pos = prompt.index("## Conversation Instructions")
        assert examples_pos < conversation_pos
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_prompt_builder.py -v`
Expected: FAIL — `AttributeError: 'PromptBuilder' object has no attribute 'build_with_examples'`

**Step 3: Add method to `src/prompt_builder.py`**

Add at the end of the `PromptBuilder` class:

```python
    def build_with_examples(self, examples: list[dict]) -> str:
        """Same as build(), but inserts few-shot examples before Conversation Instructions."""
        if not examples:
            return self.build()

        today = date.today()
        sections = [SYSTEM_PREAMBLE.format(
            today=today.isoformat(),
            current_year=today.year,
        )]

        metrics_section = self._build_metrics_section()
        if metrics_section:
            sections.append("## Metric Definitions\n" + metrics_section)

        snippets_section = self._build_snippets_section()
        if snippets_section:
            sections.append("## Reference SQL Examples\n" + snippets_section)

        rules_section = self._build_rules_section()
        if rules_section:
            sections.append("## Conditional Adjustment Rules\n" + rules_section)

        # Inject few-shot examples
        examples_text = self._format_examples(examples)
        sections.append("## Similar Examples\n" + examples_text)

        sections.append(CONVERSATION_INSTRUCTIONS)

        return "\n\n".join(sections)

    @staticmethod
    def _format_examples(examples: list[dict]) -> str:
        parts = []
        for ex in examples:
            parts.append(f"Q: {ex['question']}\nSQL: {ex['sql']}")
        return "\n\n".join(parts)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_prompt_builder.py -v`
Expected: PASS (all existing + 3 new tests)

**Step 5: Commit**

```bash
git add src/prompt_builder.py tests/test_prompt_builder.py
git commit -m "feat: add build_with_examples() for dynamic few-shot injection"
```

---

### Task 4: Agent — Self-Correction Loop + Few-shot + Auto-capture

**Files:**
- Modify: `src/agent.py`
- Modify: `tests/test_agent.py`

**Step 1: Write the failing tests**

Add to `tests/test_agent.py`:

```python
from unittest.mock import MagicMock, patch, call


def test_self_correction_retries_on_validation_error():
    """Agent should retry when validator finds errors, feeding error back to LLM."""
    mock_llm = MagicMock()
    # First call: bad SQL, second call: fixed SQL
    mock_llm.chat.side_effect = [
        '{"type": "sql", "sql": "SELECT * FROM bad.unknown_table"}',
        '{"type": "sql", "sql": "SELECT avg(a1) FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_region = \'ID\'"}',
    ]

    agent = Agent(llm_client=mock_llm)
    result = agent.ask("ID DAU in Nov 2025")

    assert result["type"] == "sql"
    assert mock_llm.chat.call_count == 2
    # Verify error feedback was injected
    messages = mock_llm.chat.call_args_list[1][0][0]
    error_msg = [m for m in messages if m["role"] == "user" and "errors" in m.get("content", "").lower()]
    assert len(error_msg) > 0


def test_self_correction_gives_up_after_max_retries():
    """Agent should give up after 2 retries and return the error."""
    mock_llm = MagicMock()
    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT * FROM bad.table"}'

    agent = Agent(llm_client=mock_llm)
    result = agent.ask("ID DAU in Nov 2025")

    assert result["type"] == "error"
    # 1 initial + 2 retries = 3 calls
    assert mock_llm.chat.call_count == 3


def test_self_correction_with_db_executor():
    """Agent should try execution after static validation passes."""
    mock_llm = MagicMock()
    valid_sql = "SELECT avg(a1) FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_region = 'ID'"
    mock_llm.chat.side_effect = [
        f'{{"type": "sql", "sql": "{valid_sql}"}}',
        f'{{"type": "sql", "sql": "{valid_sql}"}}',
    ]

    mock_executor = MagicMock()
    mock_executor.execute.side_effect = [
        {"success": False, "error": "Column 'bad_col' not found"},
        {"success": True, "rows": 1, "columns": ["dau"]},
    ]

    agent = Agent(llm_client=mock_llm, db_executor=mock_executor)
    result = agent.ask("ID DAU")

    assert mock_executor.execute.call_count == 2


def test_fewshot_examples_injected():
    """Agent should query example store and rebuild system prompt with examples."""
    mock_llm = MagicMock()
    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT 1"}'

    mock_store = MagicMock()
    mock_store.search.return_value = [
        {"question": "ID DAU in Nov", "sql": "SELECT avg(a1) FROM t"},
    ]

    agent = Agent(llm_client=mock_llm, example_store=mock_store)
    result = agent.ask("ID DAU in Dec 2025")

    mock_store.search.assert_called_once()
    # System prompt should contain examples
    messages = mock_llm.chat.call_args[0][0]
    system_msg = messages[0]["content"]
    assert "Similar Examples" in system_msg


def test_save_command(tmp_path):
    """Agent /save should save last successful Q-SQL to example store."""
    mock_llm = MagicMock()
    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT 1"}'

    mock_store = MagicMock()
    mock_store.search.return_value = []

    agent = Agent(llm_client=mock_llm, example_store=mock_store)
    agent.ask("test question")  # generates _last_successful
    result = agent.handle_command("/save")

    assert result is not None
    mock_store.add.assert_called_once()


def test_unsave_command():
    """Agent /unsave should remove last saved pair."""
    mock_llm = MagicMock()
    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT 1"}'

    mock_store = MagicMock()
    mock_store.search.return_value = []

    agent = Agent(llm_client=mock_llm, example_store=mock_store)
    agent.ask("test question")
    agent.handle_command("/save")
    result = agent.handle_command("/unsave")

    assert result is not None
    mock_store.remove.assert_called_once()


def test_implicit_save_on_next_question():
    """When user asks a new question, the previous successful Q-SQL should be auto-saved."""
    mock_llm = MagicMock()
    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT 1"}'

    mock_store = MagicMock()
    mock_store.search.return_value = []

    agent = Agent(llm_client=mock_llm, example_store=mock_store)
    agent.ask("first question")
    agent.ask("second question")  # should trigger auto-save of first

    # add should have been called for auto-save
    assert mock_store.add.called


def test_no_implicit_save_on_correction():
    """When user corrects, should NOT auto-save the previous failed attempt."""
    mock_llm = MagicMock()
    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT 1"}'

    mock_store = MagicMock()
    mock_store.search.return_value = []

    agent = Agent(llm_client=mock_llm, example_store=mock_store)
    agent.ask("first question")
    agent.ask("wrong, fix the SQL")  # correction — should skip auto-save

    assert not mock_store.add.called
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent.py -v`
Expected: FAIL — current Agent doesn't accept `db_executor`, `example_store`, or have `handle_command`

**Step 3: Modify `src/agent.py`**

Key changes to the `Agent` class:

1. Add `db_executor`, `example_store`, `dialect` constructor params
2. Add `_last_successful` and `_last_saved` tracking
3. Rewrite `ask()` with retry loop and few-shot injection
4. Add `handle_command()` for `/save` and `/unsave`
5. Add implicit save logic

The `ask()` method becomes:

```python
    def ask(self, question: str, max_retries: int = 2) -> dict:
        # Auto-save previous successful result (implicit)
        self._maybe_auto_save(question)

        # Retrieve similar examples and rebuild system prompt
        examples = []
        if self.example_store:
            examples = self.example_store.search(question, top_k=5)
        if examples:
            self.messages[0]["content"] = self.prompt_builder.build_with_examples(examples)
        else:
            self.messages[0]["content"] = self.system_prompt

        self.messages.append({"role": "user", "content": question})

        for attempt in range(max_retries + 1):
            raw = self.llm.chat(self.messages)
            self.messages.append({"role": "assistant", "content": raw})
            result = self._parse_response(raw)

            if result["type"] == "error":
                if attempt < max_retries:
                    error_msg = (
                        f"The SQL has errors: {result['message']}. "
                        f"Please fix and return corrected JSON."
                    )
                    self.messages.append({"role": "user", "content": error_msg})
                    continue
                return result

            # Static validation passed. Try execution if available.
            if result["type"] == "sql" and self.db_executor:
                exec_result = self.db_executor.execute(result["sql"])
                if not exec_result["success"]:
                    if attempt < max_retries:
                        error_msg = (
                            f"The SQL has errors: {exec_result['error']}. "
                            f"Please fix and return corrected JSON."
                        )
                        self.messages.append({"role": "user", "content": error_msg})
                        continue
                    return {"type": "error", "message": exec_result["error"]}

            # Success
            if result["type"] == "sql":
                self._last_successful = (question, result["sql"])
            return result

        return result
```

The command handler:

```python
    _CORRECTION_PREFIXES = ("wrong", "fix", "error", "no,", "no ", "incorrect")

    def handle_command(self, command: str) -> dict | None:
        cmd = command.strip().lower()
        if cmd == "/save":
            if self._last_successful and self.example_store:
                q, sql = self._last_successful
                self.example_store.add(question=q, sql=sql)
                self._last_saved = q
                return {"type": "info", "message": f"Saved example: {q[:50]}..."}
            return {"type": "info", "message": "Nothing to save."}
        if cmd == "/unsave":
            if self._last_saved and self.example_store:
                self.example_store.remove(self._last_saved)
                msg = f"Removed example: {self._last_saved[:50]}..."
                self._last_saved = None
                return {"type": "info", "message": msg}
            return {"type": "info", "message": "Nothing to unsave."}
        return None

    def _maybe_auto_save(self, new_question: str):
        """Implicitly save previous successful Q-SQL if the new message isn't a correction."""
        if not self._last_successful or not self.example_store:
            return
        lower = new_question.strip().lower()
        if any(lower.startswith(p) for p in self._CORRECTION_PREFIXES):
            self._last_successful = None
            return
        q, sql = self._last_successful
        self.example_store.add(question=q, sql=sql)
        self._last_saved = q
        self._last_successful = None
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent.py -v`
Expected: PASS (all existing + 8 new tests)

**Step 5: Commit**

```bash
git add src/agent.py tests/test_agent.py
git commit -m "feat: add self-correction loop, few-shot injection, and auto-capture"
```

---

### Task 5: CLI Updates (dialect, DB connection, seed, /save, /unsave)

**Files:**
- Modify: `src/agent.py` (the `main()` function and CLI loop)

**Step 1: Add CLI arguments**

Add to `argparse` in `main()`:

```python
    parser.add_argument("--dialect", default="presto",
                        choices=["presto", "sparksql", "hive"],
                        help="SQL dialect (default: presto)")
    parser.add_argument("--db-host", default=None,
                        help="DB host for execution validation (optional)")
    parser.add_argument("--db-port", type=int, default=None,
                        help="DB port (default: 8080 for presto, 10000 for hive/sparksql)")
    parser.add_argument("--db-catalog", default="hive",
                        help="Presto catalog (default: hive)")
    parser.add_argument("--db-schema", default="default",
                        help="DB schema (default: default)")
    parser.add_argument("--db-user", default="",
                        help="DB username")
    parser.add_argument("seed", nargs="?", default=None,
                        help="Run 'seed' to populate example store from benchmark.yaml")
```

**Step 2: Handle seed subcommand**

```python
    if args.seed == "seed":
        from src.example_store import ExampleStore
        store = ExampleStore()
        for yaml_path in ["tests/benchmark.yaml", "tests/test_cases.yaml"]:
            if os.path.exists(yaml_path):
                store.seed_from_yaml(yaml_path)
                print(f"Seeded from {yaml_path}")
        print(f"Total examples: {store.count()}")
        return
```

**Step 3: Initialize DB executor and example store**

```python
    db_executor = None
    if args.db_host:
        from src.db_executor import DBExecutor
        port = args.db_port or (8080 if args.dialect == "presto" else 10000)
        db_executor = DBExecutor(
            dialect=args.dialect,
            connection_config={
                "host": args.db_host,
                "port": port,
                "catalog": args.db_catalog,
                "schema": args.db_schema,
                "username": args.db_user,
            },
        )
        if db_executor.test_connection():
            print(f"Connected to {args.dialect} at {args.db_host}:{port}")
        else:
            print(f"Warning: could not connect to {args.db_host}:{port}, execution validation disabled")
            db_executor = None

    from src.example_store import ExampleStore
    example_store = ExampleStore()

    agent = Agent(
        model=model,
        base_url=base_url,
        db_executor=db_executor,
        example_store=example_store,
        dialect=args.dialect,
    )
```

**Step 4: Handle /save and /unsave in the CLI loop**

In the `while True` loop, before calling `agent.ask()`:

```python
        if question.startswith("/"):
            cmd_result = agent.handle_command(question)
            if cmd_result:
                print(f"\n\033[90m{cmd_result['message']}\033[0m")
                continue
```

**Step 5: Manual test**

```bash
# Seed the example store
python -m src.agent seed

# Run without DB connection (static validation only)
python -m src.agent --dialect presto

# Run with DB connection
python -m src.agent --dialect presto --db-host presto.example.com
```

**Step 6: Commit**

```bash
git add src/agent.py
git commit -m "feat: add CLI flags for dialect, DB connection, seed, /save, /unsave"
```

---

### Task 6: Dependencies and Gitignore

**Files:**
- Modify: `requirements.txt`
- Modify: `.gitignore`

**Step 1: Update requirements.txt**

Add:
```
chromadb>=0.4
sentence-transformers>=2.2
pyhive[presto,hive]>=0.7
```

**Step 2: Update .gitignore**

Add:
```
data/chromadb/
```

**Step 3: Commit**

```bash
git add requirements.txt .gitignore
git commit -m "chore: add chromadb, sentence-transformers, pyhive deps; gitignore chromadb data"
```

---

### Task 7: Full Test Suite + Integration Test

**Files:**
- Create: `tests/test_self_correction_integration.py`

**Step 1: Write integration test**

```python
# tests/test_self_correction_integration.py
"""Integration test for the self-correction + few-shot pipeline."""
import pytest
from unittest.mock import MagicMock
from src.agent import Agent
from src.example_store import ExampleStore


@pytest.fixture
def mock_llm():
    return MagicMock()


@pytest.fixture
def example_store(tmp_path):
    return ExampleStore(persist_dir=str(tmp_path / "chromadb"))


def test_full_pipeline_fewshot_helps_generation(mock_llm, example_store):
    """Add examples, then verify they appear in the LLM prompt."""
    example_store.add(
        question="ID DAU in November 2025",
        sql="SELECT avg(a1) FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_region = 'ID'",
    )

    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT avg(a1) FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_region = \'TH\'"}'

    agent = Agent(llm_client=mock_llm, example_store=example_store)
    result = agent.ask("TH DAU in December 2025")

    assert result["type"] == "sql"
    # Verify the system prompt had examples injected
    messages = mock_llm.chat.call_args[0][0]
    assert "Similar Examples" in messages[0]["content"]


def test_full_pipeline_self_correction_fixes_error(mock_llm, example_store):
    """First attempt fails validation, second attempt succeeds."""
    mock_llm.chat.side_effect = [
        '{"type": "sql", "sql": "SELECT * FROM bad.table"}',
        '{"type": "sql", "sql": "SELECT avg(a1) FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_region = \'ID\'"}',
    ]

    agent = Agent(llm_client=mock_llm, example_store=example_store)
    result = agent.ask("ID DAU in November 2025")

    assert result["type"] == "sql"
    assert mock_llm.chat.call_count == 2


def test_full_pipeline_auto_capture_and_reuse(mock_llm, example_store):
    """Successful query gets auto-saved, then retrieved for similar question."""
    mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT avg(a1) FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_region = \'ID\'"}'

    agent = Agent(llm_client=mock_llm, example_store=example_store)

    # First question — generates SQL
    agent.ask("ID DAU in November 2025")

    # Second question — triggers auto-save of first
    agent.ask("TH DAU in December 2025")

    # Verify example was saved
    assert example_store.count() == 1

    # Verify it can be retrieved
    results = example_store.search("ID DAU", top_k=1)
    assert len(results) == 1
```

**Step 2: Run integration test**

Run: `pytest tests/test_self_correction_integration.py -v`
Expected: PASS (3 tests)

**Step 3: Run full suite**

Run: `pytest tests/ --ignore=tests/test_regression.py -v`
Expected: All tests pass

**Step 4: Commit**

```bash
git add tests/test_self_correction_integration.py
git commit -m "test: add integration tests for self-correction + few-shot pipeline"
```

---

### Task 8: Seed Example Store + Final Verification

**Step 1: Seed the example store from benchmark data**

```bash
python -m src.agent seed
```

Expected output:
```
Seeded from tests/benchmark.yaml
Seeded from tests/test_cases.yaml
Total examples: ~20  (25 benchmark cases minus edge cases with empty SQL)
```

**Step 2: Run full unit test suite**

```bash
pytest tests/ --ignore=tests/test_regression.py -v
```

Expected: All pass

**Step 3: Copy design doc into worktree**

```bash
cp docs/plans/2026-01-31-self-correction-and-fewshot-design.md docs/plans/
git add docs/plans/2026-01-31-self-correction-and-fewshot-design.md docs/plans/2026-01-31-self-correction-and-fewshot-impl.md docs/FEATURELIST.md
git commit -m "docs: add self-correction + few-shot design and implementation plans"
```

---

## Summary

| Task | What it builds | Tests |
|------|---------------|-------|
| 1 | DB Executor (read-only Presto/Hive/SparkSQL) | 7 unit tests |
| 2 | Example Store (ChromaDB + sentence-transformers) | 7 unit tests |
| 3 | Prompt Builder — `build_with_examples()` | 3 unit tests |
| 4 | Agent — retry loop, few-shot, auto-capture, /save, /unsave | 8 unit tests |
| 5 | CLI — dialect, DB connection, seed command | Manual |
| 6 | Dependencies + gitignore | — |
| 7 | Integration tests | 3 integration tests |
| 8 | Seed + final verification | Manual |
