# Self-Correction Loop + Dynamic Few-shot Design

Date: 2026-01-31
Status: Design

## Overview

Two optimizations to the SQL agent based on Text-to-SQL research (DAIL-SQL, DIN-SQL, ReFoRCE):

1. **Self-Correction Loop** — When generated SQL fails validation or execution, feed the error back to the LLM and retry (up to 2 times)
2. **Dynamic Few-shot** — Retrieve similar Q-SQL examples from a ChromaDB vector store and inject them into the prompt at query time

## 1. Self-Correction Loop

### Current behavior

`Agent.ask()` validates the LLM response via `SQLValidator`. If errors are found, they are returned directly to the user as `{"type": "error", ...}`. The LLM never sees its own mistakes.

### New behavior

After the LLM generates SQL:

1. **Static validation** (existing `SQLValidator`): syntax, table existence, filter values
2. **Execution validation** (new): run SQL against a read-only DB connection
3. If either fails, inject the error as a user message into the conversation and call the LLM again
4. Max **2 retries**, then surface the error to the user

### Dialect selection

The user picks the SQL dialect at CLI startup via `--dialect` flag:
- `presto` (default)
- `sparksql`
- `hive`

This determines:
- The SQL dialect hint passed to the LLM in the system prompt
- The connection used for execution validation

### New file: `src/db_executor.py`

```python
class DBExecutor:
    """Read-only SQL execution for validation."""

    def __init__(self, dialect: str, connection_config: dict):
        """
        dialect: 'presto' | 'sparksql' | 'hive'
        connection_config: host, port, catalog, schema, etc.
        """

    def execute(self, sql: str, timeout: int = 30) -> dict:
        """
        Execute SQL and return:
        - {"success": True, "rows": int, "columns": [...]}
        - {"success": False, "error": "error message"}

        Uses LIMIT 1 wrapper to avoid fetching large result sets.
        Read-only: rejects INSERT/UPDATE/DELETE/DROP/ALTER.
        """

    def test_connection(self) -> bool:
        """Verify the connection works at startup."""
```

Connection libraries:
- Presto: `prestodb` or `pyhive[presto]`
- Hive: `pyhive[hive]`
- SparkSQL: `pyhive[hive]` (same Thrift protocol)

### Changes to `src/agent.py`

```python
class Agent:
    def __init__(self, ..., dialect: str = "presto",
                 db_executor: DBExecutor | None = None):
        self.db_executor = db_executor
        self.dialect = dialect
        # Add dialect to system prompt

    def ask(self, question: str, max_retries: int = 2) -> dict:
        self.messages.append({"role": "user", "content": question})

        for attempt in range(max_retries + 1):
            raw = self.llm.chat(self.messages)
            self.messages.append({"role": "assistant", "content": raw})
            result = self._parse_response(raw)

            if result["type"] != "error":
                # Static validation passed. Try execution if available.
                if result["type"] == "sql" and self.db_executor:
                    exec_result = self.db_executor.execute(result["sql"])
                    if not exec_result["success"]:
                        result = {"type": "error",
                                  "message": exec_result["error"]}

            if result["type"] != "error" or attempt == max_retries:
                return result

            # Feed error back to LLM for correction
            error_msg = (
                f"The SQL has errors: {result['message']}. "
                f"Please fix and return corrected JSON."
            )
            self.messages.append({"role": "user", "content": error_msg})

        return result
```

### CLI changes

```
python -m src.agent --dialect presto --db-host presto.example.com --db-port 8080
python -m src.agent --dialect hive --db-host hive.example.com
python -m src.agent --dialect sparksql --db-host spark.example.com
```

When no `--db-host` is provided, execution validation is skipped (static validation only). This keeps the tool usable offline.

## 2. Dynamic Few-shot with ChromaDB

### Core idea

Before building the prompt, query a ChromaDB collection for the top-5 Q-SQL pairs most similar to the user's question. Inject them into the system prompt as examples.

Research backing: DAIL-SQL found that question-SQL pair format works best, and dynamically selected examples significantly outperform fixed ones.

### New file: `src/example_store.py`

```python
class ExampleStore:
    """ChromaDB-backed Q-SQL example store with sentence-transformer embeddings."""

    def __init__(self, persist_dir: str = "data/chromadb",
                 model_name: str = "all-MiniLM-L6-v2"):
        """
        Initializes ChromaDB persistent client and loads
        sentence-transformers model for embeddings.
        """

    def add(self, question: str, sql: str,
            metric_name: str = "", dialect: str = "presto"):
        """
        Upsert a Q-SQL pair. Uses a hash of the question as doc ID
        for idempotency.
        """

    def search(self, question: str, top_k: int = 5) -> list[dict]:
        """
        Return top-k similar examples as:
        [{"question": ..., "sql": ..., "metric_name": ..., "dialect": ...}]
        """

    def remove(self, question: str):
        """Remove a Q-SQL pair by question text."""

    def seed_from_yaml(self, yaml_path: str):
        """
        Bulk load Q-SQL pairs from test_cases.yaml or benchmark.yaml.
        Expects format with 'question' and 'expected_sql' fields.
        Idempotent — safe to run multiple times.
        """

    def count(self) -> int:
        """Return total number of examples in the store."""
```

Storage: `data/chromadb/` directory, added to `.gitignore`.

Embedding model: `sentence-transformers/all-MiniLM-L6-v2` (local, ~80MB, no API cost).

### Changes to `src/prompt_builder.py`

Add method to inject few-shot examples into the prompt:

```python
class PromptBuilder:
    def build_with_examples(self, examples: list[dict]) -> str:
        """
        Same as build(), but inserts a few-shot section between
        'Reference SQL Examples' and 'Conversation Instructions'.

        Format per example:
            Q: {question}
            SQL: {sql}
        """
```

### Changes to `src/agent.py`

At startup, initialize the ExampleStore. Before each LLM call, retrieve similar examples and rebuild the system prompt:

```python
class Agent:
    def __init__(self, ..., example_store: ExampleStore | None = None):
        self.example_store = example_store or ExampleStore()

    def ask(self, question: str) -> dict:
        # Retrieve similar examples
        examples = self.example_store.search(question, top_k=5)

        # Rebuild system prompt with examples
        self.messages[0]["content"] = self.prompt_builder.build_with_examples(examples)

        # ... rest of ask() with retry loop
```

Note: the system prompt is rebuilt on every query because the few-shot examples are dynamic. The cost is minimal (prompt_builder is fast).

## 3. Auto-capture + /save / /unsave

### Implicit save

Track the last successful Q-SQL pair:

```python
self._last_successful: tuple[str, str] | None = None  # (question, sql)
```

When the user sends a new question and `_last_successful` is set:
- If the new message doesn't look like a correction (doesn't start with "wrong", "fix", "error", "no", "incorrect"), auto-save the previous pair to the example store
- Clear `_last_successful`

### Explicit commands

| Command | Action |
|---------|--------|
| `/save` | Immediately save `_last_successful` to the example store, confirm to user |
| `/unsave` | Remove the last auto-saved pair from ChromaDB |

These are handled alongside existing `reset`, `quit`, `exit` commands in the CLI loop.

### Seeding

A CLI subcommand to seed the example store from existing YAML test cases:

```
python -m src.agent seed
```

This calls `example_store.seed_from_yaml()` on `tests/test_cases.yaml` and `tests/benchmark.yaml`.

## 4. File changes summary

### New files

| File | Purpose |
|------|---------|
| `src/example_store.py` | ChromaDB wrapper for Q-SQL example storage and retrieval |
| `src/db_executor.py` | Read-only DB execution for validation (Presto/Hive/SparkSQL) |
| `data/chromadb/` | ChromaDB persistent storage (gitignored) |

### Modified files

| File | Changes |
|------|---------|
| `src/agent.py` | Retry loop, few-shot injection, /save /unsave, dialect, auto-capture |
| `src/prompt_builder.py` | `build_with_examples()` method |
| `requirements.txt` | Add chromadb, sentence-transformers, pyhive |
| `.gitignore` | Add `data/chromadb/` |

### Unchanged files

`src/validator.py`, `src/registry.py`, `src/models.py`, `src/rule_engine.py`, `src/llm_client.py`, `src/value_index.py`

## 5. New dependencies

| Package | Purpose |
|---------|---------|
| `chromadb` | Vector store for Q-SQL examples |
| `sentence-transformers` | Local embedding model (all-MiniLM-L6-v2) |
| `pyhive[presto,hive]` | Presto/Hive/SparkSQL connections |

## 6. Implementation order

1. `src/db_executor.py` — new file, no dependencies on other changes
2. `src/example_store.py` — new file, no dependencies on other changes
3. `src/prompt_builder.py` — add `build_with_examples()`
4. `src/agent.py` — integrate retry loop, few-shot, auto-capture, /save /unsave
5. `requirements.txt` + `.gitignore` — add deps
6. Tests for new modules
7. Seed example store from existing test cases
