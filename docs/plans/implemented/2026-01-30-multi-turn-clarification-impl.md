# Multi-Turn LLM Clarification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the hardcoded clarification loop with LLM-driven multi-turn conversation using message history.

**Architecture:** Add a `chat()` method to LLMClient that accepts a full message list. Rewrite Agent to maintain a message history per conversation — `start()` resets history, `follow_up()` appends to it. The CLI becomes a thin loop that checks if the LLM response is JSON (sql/ambiguous) or plain text (clarification). Remove `need_info` type and all manual context accumulation.

**Tech Stack:** Python 3.10+, OpenAI API (chat completions with message history)

---

### Task 1: Add `chat()` method to LLMClient

**Files:**
- Modify: `src/llm_client.py`
- Test: `tests/test_llm_client.py`

**Step 1: Write the failing test**

Add to `tests/test_llm_client.py`:

```python
def test_chat_returns_raw_response():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Which market do you need?"

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.chat(messages=[
            {"role": "system", "content": "You are a helper."},
            {"role": "user", "content": "rev?"},
        ])

    assert result == "Which market do you need?"
    mock_client.chat.completions.create.assert_called_once()
    call_kwargs = mock_client.chat.completions.create.call_args[1]
    assert call_kwargs["messages"] == [
        {"role": "system", "content": "You are a helper."},
        {"role": "user", "content": "rev?"},
    ]


def test_chat_returns_json_string_as_is():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '{"type": "sql", "sql": "SELECT 1"}'

    with patch("openai.OpenAI") as MockOpenAI:
        mock_client = MagicMock()
        MockOpenAI.return_value = mock_client
        mock_client.chat.completions.create.return_value = mock_response

        client = LLMClient(model="gpt-4o")
        result = client.chat(messages=[
            {"role": "system", "content": "test"},
            {"role": "user", "content": "DAU for ID Nov 2025"},
        ])

    assert result == '{"type": "sql", "sql": "SELECT 1"}'
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_llm_client.py -v -k "test_chat"`
Expected: FAIL with `AttributeError: 'LLMClient' object has no attribute 'chat'`

**Step 3: Write minimal implementation**

Add to `src/llm_client.py` in the `LLMClient` class:

```python
def chat(self, messages: list[dict]) -> str:
    """Send a full message list and return the raw response string."""
    response = self.client.chat.completions.create(
        model=self.model,
        temperature=0,
        messages=messages,
    )
    raw = response.choices[0].message.content
    if raw is None:
        raise ValueError("LLM returned empty response")
    return raw.strip()
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_llm_client.py -v`
Expected: All 5 tests PASS

**Step 5: Commit**

```bash
git add src/llm_client.py tests/test_llm_client.py
git commit -m "feat: add chat() method to LLMClient for multi-turn messages"
```

---

### Task 2: Rewrite Agent with message history

**Files:**
- Modify: `src/agent.py`
- Test: `tests/test_agent.py`

**Step 1: Write the failing tests**

Replace `tests/test_agent.py` entirely:

```python
# tests/test_agent.py
import json
from unittest.mock import patch, MagicMock
from src.agent import Agent


def test_start_sends_system_and_user_message():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.return_value = '{"type": "sql", "sql": "SELECT 1"}'

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        raw = agent.start("ID DAU Nov 2025")

    assert raw == '{"type": "sql", "sql": "SELECT 1"}'
    call_args = mock_llm.chat.call_args[1]
    messages = call_args["messages"]
    assert messages[0]["role"] == "system"
    assert messages[1] == {"role": "user", "content": "ID DAU Nov 2025"}
    assert len(messages) == 2


def test_start_resets_history():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.return_value = "Which market?"

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        agent.start("rev?")
        # Start a new conversation — history should reset
        agent.start("DAU?")

    # Second call should only have system + "DAU?", not "rev?"
    second_call_messages = mock_llm.chat.call_args[1]["messages"]
    assert len(second_call_messages) == 2
    assert second_call_messages[1]["content"] == "DAU?"


def test_follow_up_appends_to_history():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.side_effect = [
            "Which market and date range?",
            '{"type": "sql", "sql": "SELECT 1"}',
        ]

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        agent.start("Ads Gross Rev")
        raw = agent.follow_up("ID Nov 2025")

    assert raw == '{"type": "sql", "sql": "SELECT 1"}'
    # Second call should have full history: system, user, assistant, user
    messages = mock_llm.chat.call_args[1]["messages"]
    assert len(messages) == 4
    assert messages[0]["role"] == "system"
    assert messages[1] == {"role": "user", "content": "Ads Gross Rev"}
    assert messages[2] == {"role": "assistant", "content": "Which market and date range?"}
    assert messages[3] == {"role": "user", "content": "ID Nov 2025"}


def test_follow_up_accumulates_multiple_turns():
    with patch("src.agent.LLMClient") as MockLLM:
        mock_llm = MagicMock()
        MockLLM.return_value = mock_llm
        mock_llm.chat.side_effect = [
            "Which market?",
            "Which date range?",
            '{"type": "sql", "sql": "SELECT 1"}',
        ]

        agent = Agent(metrics_dir="metrics", snippets_dir="snippets")
        agent.start("Ads Gross Rev")
        agent.follow_up("ID")
        raw = agent.follow_up("Nov 2025")

    assert raw == '{"type": "sql", "sql": "SELECT 1"}'
    messages = mock_llm.chat.call_args[1]["messages"]
    assert len(messages) == 6  # system + 3 user + 2 assistant
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_agent.py -v`
Expected: FAIL with `AttributeError: 'Agent' object has no attribute 'start'`

**Step 3: Write minimal implementation**

Replace `src/agent.py` Agent class and remove old helpers:

```python
# src/agent.py
import json
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
        self.messages = []

    def start(self, question: str) -> str:
        """Start a new conversation. Resets history."""
        self.messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": question},
        ]
        response = self.llm.chat(messages=self.messages)
        self.messages.append({"role": "assistant", "content": response})
        return response

    def follow_up(self, answer: str) -> str:
        """Continue the conversation with a follow-up answer."""
        self.messages.append({"role": "user", "content": answer})
        response = self.llm.chat(messages=self.messages)
        self.messages.append({"role": "assistant", "content": response})
        return response
```

Keep `main()` as a stub for now (will be rewritten in Task 3):

```python
def main():
    print("CLI will be rewritten in Task 3")


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_agent.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/agent.py tests/test_agent.py
git commit -m "feat: rewrite Agent with multi-turn message history"
```

---

### Task 3: Rewrite CLI with multi-turn loop

**Files:**
- Modify: `src/agent.py` (add `main()` and `parse_response()`)
- Test: `tests/test_agent.py` (add CLI helper tests)

**Step 1: Write the failing tests**

Add to `tests/test_agent.py`:

```python
from src.agent import parse_response


def test_parse_response_sql_json():
    rtype, data = parse_response('{"type": "sql", "sql": "SELECT 1"}')
    assert rtype == "sql"
    assert data == {"type": "sql", "sql": "SELECT 1"}


def test_parse_response_ambiguous_json():
    rtype, data = parse_response('{"type": "ambiguous", "candidates": ["A", "B"]}')
    assert rtype == "ambiguous"
    assert data == {"type": "ambiguous", "candidates": ["A", "B"]}


def test_parse_response_plain_text():
    rtype, data = parse_response("Which market do you need?")
    assert rtype == "text"
    assert data == "Which market do you need?"


def test_parse_response_json_in_markdown_fences():
    rtype, data = parse_response('```json\n{"type": "sql", "sql": "SELECT 1"}\n```')
    assert rtype == "sql"
    assert data == {"type": "sql", "sql": "SELECT 1"}
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_agent.py -v -k "test_parse"`
Expected: FAIL with `ImportError: cannot import name 'parse_response'`

**Step 3: Write minimal implementation**

Add to `src/agent.py`:

```python
def parse_response(raw: str) -> tuple:
    """Parse LLM response. Returns (type, data).

    type is one of: "sql", "ambiguous", "text"
    data is the parsed dict for sql/ambiguous, or the raw string for text.
    """
    text = raw.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        import re
        text = re.sub(r"^```\w*\n|```\s*$", "", text).strip()
    try:
        parsed = json.loads(text)
        return parsed.get("type", "text"), parsed
    except (json.JSONDecodeError, ValueError):
        return "text", raw.strip()
```

Now rewrite `main()`:

```python
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

        try:
            raw = agent.start(question)
        except Exception as e:
            print(f"\nError: {e}")
            continue

        while True:
            rtype, data = parse_response(raw)

            if rtype == "sql":
                print(f"\n--- Generated SQL ---\n{data['sql']}")
                break

            if rtype == "ambiguous":
                candidates = data["candidates"]
                print("\nAmbiguous request. Did you mean:")
                for i, candidate in enumerate(candidates, 1):
                    print(f"  {i}. {candidate}")
                print("Select a number or rephrase your question.")
            else:
                # Plain text clarification from LLM
                print(f"\n{data}")

            try:
                answer = input("\nQ: ").strip()
            except (EOFError, KeyboardInterrupt):
                return
            if answer.lower() in ("quit", "exit", "q"):
                return
            if not answer:
                break

            # Handle numeric selection for ambiguous
            if rtype == "ambiguous" and answer.isdigit():
                idx = int(answer)
                if 1 <= idx <= len(candidates):
                    answer = candidates[idx - 1]

            try:
                raw = agent.follow_up(answer)
            except Exception as e:
                print(f"\nError: {e}")
                break
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_agent.py -v`
Expected: All 8 tests PASS

**Step 5: Commit**

```bash
git add src/agent.py tests/test_agent.py
git commit -m "feat: rewrite CLI with multi-turn conversation loop"
```

---

### Task 4: Update system prompt — remove `need_info`, add conversation instructions

**Files:**
- Modify: `src/prompt_builder.py`
- Test: `tests/test_prompt_builder.py`

**Step 1: Write the failing test**

Add to `tests/test_prompt_builder.py`:

```python
def test_prompt_has_conversation_instructions():
    pb = PromptBuilder(metrics_dir="metrics", snippets_dir="snippets")
    prompt = pb.build()
    assert "ask in plain text" in prompt.lower() or "ask the user" in prompt.lower()
    assert "need_info" not in prompt
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_prompt_builder.py::test_prompt_has_conversation_instructions -v`
Expected: FAIL — `need_info` is still in the prompt

**Step 3: Write minimal implementation**

In `src/prompt_builder.py`, replace `OUTPUT_FORMAT`:

```python
OUTPUT_FORMAT = """
You must respond in one of these ways:

1. If you can generate the SQL query, respond with ONLY this JSON:
{
  "type": "sql",
  "sql": "SELECT ... (the complete SQL query)"
}

2. If the request is ambiguous (could refer to multiple metrics), respond with ONLY this JSON:
{
  "type": "ambiguous",
  "candidates": ["Candidate interpretation 1", "Candidate interpretation 2"]
}

3. If you need clarification (market, date range, or other details), ask in plain text. Do NOT return JSON. Just ask a short, natural question like "Which market and date range?" The user will reply and you can continue the conversation.

IMPORTANT: Only return JSON when you are ready to output the final SQL or when the metric is ambiguous. For all other cases, ask in plain text.
"""
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_prompt_builder.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/prompt_builder.py tests/test_prompt_builder.py
git commit -m "feat: update prompt for multi-turn conversation, remove need_info"
```

---

### Task 5: Run full test suite and regression tests

**Files:**
- No new files

**Step 1: Run all unit tests**

Run: `python -m pytest tests/ --ignore=tests/test_regression.py -v`
Expected: All tests PASS (should be ~12 tests: 5 llm_client + 4 agent + 4 prompt_builder + others)

**Step 2: Run live regression suite**

Run: `python -m pytest tests/test_regression.py -v -m live`

Note: `test_regression.py` calls `agent.ask()` which no longer exists. It needs to be updated to use `agent.start()` instead.

**Step 3: Update regression test runner**

In `tests/test_regression.py`, change line 30:

```python
# OLD:
result = agent.ask(case["question"])

# NEW:
raw = agent.start(case["question"])
rtype, data = parse_response(raw)
result = data if isinstance(data, dict) else {"type": "text", "message": data}
```

And add the import at top:

```python
from src.agent import Agent, parse_response
```

**Step 4: Run regression suite again**

Run: `python -m pytest tests/test_regression.py -v -m live`
Expected: All 6 tests PASS

**Step 5: Commit**

```bash
git add tests/test_regression.py
git commit -m "fix: update regression tests for multi-turn Agent API"
```

---

### Task 6: Manual smoke test and cleanup

**Step 1: Test the full multi-turn flow**

Run: `printf "rev?\n1\nall market Nov 2025\nquit\n" | python -m src.agent`

Expected output:
```
S&R&A Metric Agent (type 'quit' to exit)
--------------------------------------------------

Q:
Ambiguous request. Did you mean:
  1. Ads Gross Rev
  2. Net Ads Rev
  ...
Select a number or rephrase your question.

Q:
[LLM asks for market/date in plain text]

Q:
--- Generated SQL ---
SELECT ...
```

**Step 2: Verify no dead code remains**

Check that these are gone from `src/agent.py`:
- `_read_input()`
- `_is_quit()`
- `_handle_result()`
- `ask()` method on Agent

**Step 3: Final commit if any cleanup needed**

```bash
git add -A
git commit -m "chore: cleanup after multi-turn refactor"
```
