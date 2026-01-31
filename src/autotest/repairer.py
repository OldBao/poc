import os
from dataclasses import dataclass, field
from typing import Optional
from src.llm_backend import LLMBackend


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
        backend: LLMBackend | None = None,
        metrics_dir: str = "metrics",
        snippets_dir: str = "snippets",
        # legacy compat
        llm_client=None,
    ):
        if llm_client is not None:
            self.llm = llm_client
            self._use_legacy = True
        elif backend is not None:
            self.llm = backend
            self._use_legacy = False
        else:
            raise ValueError("Either 'backend' or 'llm_client' must be provided")
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

        if self._use_legacy:
            result = self.llm.call(
                system_prompt=prompt,
                user_message="Propose a repair plan.",
            )
        else:
            result = self.llm.generate_json(
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
