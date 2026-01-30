import json
from typing import Optional

from src.llm_client import LLMClient
from src.prompt_builder import PromptBuilder
from src.validator import SQLValidator
from src.registry import MetricRegistry
from src.value_index import ValueIndex


class Agent:
    def __init__(
        self,
        metrics_dir: str = "metrics",
        snippets_dir: str = "snippets",
        rules_dir: str = "rules",
        value_index_path: str = "value_index.db",
        model: str = "gpt-4o",
        llm_client: Optional[LLMClient] = None,
    ):
        self.registry = MetricRegistry(metrics_dir=metrics_dir)
        self.registry.load()

        self.value_index = ValueIndex(value_index_path)
        self.value_index.init_db()

        self.llm = llm_client or LLMClient(model=model)

        self.prompt_builder = PromptBuilder(
            metrics_dir=metrics_dir,
            snippets_dir=snippets_dir,
            rules_dir=rules_dir,
        )
        self.validator = SQLValidator(
            registry=self.registry,
            value_index=self.value_index,
        )

        self.system_prompt = self.prompt_builder.build()
        self.messages: list[dict] = [
            {"role": "system", "content": self.system_prompt}
        ]

    def ask(self, question: str) -> dict:
        self.messages.append({"role": "user", "content": question})
        raw = self.llm.chat(self.messages)
        self.messages.append({"role": "assistant", "content": raw})
        return self._parse_response(raw)

    def _parse_response(self, raw: str) -> dict:
        stripped = LLMClient._strip_fences(raw)
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                if parsed.get("type") == "sql":
                    errors = self.validator.validate(parsed["sql"])
                    if errors:
                        return {"type": "error", "message": "; ".join(errors)}
                    return parsed
                if parsed.get("type") == "sql_list":
                    for q in parsed.get("queries", []):
                        errors = self.validator.validate(q.get("sql", ""))
                        if errors:
                            return {
                                "type": "error",
                                "message": f"{q.get('metric', '?')}: {'; '.join(errors)}",
                            }
                    return parsed
                if parsed.get("type") == "ambiguous":
                    return {
                        "type": "clarification",
                        "message": "Which did you mean?",
                        "candidates": parsed.get("candidates", []),
                    }
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
        # Plain text = LLM is asking a clarification question
        return {"type": "clarification", "message": raw}

    def reset(self):
        """Start a new conversation, keep system prompt."""
        self.messages = [self.messages[0]]


def _print_sql(sql: str) -> None:
    """Print SQL with syntax highlighting if pygments is available."""
    try:
        from pygments import highlight
        from pygments.lexers import SqlLexer
        from pygments.formatters import TerminalTrueColorFormatter

        print(highlight(sql, SqlLexer(), TerminalTrueColorFormatter(style="monokai")))
    except ImportError:
        print(sql)


def main():
    from prompt_toolkit import prompt as pt_prompt
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.formatted_text import HTML
    from prompt_toolkit.styles import Style
    from prompt_toolkit.key_binding.bindings.emacs import load_emacs_bindings
    import os

    # Explicitly load emacs bindings to ensure Ctrl+A, Ctrl+E, etc. work
    emacs_bindings = load_emacs_bindings()

    style = Style.from_dict({
        "prompt": "#00aa00 bold",
        "": "#ffffff",
    })

    history_file = os.path.expanduser("~/.sra_agent_history")
    history = FileHistory(history_file)

    agent = Agent()

    print("\033[1;36mS&R&A Metric Agent\033[0m (type 'quit' to exit, 'reset' for new conversation)")
    print("\033[90m" + "-" * 50 + "\033[0m")

    while True:
        try:
            question = pt_prompt(
                HTML("<prompt>Q: </prompt>"),
                style=style,
                history=history,
                key_bindings=emacs_bindings,
                enable_open_in_editor=True,
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if question.lower() in ("quit", "exit", "q"):
            break
        if question.lower() == "reset":
            agent.reset()
            print("\033[90m(conversation reset)\033[0m")
            continue
        if not question:
            continue

        result = agent.ask(question)

        if result["type"] == "sql":
            print("\n\033[1;32m--- Generated SQL ---\033[0m")
            _print_sql(result["sql"])
        elif result["type"] == "sql_list":
            for q in result["queries"]:
                print(f"\n\033[1;32m--- {q['metric']} ---\033[0m")
                _print_sql(q["sql"])
        elif result["type"] == "clarification":
            print(f"\n\033[1;33m{result['message']}\033[0m")
            candidates = result.get("candidates")
            if candidates:
                for i, c in enumerate(candidates, 1):
                    print(f"  \033[1;36m{i}.\033[0m {c}")
        elif result["type"] == "error":
            print(f"\n\033[1;31mError:\033[0m {result['message']}")
        else:
            print(f"\nUnexpected: {result}")


if __name__ == "__main__":
    main()
