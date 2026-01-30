# src/agent.py
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


def _read_input():
    """Read user input, returning None on EOF/interrupt."""
    try:
        return input("\nQ: ").strip()
    except (EOFError, KeyboardInterrupt):
        return None


def _is_quit(text):
    return text.lower() in ("quit", "exit", "q")


def _handle_result(agent, result):
    """Handle a result, prompting for follow-up if needed. Returns True to continue, False to quit."""
    while True:
        rtype = result.get("type")

        if rtype == "sql":
            print(f"\n--- Generated SQL ---\n{result['sql']}")
            return True

        if rtype == "ambiguous":
            candidates = result["candidates"]
            print("\nAmbiguous request. Did you mean:")
            for i, candidate in enumerate(candidates, 1):
                print(f"  {i}. {candidate}")
            print("Select a number or rephrase your question.")
            follow_up = _read_input()
            if follow_up is None or _is_quit(follow_up):
                return False
            if not follow_up:
                return True
            if follow_up.isdigit() and 1 <= int(follow_up) <= len(candidates):
                follow_up = candidates[int(follow_up) - 1]

        elif rtype == "need_info":
            print(f"\n{result.get('message', 'Please provide more details.')}")
            follow_up = _read_input()
            if follow_up is None or _is_quit(follow_up):
                return False
            if not follow_up:
                return True
            # Combine metric context with user's answer
            metric = result.get("metric", "")
            follow_up = f"{metric} — user provided: {follow_up}. Generate the SQL query."

        else:
            print(f"\nUnexpected response: {result}")
            return True

        try:
            result = agent.ask(follow_up)
        except Exception as e:
            print(f"\nError: {e}")
            return True


def main():
    agent = Agent()
    print("S&R&A Metric Agent (type 'quit' to exit)")
    print("-" * 50)
    while True:
        question = _read_input()
        if question is None or _is_quit(question):
            break
        if not question:
            continue

        try:
            result = agent.ask(question)
        except Exception as e:
            print(f"\nError: {e}")
            continue

        if not _handle_result(agent, result):
            break


if __name__ == "__main__":
    main()
