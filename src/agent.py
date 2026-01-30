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
