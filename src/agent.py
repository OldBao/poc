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
        response = self.llm.chat(messages=list(self.messages))
        self.messages.append({"role": "assistant", "content": response})
        return response

    def follow_up(self, answer: str) -> str:
        """Continue the conversation with a follow-up answer."""
        self.messages.append({"role": "user", "content": answer})
        response = self.llm.chat(messages=list(self.messages))
        self.messages.append({"role": "assistant", "content": response})
        return response


def parse_response(raw: str) -> tuple:
    """Parse LLM response. Returns (type, data).

    type is one of: "sql", "ambiguous", "text"
    data is the parsed dict for sql/ambiguous, or the raw string for text.
    """
    import re
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n|```\s*$", "", text).strip()
    try:
        parsed = json.loads(text)
        return parsed.get("type", "text"), parsed
    except (json.JSONDecodeError, ValueError):
        return "text", raw.strip()


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


if __name__ == "__main__":
    main()
