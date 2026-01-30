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


def main():
    print("CLI will be rewritten in Task 3")


if __name__ == "__main__":
    main()
