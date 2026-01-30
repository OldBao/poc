import json
import re
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
        raw = response.choices[0].message.content
        if raw is None:
            raise ValueError("LLM returned empty response")
        return self._parse(raw.strip())

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

    def call_raw(self, system_prompt: str, user_message: str) -> list:
        """Call LLM and return parsed JSON list (for analyzer use)."""
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        raw = response.choices[0].message.content
        if raw is None:
            raise ValueError("LLM returned empty response")
        return json.loads(self._strip_fences(raw.strip()))

    @staticmethod
    def _strip_fences(text: str) -> str:
        text = text.strip()
        return re.sub(r"^```\w*\n|```\s*$", "", text).strip()

    @staticmethod
    def _parse(raw: str) -> dict:
        text = LLMClient._strip_fences(raw)
        return json.loads(text)
