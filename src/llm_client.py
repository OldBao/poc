import json
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
        raw = response.choices[0].message.content.strip()
        return self._parse(raw)

    @staticmethod
    def _parse(raw: str) -> dict:
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        return json.loads(text)
