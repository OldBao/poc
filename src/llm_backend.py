"""LLM Backend abstraction — supports OpenAI and Claude Code backends."""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod

import openai


def strip_fences(text: str) -> str:
    """Remove Markdown code fences from LLM output."""
    text = text.strip()
    return re.sub(r"^```\w*\n|```\s*$", "", text).strip()


class LLMBackend(ABC):
    """Protocol for LLM backends used by Agent and other components."""

    @abstractmethod
    def generate(self, system_prompt: str, user_message: str) -> str:
        """Send system_prompt + user_message, return raw response string."""

    # ── convenience helpers built on generate() ──

    def generate_json(self, system_prompt: str, user_message: str) -> dict:
        """generate() → strip fences → parse as JSON dict."""
        raw = self.generate(system_prompt, user_message)
        return json.loads(strip_fences(raw))

    def generate_json_list(self, system_prompt: str, user_message: str) -> list:
        """generate() → strip fences → parse as JSON list."""
        raw = self.generate(system_prompt, user_message)
        return json.loads(strip_fences(raw))


class OpenAIBackend(LLMBackend):
    """Backend wrapping the OpenAI chat-completions API."""

    def __init__(
        self,
        model: str = "gpt-4o",
        base_url: str | None = None,
        api_key: str | None = None,
    ):
        self.model = model
        if base_url:
            self.client = openai.OpenAI(
                base_url=base_url,
                api_key=api_key or openai.api_key or "ollama",
            )
        else:
            self.client = openai.OpenAI(api_key=api_key) if api_key else openai.OpenAI()

    def generate(self, system_prompt: str, user_message: str) -> str:
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
        return raw.strip()

    def chat(self, messages: list[dict]) -> str:
        """Send a full message list (for multi-turn conversations)."""
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=messages,
        )
        raw = response.choices[0].message.content
        if raw is None:
            raise ValueError("LLM returned empty response")
        return raw.strip()


class ClaudeCodeBackend(LLMBackend):
    """Backend using the Claude Code SDK (single-turn only)."""

    def __init__(self):
        from claude_code_sdk import query as claude_query, ClaudeCodeOptions

        self._query = claude_query
        self._options_cls = ClaudeCodeOptions

    def generate(self, system_prompt: str, user_message: str) -> str:
        import asyncio

        return asyncio.run(self._agenerate(system_prompt, user_message))

    async def _agenerate(self, system_prompt: str, user_message: str) -> str:
        parts: list[str] = []
        async for msg in self._query(
            prompt=user_message,
            options=self._options_cls(system_prompt=system_prompt),
        ):
            if msg.type == "text":
                parts.append(msg.content)
        result = "".join(parts)
        if not result:
            raise ValueError("Claude Code returned empty response")
        return result.strip()


def create_backend(backend: str = "claude", **kwargs) -> LLMBackend:
    """Factory: create an LLMBackend from a backend name.

    Parameters
    ----------
    backend : "openai" | "claude"
    **kwargs : forwarded to backend constructor (model, base_url, api_key for openai)
    """
    if backend == "openai":
        return OpenAIBackend(**kwargs)
    if backend == "claude":
        return ClaudeCodeBackend()
    raise ValueError(f"Unknown backend: {backend!r}. Choose 'openai' or 'claude'.")
