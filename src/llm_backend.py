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
    """Backend that shells out to the `claude` CLI (single-turn only)."""

    def generate(self, system_prompt: str, user_message: str) -> str:
        import subprocess

        result = subprocess.run(
            ["claude", "-p", "--output-format", "text"],
            input=f"{system_prompt}\n\n{user_message}",
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"claude CLI failed (rc={result.returncode}): {result.stderr.strip()}")
        output = result.stdout.strip()
        if not output:
            raise ValueError("Claude Code returned empty response")
        return output


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
