import json
from typing import Optional
from src.registry import MetricRegistry

SYSTEM_PROMPT_TEMPLATE = """You are a metric query parser for the S&R&A (Search, Recommendation & Ads) team at Shopee.

Available metrics:
{metric_list}

Available dimensions:
- market: ID, VN, TH, TW, BR, MX, PH, SG, MY, CO, CL (country codes)
- date_range: supports YYYY-MM-DD dates, "last month", "November 2025", etc.
- compare_to: MoM (month-over-month), YoY (year-over-year), or specific period
- module: DD (Daily Discover), YMAL (You May Also Like), PP (Post Purchase), etc.

Parse the user question into this exact JSON format:
{{
  "intent": "query" | "compare" | "trend" | "breakdown",
  "metrics": ["metric_name"],
  "dimensions": {{
    "market": "XX" | null,
    "date_range": {{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}} | null,
    "compare_to": {{"type": "MoM" | "YoY" | "custom", "start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}} | null,
    "module": "XX" | null
  }},
  "clarification_needed": null | "string explaining what's ambiguous"
}}

Rules:
- Use exact metric names from the list above
- If the question is ambiguous (e.g., "revenue" could be multiple metrics), set clarification_needed
- For monthly data, use first and last day of month (e.g., 2025-11-01 to 2025-11-30)
- If no market specified, set market to null (means all markets)
- Return ONLY valid JSON, no other text"""


class IntentExtractor:
    def __init__(self, registry: MetricRegistry, model: str = "claude-sonnet-4-20250514"):
        self.registry = registry
        self.model = model

    def build_system_prompt(self) -> str:
        metric_list = "\n".join(
            f"- {entry}" for entry in self.registry.list_names_and_aliases()
        )
        return SYSTEM_PROMPT_TEMPLATE.format(metric_list=metric_list)

    def extract(self, question: str) -> dict:
        import anthropic
        client = anthropic.Anthropic()
        response = client.messages.create(
            model=self.model,
            max_tokens=1024,
            system=self.build_system_prompt(),
            messages=[{"role": "user", "content": question}],
        )
        raw = response.content[0].text
        return self.parse_response(raw)

    @staticmethod
    def parse_response(raw: str) -> dict:
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        return json.loads(text)
