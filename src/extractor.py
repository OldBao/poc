from dataclasses import dataclass
from datetime import date
from typing import Optional

from src.llm_client import LLMClient


EXTRACTION_PROMPT = """You are an intent and entity extractor for S&R&A metrics questions.

Today's date is {today}. When the user mentions a month without a year, default to the current year ({current_year}). When the user mentions a month that is in the future relative to today, use the previous year.

Given a user question, extract:
- intent: "query" (single metric lookup), "compare" (MoM/YoY comparison), "trend" (time series), "breakdown" (by channel/module)
- metrics: list of metric names mentioned (use canonical names from the list below)
- dimensions: market, date_range, compare_to, module
- clarification_needed: if the question is ambiguous, explain what needs clarifying. Otherwise null.

Available metrics:
{metric_names}

Available markets: ID, VN, TH, TW, BR, MX, PH, SG, MY, CO, CL, AR

Respond with ONLY valid JSON:
{{
  "intent": "query|compare|trend|breakdown",
  "metrics": ["metric_name"],
  "dimensions": {{
    "market": "XX" or null,
    "date_range": {{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}} or null,
    "compare_to": {{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}} or null,
    "module": "name" or null
  }},
  "clarification_needed": null or "explanation"
}}
"""


@dataclass
class ExtractionResult:
    intent: str
    metrics: list[str]
    dimensions: dict
    clarification_needed: Optional[str] = None


class Extractor:
    def __init__(self, llm_client: LLMClient, metric_names: list[str]):
        self.llm = llm_client
        today = date.today()
        self.system_prompt = EXTRACTION_PROMPT.format(
            metric_names="\n".join(f"- {n}" for n in metric_names),
            today=today.isoformat(),
            current_year=today.year,
        )

    def extract(self, question: str) -> ExtractionResult:
        result = self.llm.call(
            system_prompt=self.system_prompt,
            user_message=question,
        )
        return ExtractionResult(
            intent=result["intent"],
            metrics=result.get("metrics", []),
            dimensions=result.get("dimensions", {}),
            clarification_needed=result.get("clarification_needed"),
        )
