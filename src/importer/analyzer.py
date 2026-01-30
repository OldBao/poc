import json
import openai

SQL_ANALYZER_PROMPT = """You are an expert SQL analyst. Given an ETL SQL query, extract all metrics defined in it.

For each metric, return a JSON array of objects with these fields:
- name: Human-readable metric name (e.g., "DAU", "Ads Gross Rev")
- aliases: List of alternative names
- type: "simple" (single table, direct aggregation) or "complex" (multi-table joins, CASE WHEN logic)
- table: Primary source table (full schema.table name)
- columns: Dict of column mappings (value, date, region, etc.)
- filters: List of required WHERE clauses (e.g., "tz_type = 'local'")
- aggregation: Aggregation function (avg, sum, count, etc.)
- snippet: The isolated SQL fragment for this metric, with {{ date_start }}, {{ date_end }}, {{ market }} as placeholders
- notes: Any business rules or edge cases

Return ONLY a JSON array. No explanations."""

DOC_ANALYZER_PROMPT = """You are an expert data analyst. Given a document describing business metrics, extract metric definitions.

For each metric, return a JSON array of objects with these fields:
- name: Human-readable metric name
- aliases: List of alternative names
- type: "simple" or "complex"
- notes: Business rules, formulas, edge cases described in the document

Include any field from the SQL schema if the document provides enough information:
- table, columns, filters, aggregation, snippet

Return ONLY a JSON array. No explanations."""


class SQLAnalyzer:
    def __init__(self, model: str = "gpt-4o"):
        self.model = model
        self.client = openai.OpenAI()

    def _call_llm(self, system_prompt: str, content: str) -> list[dict]:
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content},
            ],
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1])
        return json.loads(raw)

    def analyze_sql(self, sql: str) -> list[dict]:
        return self._call_llm(SQL_ANALYZER_PROMPT, sql)

    def analyze_doc(self, doc_text: str) -> list[dict]:
        return self._call_llm(DOC_ANALYZER_PROMPT, doc_text)
