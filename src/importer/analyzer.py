from src.llm_backend import LLMBackend, OpenAIBackend

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
    def __init__(self, backend: LLMBackend | None = None, model: str = "gpt-4o"):
        self.backend = backend or OpenAIBackend(model=model)

    def analyze_sql(self, sql: str) -> list[dict]:
        return self.backend.generate_json_list(SQL_ANALYZER_PROMPT, sql)

    def analyze_doc(self, doc_text: str) -> list[dict]:
        return self.backend.generate_json_list(DOC_ANALYZER_PROMPT, doc_text)
