import os
import yaml

OUTPUT_FORMAT = """
You must respond with ONLY valid JSON in one of these formats:

If the request is clear and you can generate SQL:
{
  "type": "sql",
  "sql": "SELECT ... (the complete SQL query)"
}

If the request is ambiguous (e.g., "revenue" could mean multiple metrics):
{
  "type": "ambiguous",
  "candidates": ["Candidate interpretation 1", "Candidate interpretation 2"]
}

If the metric is clear but required parameters are missing (market, date range, etc.):
{
  "type": "need_info",
  "metric": "The identified metric name",
  "missing": ["market", "date_range"],
  "message": "A short question asking for the missing info"
}
"""

SYSTEM_PREAMBLE = """You are an expert SQL generator for the S&R&A (Search, Recommendation & Ads) team at Shopee.

Given a user question about S&R&A metrics, generate the exact SQL query to answer it.

RULES:
- Use ONLY the tables, columns, and filters defined in the metric definitions below.
- Follow the reference SQL examples exactly for query patterns (aggregation style, date handling, filters).
- For monthly metrics, use: substr(cast(date_col as varchar), 1, 7) AS period, and avg() aggregation.
- Always include required filters (e.g., tz_type).
- If market is specified, add a grass_region = 'XX' filter.
- If the user says "all markets" or "all", do NOT add a grass_region filter — query across all markets and include grass_region in the GROUP BY.
- Date ranges use: BETWEEN date 'YYYY-MM-DD' AND date 'YYYY-MM-DD'
- When the user says a month name like "Nov" or "November 2025", infer the full date range (e.g., 2025-11-01 to 2025-11-30). Do NOT ask for clarification on date ranges when a month is given.
- For comparison queries (MoM, YoY), use a CTE with current_period and previous_period, and compute change_rate.
- If the question is ambiguous (could refer to multiple metrics), return ambiguous candidates instead of guessing.
- Return ONLY JSON. No explanations, no markdown outside JSON.

Available markets: ID, VN, TH, TW, BR, MX, PH, SG, MY, CO, CL
"""


class PromptBuilder:
    def __init__(self, metrics_dir: str = "metrics", snippets_dir: str = "snippets"):
        self.metrics_dir = metrics_dir
        self.snippets_dir = snippets_dir

    def build(self) -> str:
        sections = [SYSTEM_PREAMBLE]

        metrics_section = self._build_metrics_section()
        if metrics_section:
            sections.append("## Metric Definitions\n" + metrics_section)

        snippets_section = self._build_snippets_section()
        if snippets_section:
            sections.append("## Reference SQL Examples\n" + snippets_section)

        sections.append("## Output Format\n" + OUTPUT_FORMAT)

        return "\n\n".join(sections)

    def _build_metrics_section(self) -> str:
        if not os.path.isdir(self.metrics_dir):
            return ""
        parts = []
        for fname in sorted(os.listdir(self.metrics_dir)):
            if not fname.endswith((".yaml", ".yml")):
                continue
            path = os.path.join(self.metrics_dir, fname)
            with open(path) as f:
                data = yaml.safe_load(f)
            if data and "metric" in data:
                parts.append(self._format_metric(data["metric"]))
        return "\n".join(parts)

    def _format_metric(self, m: dict) -> str:
        lines = [f"### {m['name']}"]
        if m.get("aliases"):
            lines.append(f"Aliases: {', '.join(m['aliases'])}")
        lines.append(f"Type: {m['type']}")
        if m.get("formula"):
            lines.append(f"Formula: {m['formula']}")
        if m.get("aggregation"):
            lines.append(f"Aggregation: {m['aggregation']}")
        for source in m.get("sources", []):
            lines.append(f"Source table: {source['table']}")
            lines.append(f"  Columns: {source['columns']}")
            if source.get("filters"):
                lines.append(f"  Filters: {source['filters']}")
            if source.get("use_when"):
                lines.append(f"  Use when: {source['use_when']}")
        if m.get("snippet_file"):
            lines.append(f"Snippet: {m['snippet_file']}")
        if m.get("notes"):
            lines.append(f"Notes: {m['notes']}")
        dims = m.get("dimensions", {})
        lines.append(f"Required dimensions: {dims.get('required', [])}")
        lines.append(f"Optional dimensions: {dims.get('optional', [])}")
        lines.append("")
        return "\n".join(lines)

    def _build_snippets_section(self) -> str:
        if not os.path.isdir(self.snippets_dir):
            return ""
        parts = []
        for fname in sorted(os.listdir(self.snippets_dir)):
            if not fname.endswith(".sql"):
                continue
            path = os.path.join(self.snippets_dir, fname)
            with open(path) as f:
                sql = f.read().strip()
            name = fname.replace(".sql", "").replace("_", " ").title()
            parts.append(f"### {name}\n```sql\n{sql}\n```\n")
        return "\n".join(parts)
