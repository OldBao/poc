import os
from datetime import date

import yaml


SYSTEM_PREAMBLE = """You are an expert SQL generator for the S&R&A (Search, Recommendation & Ads) team at Shopee.

Today's date is {today}. When the user mentions a month without a year, default to {current_year}. When the user mentions a month that is in the future relative to today, use the previous year.

Given user questions about S&R&A metrics, generate the exact SQL query to answer them.

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

Available markets: ID, VN, TH, TW, BR, MX, PH, SG, MY, CO, CL, AR
"""

CONVERSATION_INSTRUCTIONS = """
## Conversation Instructions

You are in a multi-turn conversation. Follow these rules:

OUTPUT FORMAT:
- When you can generate SQL, respond with ONLY this JSON:
  {"type": "sql", "sql": "SELECT ..."}
- When the user asks for multiple metrics, respond with ONLY this JSON:
  {"type": "sql_list", "queries": [{"metric": "Metric Name", "sql": "SELECT ..."}, ...]}
- When the query is ambiguous between metrics, respond with ONLY this JSON:
  {"type": "ambiguous", "candidates": ["Metric A", "Metric B"]}
- When you need more information (market, date range, etc.), ask in plain text.
  Be brief: "Which market and date range?"

IMPORTANT: Only return JSON when you are ready to output the final SQL or when the metric is ambiguous. For all other cases, ask in plain text.

CONTEXT CARRY-OVER:
- Remember the metric, market, and date range from earlier turns.
- If the user says "same for TH", keep the metric and date, change the market.
- If the user says "change to October", keep the metric and market, change the date.
- If the user says "break down by channel", switch to the channel breakdown metric for the same market and date.
- If the user says "what about GMV?", switch to GMV keeping the same market and date.

DERIVED METRICS:
- For ratio metrics (e.g. Gross Take Rate = Ads Gross Rev / GMV), generate a single SQL using CTEs for each sub-metric and compute the ratio in the final SELECT.

MISSING DIMENSIONS:
- If the user provides no date range, ask for one. Do NOT generate SQL with empty dates.
- If the user provides no market, generate for all markets (no grass_region filter, include grass_region in GROUP BY).

CONDITIONAL RULES:
- Apply the conditional adjustment rules listed below when the market and metric tags match.
- For example, BR market + net revenue metrics require the SCS credit adjustment.
"""


class PromptBuilder:
    def __init__(
        self,
        metrics_dir: str = "metrics",
        snippets_dir: str = "snippets",
        rules_dir: str = "rules",
    ):
        self.metrics_dir = metrics_dir
        self.snippets_dir = snippets_dir
        self.rules_dir = rules_dir

    def build(self) -> str:
        today = date.today()
        sections = [SYSTEM_PREAMBLE.format(
            today=today.isoformat(),
            current_year=today.year,
        )]

        metrics_section = self._build_metrics_section()
        if metrics_section:
            sections.append("## Metric Definitions\n" + metrics_section)

        snippets_section = self._build_snippets_section()
        if snippets_section:
            sections.append("## Reference SQL Examples\n" + snippets_section)

        rules_section = self._build_rules_section()
        if rules_section:
            sections.append("## Conditional Adjustment Rules\n" + rules_section)

        sections.append(CONVERSATION_INSTRUCTIONS)

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
        if m.get("tags"):
            lines.append(f"Tags: {', '.join(m['tags'])}")
        if m.get("formula"):
            lines.append(f"Formula: {m['formula']}")
        if m.get("depends_on"):
            lines.append(f"Depends on: {', '.join(m['depends_on'])}")
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
        for root, _dirs, files in os.walk(self.snippets_dir):
            for fname in sorted(files):
                if not fname.endswith(".sql"):
                    continue
                path = os.path.join(root, fname)
                rel_path = os.path.relpath(path, self.snippets_dir)
                with open(path) as f:
                    sql = f.read().strip()
                name = rel_path.replace(".sql", "").replace("_", " ").replace("/", " / ").title()
                parts.append(f"### {name}\n```sql\n{sql}\n```\n")
        return "\n".join(parts)

    def _build_rules_section(self) -> str:
        if not os.path.isdir(self.rules_dir):
            return ""
        parts = []
        for fname in sorted(os.listdir(self.rules_dir)):
            if not fname.endswith((".yaml", ".yml")):
                continue
            path = os.path.join(self.rules_dir, fname)
            with open(path) as f:
                data = yaml.safe_load(f)
            if data and "rule" in data:
                parts.append(self._format_rule(data["rule"]))
        return "\n".join(parts)

    def _format_rule(self, r: dict) -> str:
        lines = [f"### {r['name']}"]
        if r.get("description"):
            lines.append(r["description"].strip())
        when = r.get("when", {})
        conditions = []
        if "market" in when:
            conditions.append(f"market = {when['market']}")
        if "metric_tags" in when:
            conditions.append(f"metric tags include {when['metric_tags']}")
        if conditions:
            lines.append(f"When: {' AND '.join(conditions)}")
        if r.get("valid_from"):
            lines.append(f"Valid from: {r['valid_from']}")
        effect = r.get("effect", {})
        lines.append(f"Effect: {effect.get('type', 'unknown')}")
        if effect.get("snippet_file"):
            snippet_path = effect["snippet_file"]
            try:
                with open(snippet_path) as f:
                    snippet_sql = f.read().strip()
                lines.append(f"Adjustment SQL:\n```sql\n{snippet_sql}\n```")
            except FileNotFoundError:
                lines.append(f"Snippet file: {snippet_path}")
        if effect.get("join_keys"):
            lines.append(f"Join keys: {', '.join(effect['join_keys'])}")
        lines.append("")
        return "\n".join(lines)
