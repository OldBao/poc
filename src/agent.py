from typing import Optional

from src.llm_client import LLMClient
from src.registry import MetricRegistry
from src.extractor import Extractor
from src.resolver import Resolver
from src.assembler import Assembler
from src.validator import SQLValidator
from src.value_index import ValueIndex
from src.prompt_builder import PromptBuilder
from src.rule_engine import RuleEngine
from src.atomic_assembler import AtomicAssembler
from src.models import QueryIntent


class Agent:
    def __init__(
        self,
        metrics_dir: str = "metrics",
        snippets_dir: str = "snippets",
        templates_dir: str = "templates",
        rules_dir: str = "rules",
        value_index_path: str = "value_index.db",
        model: str = "gpt-4o",
        llm_client: Optional[LLMClient] = None,
    ):
        self.registry = MetricRegistry(metrics_dir=metrics_dir)
        self.registry.load()

        self.value_index = ValueIndex(value_index_path)
        self.value_index.init_db()

        self.llm = llm_client or LLMClient(model=model)

        self.extractor = Extractor(
            llm_client=self.llm,
            metric_names=self.registry.list_names_and_aliases(),
        )
        self.resolver = Resolver(
            registry=self.registry,
            value_index=self.value_index,
        )
        self.assembler = Assembler(templates_dir=templates_dir)
        self.validator = SQLValidator(
            registry=self.registry,
            value_index=self.value_index,
        )
        self.prompt_builder = PromptBuilder(
            metrics_dir=metrics_dir,
            snippets_dir=snippets_dir,
        )
        self.snippets_dir = snippets_dir

        self.rule_engine = RuleEngine(rules_dir=rules_dir)
        self.rule_engine.load()

        self.atomic_assembler = AtomicAssembler()

    def ask(self, question: str) -> dict:
        # Step 1: Extract intent and entities
        extraction = self.extractor.extract(question)

        if extraction.clarification_needed:
            return {"type": "clarification", "message": extraction.clarification_needed}

        # Step 2: Resolve metric and source
        resolution = self.resolver.resolve(extraction)

        if resolution.errors:
            return {"type": "error", "message": "; ".join(resolution.errors)}

        metric = resolution.metric

        # Step 3: Generate SQL based on metric type
        if metric.type == "simple":
            sql = self._handle_simple(metric, resolution.source, extraction)
        elif metric.type == "complex":
            sql = self._handle_complex(metric, extraction, question)
        elif metric.type == "derived":
            sql = self._handle_derived(metric, extraction)
        elif metric.type == "atomic":
            sql = self._handle_atomic(metric, extraction)
        else:
            return {"type": "error", "message": f"Unknown metric type: {metric.type}"}

        if isinstance(sql, dict):
            return sql  # Error dict

        return {"type": "sql", "sql": sql}

    def _handle_simple(self, metric, source, extraction) -> str | dict:
        dims = extraction.dimensions
        date_range = dims.get("date_range", {})
        compare_to = dims.get("compare_to")

        if extraction.intent == "compare" and compare_to:
            return self.assembler.render_compare(
                metric=metric,
                source=source,
                current_start=date_range.get("start", ""),
                current_end=date_range.get("end", ""),
                previous_start=compare_to.get("start", ""),
                previous_end=compare_to.get("end", ""),
                market=dims.get("market"),
            )

        return self.assembler.render_simple(
            metric=metric,
            source=source,
            date_start=date_range.get("start", ""),
            date_end=date_range.get("end", ""),
            market=dims.get("market"),
        )

    def _handle_complex(self, metric, extraction, question: str, retry: bool = False) -> str | dict:
        dims = extraction.dimensions
        snippet_path = metric.snippet_file
        if not snippet_path:
            return {"type": "error", "message": f"No snippet file for complex metric '{metric.name}'"}

        with open(snippet_path) as f:
            base_snippet = f.read()

        # Match rules based on query context
        date_range = dims.get("date_range", {})
        matched_rules = self.rule_engine.match(
            market=dims.get("market"),
            metric_tags=getattr(metric, "tags", []),
            query_date_start=date_range.get("start"),
        )

        # Build assembly context
        assembly_context = self.rule_engine.build_context(
            base_snippet=base_snippet,
            matched_rules=matched_rules,
        )

        # Build prompt from assembly context
        system_prompt = self.prompt_builder.build_assembled_prompt(
            assembly_context,
            metric_name=metric.name,
        )

        user_msg = f"Question: {question}\nMarket: {dims.get('market', 'all')}\nDate range: {date_range}"
        result = self.llm.call(system_prompt=system_prompt, user_message=user_msg)

        sql = result.get("sql", "") if isinstance(result, dict) else str(result)

        errors = self.validator.validate(sql)
        if errors and not retry:
            return self._handle_complex(metric, extraction, question, retry=True)
        elif errors:
            return {"type": "error", "message": f"SQL validation failed: {'; '.join(errors)}"}

        return sql

    def _handle_derived(self, metric, extraction) -> str | dict:
        deps = metric.depends_on
        if len(deps) != 2 or "/" not in (metric.formula or ""):
            return {"type": "error", "message": f"Unsupported derived metric formula: {metric.formula}"}

        sub_sqls = []
        for dep_name in deps:
            dep_metric = self.registry.find(dep_name)
            if dep_metric is None:
                return {"type": "error", "message": f"Dependency '{dep_name}' not found"}
            if dep_metric.type == "simple" and dep_metric.sources:
                source = dep_metric.select_source("platform")
                sql = self._handle_simple(dep_metric, source, extraction)
                if isinstance(sql, dict):
                    return sql
                sub_sqls.append((dep_metric.name.lower().replace(" ", "_"), sql))
            elif dep_metric.type == "complex":
                return {"type": "error", "message": f"Derived from complex metric '{dep_name}' not yet supported"}

        if len(sub_sqls) != 2:
            return {"type": "error", "message": "Could not resolve both dependencies"}

        name_a, sql_a = sub_sqls[0]
        name_b, sql_b = sub_sqls[1]
        ratio_name = metric.name.lower().replace(" ", "_")

        combined = f"""WITH {name_a}_cte AS (
    {sql_a}
),
{name_b}_cte AS (
    {sql_b}
)
SELECT
    a.period
    , a.market
    , a.{name_a}
    , b.{name_b}
    , a.{name_a} / NULLIF(b.{name_b}, 0) AS {ratio_name}
FROM {name_a}_cte a
JOIN {name_b}_cte b ON a.period = b.period AND a.market = b.market
"""
        return combined


    def _handle_atomic(self, metric, extraction) -> str | dict:
        """Handle atomic metrics with deterministic SQL assembly (no LLM)."""
        dims = extraction.dimensions
        date_range = dims.get("date_range") or {}

        intent = QueryIntent(
            metric_name=metric.name,
            market=dims.get("market"),
            date_start=date_range.get("start"),
            date_end=date_range.get("end"),
            granularity=dims.get("granularity", "monthly"),
            variant=dims.get("variant"),
        )

        try:
            return self.atomic_assembler.assemble(metric, intent)
        except ValueError as e:
            return {"type": "error", "message": str(e)}


def main():
    agent = Agent()
    print("S&R&A Metric Agent (type 'quit' to exit)")
    print("-" * 50)
    while True:
        try:
            question = input("\nQ: ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if question.lower() in ("quit", "exit", "q"):
            break
        if not question:
            continue

        result = agent.ask(question)

        if result["type"] == "sql":
            print(f"\n--- Generated SQL ---\n{result['sql']}")
        elif result["type"] == "clarification":
            print(f"\n{result['message']}")
        elif result["type"] == "error":
            print(f"\nError: {result['message']}")
        else:
            print(f"\nUnexpected: {result}")


if __name__ == "__main__":
    main()
