import sys
from src.registry import MetricRegistry
from src.extractor import IntentExtractor
from src.assembler import SQLAssembler


class Agent:
    def __init__(
        self,
        metrics_dir: str = "metrics",
        templates_dir: str = "templates",
        snippets_dir: str = "snippets",
        model: str = "claude-sonnet-4-20250514",
    ):
        self.registry = MetricRegistry(metrics_dir)
        self.registry.load()
        self.extractor = IntentExtractor(registry=self.registry, model=model)
        self.assembler = SQLAssembler(templates_dir=templates_dir, snippets_dir=snippets_dir)

    def ask(self, question: str) -> dict:
        parsed = self.extractor.extract(question)

        if parsed.get("clarification_needed"):
            return {"clarification": parsed["clarification_needed"]}

        metric_name = parsed["metrics"][0] if parsed["metrics"] else None
        if not metric_name:
            return {"error": "No metric identified in the question."}

        metric = self.registry.find(metric_name)
        if not metric:
            return {"error": f"Metric '{metric_name}' not found in registry."}

        dims = parsed["dimensions"]
        market = dims.get("market")
        date_range = dims.get("date_range") or {}
        date_start = date_range.get("start", "")
        date_end = date_range.get("end", "")
        module = dims.get("module")
        granularity = "module" if module else "platform"

        intent = parsed["intent"]

        if intent == "compare" and dims.get("compare_to"):
            comp = dims["compare_to"]
            sql = self.assembler.assemble_compare(
                metric=metric,
                market=market,
                current_start=date_start,
                current_end=date_end,
                previous_start=comp.get("start", ""),
                previous_end=comp.get("end", ""),
                granularity=granularity,
            )
        else:
            sql = self.assembler.assemble(
                metric=metric,
                market=market,
                date_start=date_start,
                date_end=date_end,
                granularity=granularity,
            )

        return {
            "intent": intent,
            "metric": metric.name,
            "market": market,
            "date_range": date_range,
            "sql": sql,
        }


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

        if "clarification" in result:
            print(f"\nClarification needed: {result['clarification']}")
        elif "error" in result:
            print(f"\nError: {result['error']}")
        else:
            print(f"\nMetric: {result['metric']}")
            print(f"Market: {result.get('market', 'All')}")
            print(f"Intent: {result['intent']}")
            print(f"\n--- Generated SQL ---\n{result['sql']}")


if __name__ == "__main__":
    main()
