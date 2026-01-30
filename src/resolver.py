from dataclasses import dataclass, field
from typing import Optional

from src.models import MetricDefinition, MetricSource
from src.registry import MetricRegistry
from src.value_index import ValueIndex
from src.extractor import ExtractionResult


@dataclass
class ResolverResult:
    metric: Optional[MetricDefinition] = None
    source: Optional[MetricSource] = None
    errors: list[str] = field(default_factory=list)


class Resolver:
    def __init__(self, registry: MetricRegistry, value_index: ValueIndex):
        self.registry = registry
        self.value_index = value_index

    def resolve(self, extraction: ExtractionResult) -> ResolverResult:
        result = ResolverResult()

        if not extraction.metrics:
            result.errors.append("No metrics specified in the query.")
            return result

        metric_name = extraction.metrics[0]
        metric = self.registry.find(metric_name)
        if metric is None:
            result.errors.append(
                f"Metric '{metric_name}' not found. Available: {', '.join(self.registry.list_names_and_aliases())}"
            )
            return result

        result.metric = metric

        # Select source for simple metrics
        if metric.type == "simple" and metric.sources:
            granularity = extraction.dimensions.get("module")
            result.source = metric.select_source(
                granularity="module" if granularity else "platform"
            )

        # Validate market against value index
        market = extraction.dimensions.get("market")
        if market and result.source:
            region_col = result.source.columns.get("region")
            if region_col and not self.value_index.value_exists(
                result.source.table, region_col, market
            ):
                valid = self.value_index.get_values(result.source.table, region_col)
                if valid:
                    result.errors.append(
                        f"Unknown market '{market}'. Valid values: {', '.join(valid)}"
                    )

        return result
