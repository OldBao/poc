from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AtomicSource:
    """Source definition for atomic metrics — single table with grain metadata."""
    table: str
    grain: str  # e.g., "daily"
    date_column: str
    region_column: str
    base_filters: list[str] = field(default_factory=list)


@dataclass
class AtomicColumn:
    """Column definition with inner expression and cross-day aggregation."""
    expr: str  # SQL expression at source grain, e.g., "sum(net_ads_rev_usd)"
    agg_across_days: str  # Rollup function, e.g., "sum", "avg"
    variant: Optional[str] = None  # e.g., "excl_1p"


@dataclass
class QueryIntent:
    """Structured intent for atomic metric assembly."""
    metric_name: str
    market: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    granularity: str = "monthly"  # daily, monthly, yearly, total
    variant: Optional[str] = None


@dataclass
class MetricSource:
    id: str
    layer: str
    table: str
    columns: dict
    filters: list[str] = field(default_factory=list)
    use_when: dict = field(default_factory=dict)


@dataclass
class MetricDefinition:
    name: str
    aliases: list[str]
    type: str  # "simple" or "complex"
    dimensions: dict
    sources: list[MetricSource] = field(default_factory=list)
    aggregation: Optional[str] = None
    unit: Optional[str] = None
    snippet_file: Optional[str] = None
    sub_metrics: list[str] = field(default_factory=list)
    formula: Optional[str] = None
    depends_on: list[str] = field(default_factory=list)
    owner: Optional[str] = None
    notes: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    atomic_source: Optional[AtomicSource] = None
    atomic_columns: dict[str, AtomicColumn] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "MetricDefinition":
        m = data["metric"]
        sources = [MetricSource(**s) for s in m.get("sources", [])]

        # Parse atomic source (singular "source" block)
        atomic_source = None
        if "source" in m:
            src = m["source"]
            atomic_source = AtomicSource(
                table=src["table"],
                grain=src["grain"],
                date_column=src["date_column"],
                region_column=src["region_column"],
                base_filters=src.get("base_filters", []),
            )

        # Parse atomic columns
        atomic_columns = {}
        if "columns" in m:
            for col_name, col_def in m["columns"].items():
                atomic_columns[col_name] = AtomicColumn(
                    expr=col_def["expr"],
                    agg_across_days=col_def["agg_across_days"],
                    variant=col_def.get("variant"),
                )

        return cls(
            name=m["name"],
            aliases=m.get("aliases", []),
            type=m["type"],
            dimensions=m["dimensions"],
            sources=sources,
            aggregation=m.get("aggregation"),
            unit=m.get("unit"),
            snippet_file=m.get("snippet_file"),
            sub_metrics=m.get("sub_metrics", []),
            formula=m.get("formula"),
            depends_on=m.get("depends_on", []),
            owner=m.get("owner"),
            notes=m.get("notes"),
            tags=m.get("tags", []),
            atomic_source=atomic_source,
            atomic_columns=atomic_columns,
        )

    def select_source(self, granularity: Optional[str] = None) -> MetricSource:
        if granularity:
            for s in self.sources:
                if granularity in s.use_when.get("granularity", []):
                    return s
        return self.sources[0]


@dataclass
class Rule:
    name: str
    description: str
    when: dict
    effect_type: str  # "left_join", "filter", "column", "wrap"
    snippet_file: Optional[str] = None
    join_keys: list[str] = field(default_factory=list)
    clause: Optional[str] = None
    priority: int = 0
    valid_from: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Rule":
        r = data["rule"]
        effect = r["effect"]
        return cls(
            name=r["name"],
            description=r.get("description", ""),
            when=r["when"],
            effect_type=effect["type"],
            snippet_file=effect.get("snippet_file"),
            join_keys=effect.get("join_keys", []),
            clause=effect.get("clause"),
            priority=effect.get("priority", 0),
            valid_from=r.get("valid_from"),
        )


@dataclass
class JoinAdjustment:
    name: str
    snippet: str
    join_keys: list[str]


@dataclass
class WrapAdjustment:
    name: str
    snippet: str
    priority: int


@dataclass
class AssemblyContext:
    base_snippet: str
    joins: list[JoinAdjustment] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    wrappers: list[WrapAdjustment] = field(default_factory=list)
