from dataclasses import dataclass, field
from typing import Optional


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

    @classmethod
    def from_dict(cls, data: dict) -> "MetricDefinition":
        m = data["metric"]
        sources = [MetricSource(**s) for s in m.get("sources", [])]
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
        )

    def select_source(self, granularity: Optional[str] = None) -> MetricSource:
        if granularity:
            for s in self.sources:
                if granularity in s.use_when.get("granularity", []):
                    return s
        return self.sources[0]
