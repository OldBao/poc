from dataclasses import dataclass, field
from typing import Optional
import yaml


@dataclass
class BenchmarkCase:
    id: str
    question: str
    expected_sql: str
    tags: list[str] = field(default_factory=list)


class BenchmarkLoader:
    def __init__(self, path: str = "tests/benchmark.yaml"):
        self.path = path

    def load(
        self,
        tags: Optional[list[str]] = None,
        case_id: Optional[str] = None,
    ) -> list[BenchmarkCase]:
        with open(self.path) as f:
            data = yaml.safe_load(f)

        cases = [
            BenchmarkCase(
                id=c["id"],
                question=c["question"],
                expected_sql=c["expected_sql"].strip(),
                tags=c.get("tags", []),
            )
            for c in data["cases"]
        ]

        if case_id:
            cases = [c for c in cases if c.id == case_id]
        if tags:
            cases = [c for c in cases if set(tags) & set(c.tags)]

        return cases
