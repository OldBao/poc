import os
import yaml
from typing import Optional
from src.models import MetricDefinition


class MetricRegistry:
    def __init__(self, metrics_dir: str):
        self.metrics_dir = metrics_dir
        self.metrics: list[MetricDefinition] = []
        self._name_index: dict[str, MetricDefinition] = {}

    def load(self):
        self.metrics = []
        self._name_index = {}
        for fname in os.listdir(self.metrics_dir):
            if not fname.endswith(".yaml") and not fname.endswith(".yml"):
                continue
            path = os.path.join(self.metrics_dir, fname)
            with open(path) as f:
                data = yaml.safe_load(f)
            if data and "metric" in data:
                m = MetricDefinition.from_dict(data)
                self.metrics.append(m)
                self._name_index[m.name.lower()] = m
                for alias in m.aliases:
                    self._name_index[alias.lower()] = m

    def find(self, name: str) -> Optional[MetricDefinition]:
        return self._name_index.get(name.lower())

    def list_names_and_aliases(self) -> list[str]:
        result = []
        for m in self.metrics:
            aliases_str = ", ".join(m.aliases)
            result.append(f"{m.name} ({aliases_str})")
        return result
