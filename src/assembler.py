import os
from jinja2 import Environment, FileSystemLoader
from src.models import MetricDefinition


class SQLAssembler:
    def __init__(self, templates_dir: str = "templates", snippets_dir: str = "snippets"):
        self.templates_dir = templates_dir
        self.snippets_dir = snippets_dir
        self.env = Environment(
            loader=FileSystemLoader(templates_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def assemble(
        self,
        metric: MetricDefinition,
        market: str | None = None,
        date_start: str = "",
        date_end: str = "",
        granularity: str = "platform",
    ) -> str:
        if metric.type == "simple":
            return self._assemble_simple(metric, market, date_start, date_end, granularity)
        elif metric.type == "complex":
            return self._assemble_complex(metric, market, date_start, date_end)
        elif metric.type == "derived":
            return self._assemble_derived(metric, market, date_start, date_end)
        raise ValueError(f"Unknown metric type: {metric.type}")

    def _assemble_simple(self, metric, market, date_start, date_end, granularity):
        source = metric.select_source(granularity=granularity)
        template = self.env.get_template("simple_metric.sql.j2")
        return template.render(
            metric=metric,
            source=source,
            market=market,
            date_start=date_start,
            date_end=date_end,
        )

    def _assemble_complex(self, metric, market, date_start, date_end):
        snippet_path = os.path.join(self.snippets_dir, os.path.basename(metric.snippet_file))
        with open(snippet_path) as f:
            snippet_template = f.read()
        template = self.env.from_string(snippet_template)
        return template.render(
            market=market,
            date_start=date_start,
            date_end=date_end,
        )

    def _assemble_derived(self, metric, market, date_start, date_end):
        return f"-- Derived metric: {metric.name}\n-- Formula: {metric.formula}\n-- Resolve depends_on metrics first, then compute ratio"
