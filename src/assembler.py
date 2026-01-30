from typing import Optional
from jinja2 import Environment, FileSystemLoader

from src.models import MetricDefinition, MetricSource


class Assembler:
    def __init__(self, templates_dir: str = "templates"):
        self.env = Environment(loader=FileSystemLoader(templates_dir))

    def render_simple(
        self,
        metric: MetricDefinition,
        source: MetricSource,
        date_start: str,
        date_end: str,
        market: Optional[str] = None,
    ) -> str:
        template = self.env.get_template("simple_metric.sql.j2")
        return template.render(
            metric=metric,
            source=source,
            date_start=date_start,
            date_end=date_end,
            market=market,
        )

    def render_compare(
        self,
        metric: MetricDefinition,
        source: MetricSource,
        current_start: str,
        current_end: str,
        previous_start: str,
        previous_end: str,
        market: Optional[str] = None,
    ) -> str:
        metric_col = metric.name.lower().replace(" ", "_")
        template = self.env.get_template("compare.sql.j2")
        return template.render(
            metric=metric,
            source=source,
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
            market=market,
            metric_col=metric_col,
        )

    def render_snippet(
        self,
        snippet_path: str,
        date_start: str,
        date_end: str,
        market: Optional[str] = None,
    ) -> str:
        with open(snippet_path) as f:
            template_str = f.read()
        template = self.env.from_string(template_str)
        return template.render(
            date_start=date_start,
            date_end=date_end,
            market=market,
        )
