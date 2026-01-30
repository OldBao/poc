"""Deterministic SQL assembly for atomic metrics.

Generates SQL in two phases:
1. Inner query: aggregates at source grain (daily) from the source table.
2. Outer query: rolls up to the requested granularity using agg_across_days.
"""

from typing import Optional

from src.models import MetricDefinition, AtomicColumn, QueryIntent


VALID_GRANULARITIES = ("daily", "monthly", "yearly", "total")


class AtomicAssembler:
    """Assembles SQL deterministically from atomic metric YAML definitions."""

    def assemble(self, metric: MetricDefinition, intent: QueryIntent) -> str:
        """Generate SQL for an atomic metric given a structured intent."""
        if not metric.atomic_source:
            raise ValueError(f"Metric '{metric.name}' has no atomic source definition")

        columns = self._select_columns(metric.atomic_columns, intent.variant)
        if not columns:
            raise ValueError(
                f"No columns match variant '{intent.variant}' for metric '{metric.name}'"
            )

        inner_sql = self._build_inner_query(metric, intent, columns)
        return self._build_outer_query(metric, inner_sql, columns, intent)

    def _select_columns(
        self,
        all_columns: dict[str, AtomicColumn],
        variant: Optional[str],
    ) -> dict[str, AtomicColumn]:
        """Filter columns by variant.

        - variant=None → return all columns (default + variant columns).
        - variant="excl_1p" → return only columns matching that variant.
        """
        if variant is None:
            return dict(all_columns)
        return {
            name: col for name, col in all_columns.items()
            if col.variant == variant
        }

    def _build_inner_query(
        self,
        metric: MetricDefinition,
        intent: QueryIntent,
        columns: dict[str, AtomicColumn],
    ) -> str:
        """Build inner query at source grain (daily)."""
        src = metric.atomic_source

        # SELECT clause
        select_parts = [src.region_column, src.date_column]
        for col_name, col_def in columns.items():
            select_parts.append(f"{col_def.expr} AS {col_name}")

        # WHERE clause
        where_parts = []
        if intent.date_start and intent.date_end:
            where_parts.append(
                f"{src.date_column} BETWEEN date '{intent.date_start}' AND date '{intent.date_end}'"
            )
        where_parts.extend(src.base_filters)
        if intent.market:
            where_parts.append(f"{src.region_column} = '{intent.market}'")

        where_clause = "\n      AND ".join(where_parts) if where_parts else "1 = 1"

        lines = [
            f"    SELECT {select_parts[0]}, {select_parts[1]},",
        ]
        for i, part in enumerate(select_parts[2:]):
            comma = "," if i < len(select_parts) - 3 else ""
            lines.append(f"           {part}{comma}")
        lines.append(f"    FROM {src.table}")
        lines.append(f"    WHERE {where_clause}")
        lines.append("    GROUP BY 1, 2")

        return "\n".join(lines)

    def _build_outer_query(
        self,
        metric: MetricDefinition,
        inner_sql: str,
        columns: dict[str, AtomicColumn],
        intent: QueryIntent,
    ) -> str:
        """Build outer query with granularity-dependent rollup."""
        src = metric.atomic_source
        granularity = intent.granularity

        if granularity not in VALID_GRANULARITIES:
            raise ValueError(
                f"Unsupported granularity '{granularity}'. "
                f"Valid options: {', '.join(VALID_GRANULARITIES)}"
            )

        # Determine period expression and GROUP BY based on granularity
        if granularity == "daily":
            period_expr = f"n1.{src.date_column} AS period"
            group_by = f"n1.{src.region_column}, n1.{src.date_column}"
        elif granularity == "monthly":
            substr = f"substr(cast(n1.{src.date_column} as varchar), 1, 7)"
            period_expr = f"{substr} AS period"
            group_by = f"n1.{src.region_column}, {substr}"
        elif granularity == "yearly":
            substr = f"substr(cast(n1.{src.date_column} as varchar), 1, 4)"
            period_expr = f"{substr} AS period"
            group_by = f"n1.{src.region_column}, {substr}"
        else:  # total
            period_expr = None
            group_by = f"n1.{src.region_column}"

        # Build outer SELECT
        select_parts = [f"n1.{src.region_column}"]
        if period_expr:
            select_parts.append(period_expr)
        for col_name, col_def in columns.items():
            select_parts.append(f"{col_def.agg_across_days}({col_name}) AS {col_name}")

        select_clause = ",\n    ".join(select_parts)

        return (
            f"SELECT\n"
            f"    {select_clause}\n"
            f"FROM (\n"
            f"{inner_sql}\n"
            f") n1\n"
            f"GROUP BY {group_by}"
        )
