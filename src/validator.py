import re
import sqlglot
from sqlglot import exp

from src.registry import MetricRegistry
from src.value_index import ValueIndex


class SQLValidator:
    def __init__(self, registry: MetricRegistry, value_index: ValueIndex):
        self.registry = registry
        self.value_index = value_index
        self._known_tables = self._build_table_set()

    def _build_table_set(self) -> set[str]:
        tables = set()
        for m in self.registry.metrics:
            for s in m.sources:
                tables.add(s.table.lower())
        return tables

    def validate(self, sql: str) -> list[str]:
        errors = []
        errors.extend(self._check_syntax(sql))
        if errors:
            return errors
        errors.extend(self._check_tables(sql))
        errors.extend(self._check_filter_values(sql))
        return errors

    def _check_syntax(self, sql: str) -> list[str]:
        try:
            parsed = sqlglot.parse(sql)
            # sqlglot is lenient: "SELEC * FORM table" parses as an expression,
            # not a SELECT statement.  Verify we got a real statement.
            for statement in parsed:
                if statement is None:
                    continue
                if not isinstance(statement, (exp.Select, exp.Union, exp.CTE, exp.Subquery)):
                    return [
                        f"SQL syntax error: parsed as {type(statement).__name__}, "
                        f"expected a SELECT statement"
                    ]
            return []
        except sqlglot.errors.ParseError as e:
            return [f"SQL syntax error: {e}"]

    def _check_tables(self, sql: str) -> list[str]:
        errors = []
        try:
            parsed = sqlglot.parse(sql)
            for statement in parsed:
                if statement is None:
                    continue
                for table in statement.find_all(exp.Table):
                    full_name = table.sql().lower().strip('"').strip("'")
                    if "." not in full_name:
                        continue
                    if full_name not in self._known_tables:
                        errors.append(f"Unknown table: {full_name}")
        except Exception:
            pass
        return errors

    def _check_filter_values(self, sql: str) -> list[str]:
        errors = []
        pattern = r"(\w+)\s*=\s*'([^']+)'"
        for match in re.finditer(pattern, sql):
            column = match.group(1)
            value = match.group(2)
            if column in ("grass_region",):
                all_values = self.value_index.get_all_values_for_column(column)
                if all_values and value not in all_values:
                    errors.append(
                        f"Invalid value '{value}' for {column}. Valid: {', '.join(all_values)}"
                    )
        return errors
