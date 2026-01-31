from dataclasses import dataclass, field
from src.llm_backend import LLMBackend
from src.query_service import QueryResult


@dataclass
class CompareResult:
    match: bool
    differences: list[str] = field(default_factory=list)


STRUCTURAL_COMPARE_PROMPT = """You are an expert SQL analyst. Compare two Presto SQL queries structurally.

Extract and compare these components:
- Tables referenced
- Join conditions
- WHERE filters (date ranges, region, tz_type, etc.)
- Aggregation functions and columns
- GROUP BY / ORDER BY clauses

Two queries are structurally equivalent if they would produce the same result, even if:
- Column aliases differ
- Formatting/whitespace differs
- Equivalent date functions are used (e.g., substr(cast(...)) vs date_format)
- Column order differs

Respond with ONLY this JSON:
{
  "match": true/false,
  "differences": ["difference 1", "difference 2"]
}

If match is true, differences should be an empty list.
"""


class StructuralComparator:
    def __init__(self, backend: LLMBackend = None, *, llm_client=None):
        if llm_client is not None:
            self.llm = llm_client
            self._use_legacy = True
        else:
            self.llm = backend
            self._use_legacy = False

    def compare(self, expected_sql: str, generated_sql: str) -> CompareResult:
        user_message = (
            f"## Expected SQL\n```sql\n{expected_sql}\n```\n\n"
            f"## Generated SQL\n```sql\n{generated_sql}\n```"
        )
        if self._use_legacy:
            result = self.llm.call(
                system_prompt=STRUCTURAL_COMPARE_PROMPT,
                user_message=user_message,
            )
        else:
            result = self.llm.generate_json(
                system_prompt=STRUCTURAL_COMPARE_PROMPT,
                user_message=user_message,
            )
        return CompareResult(
            match=result.get("match", False),
            differences=result.get("differences", []),
        )


@dataclass
class ResultCompareResult:
    match: bool
    schema_diff: list[str] = field(default_factory=list)
    row_mismatches: int = 0
    sample_diffs: list[str] = field(default_factory=list)


class ResultComparator:
    def __init__(self, tolerance: float = 0.0001):
        self.tolerance = tolerance

    def compare(self, expected: QueryResult, generated: QueryResult) -> ResultCompareResult:
        if expected.has_error:
            return ResultCompareResult(
                match=False,
                schema_diff=[f"Expected SQL error: {expected.error}"],
            )
        if generated.has_error:
            return ResultCompareResult(
                match=False,
                schema_diff=[f"Generated SQL error: {generated.error}"],
            )

        # Schema check (order-insensitive)
        exp_cols = sorted(expected.columns)
        gen_cols = sorted(generated.columns)
        if exp_cols != gen_cols:
            return ResultCompareResult(
                match=False,
                schema_diff=[f"Expected columns: {exp_cols}, Got: {gen_cols}"],
            )

        # Data check: sort rows and compare
        exp_rows = sorted(expected.rows, key=str)
        gen_rows = sorted(generated.rows, key=str)

        mismatches = 0
        diffs = []
        max_rows = max(len(exp_rows), len(gen_rows))
        for i in range(max_rows):
            if i >= len(exp_rows):
                mismatches += 1
                diffs.append(f"Row {i}: missing in expected")
                continue
            if i >= len(gen_rows):
                mismatches += 1
                diffs.append(f"Row {i}: missing in generated")
                continue
            if not self._rows_equal(exp_rows[i], gen_rows[i]):
                mismatches += 1
                if len(diffs) < 5:  # sample up to 5
                    diffs.append(f"Row {i}: expected {exp_rows[i]}, got {gen_rows[i]}")

        return ResultCompareResult(
            match=mismatches == 0,
            row_mismatches=mismatches,
            sample_diffs=diffs,
        )

    def _rows_equal(self, row_a: list, row_b: list) -> bool:
        if len(row_a) != len(row_b):
            return False
        for a, b in zip(row_a, row_b):
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if abs(a) < 1e-9 and abs(b) < 1e-9:
                    continue
                if abs(a - b) / max(abs(a), abs(b)) > self.tolerance:
                    return False
            elif a != b:
                return False
        return True
