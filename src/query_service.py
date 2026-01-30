from dataclasses import dataclass
from typing import Optional


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list]
    error: Optional[str] = None

    @property
    def has_error(self) -> bool:
        return self.error is not None


class QueryService:
    """Thin client for Shopee's internal query service.

    TODO: Replace stub implementation with real API calls.
    """

    def __init__(self, base_url: str = "", token: str = ""):
        self.base_url = base_url
        self.token = token

    def execute(self, sql: str, limit: int = 100) -> QueryResult:
        wrapped = self._wrap_with_limit(sql, limit)
        # STUB: return empty result. Replace with real API call.
        return QueryResult(columns=[], rows=[], error=None)

    def _wrap_with_limit(self, sql: str, limit: int) -> str:
        sql = sql.strip().rstrip(";")
        return f"{sql}\nLIMIT {limit}"
