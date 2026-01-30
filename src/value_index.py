import sqlite3
from datetime import datetime, timezone


class ValueIndex:
    def __init__(self, db_path: str = "value_index.db"):
        self.db_path = db_path
        self._conn = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
        return self._conn

    def _execute(self, sql: str, params: tuple = ()) -> list:
        cur = self._get_conn().execute(sql, params)
        return cur.fetchall()

    def init_db(self):
        self._get_conn().execute("""
            CREATE TABLE IF NOT EXISTS dimension_values (
                table_name  TEXT NOT NULL,
                column_name TEXT NOT NULL,
                value       TEXT NOT NULL,
                count       INTEGER DEFAULT 0,
                updated_at  TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name, value)
            )
        """)
        self._get_conn().commit()

    def upsert(self, table_name: str, column_name: str, values: list[tuple[str, int]]):
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "DELETE FROM dimension_values WHERE table_name = ? AND column_name = ?",
            (table_name, column_name),
        )
        conn.executemany(
            "INSERT INTO dimension_values (table_name, column_name, value, count, updated_at) VALUES (?, ?, ?, ?, ?)",
            [(table_name, column_name, v, c, now) for v, c in values],
        )
        conn.commit()

    def get_values(self, table_name: str, column_name: str) -> list[str]:
        rows = self._execute(
            "SELECT value FROM dimension_values WHERE table_name = ? AND column_name = ? ORDER BY count DESC",
            (table_name, column_name),
        )
        return [r[0] for r in rows]

    def get_values_with_counts(self, table_name: str, column_name: str) -> list[tuple[str, int]]:
        rows = self._execute(
            "SELECT value, count FROM dimension_values WHERE table_name = ? AND column_name = ? ORDER BY count DESC",
            (table_name, column_name),
        )
        return rows

    def value_exists(self, table_name: str, column_name: str, value: str) -> bool:
        rows = self._execute(
            "SELECT 1 FROM dimension_values WHERE table_name = ? AND column_name = ? AND value = ? LIMIT 1",
            (table_name, column_name, value),
        )
        return len(rows) > 0

    def get_all_values_for_column(self, column_name: str) -> list[str]:
        rows = self._execute(
            "SELECT DISTINCT value FROM dimension_values WHERE column_name = ? ORDER BY value",
            (column_name,),
        )
        return [r[0] for r in rows]

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
