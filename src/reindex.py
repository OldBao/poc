from src.registry import MetricRegistry
from src.value_index import ValueIndex


class Reindexer:
    def __init__(self, registry: MetricRegistry, value_index: ValueIndex, db_conn):
        self.registry = registry
        self.value_index = value_index
        self.db_conn = db_conn

    def discover_columns(self) -> list[dict]:
        columns = []
        seen = set()
        for metric in self.registry.metrics:
            for source in metric.sources:
                for col_key, col_name in source.columns.items():
                    if col_key == "value":
                        continue
                    key = (source.table, col_name)
                    if key not in seen:
                        seen.add(key)
                        columns.append({
                            "table_name": source.table,
                            "column_name": col_name,
                        })
        return columns

    def reindex_column(self, table_name: str, column_name: str):
        cursor = self.db_conn.cursor()
        sql = f"SELECT {column_name} AS value, COUNT(*) AS count FROM {table_name} GROUP BY {column_name} ORDER BY count DESC"
        cursor.execute(sql)
        rows = cursor.fetchall()
        self.value_index.upsert(table_name, column_name, [(str(r[0]), r[1]) for r in rows])

    def reindex_all(self):
        columns = self.discover_columns()
        for col in columns:
            try:
                self.reindex_column(col["table_name"], col["column_name"])
                print(f"  Indexed {col['table_name']}.{col['column_name']}")
            except Exception as e:
                print(f"  Failed {col['table_name']}.{col['column_name']}: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Reindex dimension values from AlloyDB")
    parser.add_argument("--all", action="store_true", help="Reindex all columns")
    parser.add_argument("--dimension", type=str, help="Reindex a specific column name")
    parser.add_argument("--db-url", type=str, default=None, help="AlloyDB connection string")
    args = parser.parse_args()

    registry = MetricRegistry(metrics_dir="metrics")
    registry.load()
    value_index = ValueIndex("value_index.db")
    value_index.init_db()

    db_conn = None
    if args.db_url:
        import psycopg2
        db_conn = psycopg2.connect(args.db_url)

    if db_conn is None:
        print("Error: --db-url required for reindexing from AlloyDB")
        return

    reindexer = Reindexer(registry=registry, value_index=value_index, db_conn=db_conn)

    if args.all:
        print("Reindexing all dimension columns...")
        reindexer.reindex_all()
    elif args.dimension:
        columns = [c for c in reindexer.discover_columns() if c["column_name"] == args.dimension]
        for col in columns:
            reindexer.reindex_column(col["table_name"], col["column_name"])
            print(f"Indexed {col['table_name']}.{col['column_name']}")
    else:
        parser.print_help()

    print("Done.")


if __name__ == "__main__":
    main()
