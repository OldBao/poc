import pytest
from unittest.mock import MagicMock
from src.reindex import Reindexer
from src.registry import MetricRegistry
from src.value_index import ValueIndex


@pytest.fixture
def registry():
    reg = MetricRegistry(metrics_dir="metrics")
    reg.load()
    return reg


@pytest.fixture
def value_index(tmp_path):
    idx = ValueIndex(str(tmp_path / "test.db"))
    idx.init_db()
    return idx


def test_discover_columns(registry, value_index):
    reindexer = Reindexer(registry=registry, value_index=value_index, db_conn=None)
    columns = reindexer.discover_columns()
    assert any(c["column_name"] == "grass_region" for c in columns)


def test_reindex_with_mock_db(registry, value_index):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("ID", 1000), ("TH", 500)]
    mock_cursor.description = [("value", None), ("count", None)]

    reindexer = Reindexer(registry=registry, value_index=value_index, db_conn=mock_conn)
    reindexer.reindex_column(
        table_name="traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
        column_name="grass_region",
    )
    values = value_index.get_values(
        "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
        "grass_region",
    )
    assert "ID" in values
    assert "TH" in values
