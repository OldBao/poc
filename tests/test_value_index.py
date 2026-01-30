import pytest
from src.value_index import ValueIndex


@pytest.fixture
def tmp_index(tmp_path):
    db_path = str(tmp_path / "test_index.db")
    idx = ValueIndex(db_path)
    idx.init_db()
    return idx


def test_init_creates_table(tmp_index):
    rows = tmp_index._execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dimension_values'")
    assert len(rows) == 1


def test_upsert_and_lookup(tmp_index):
    tmp_index.upsert("my_table", "grass_region", [("ID", 1000), ("TH", 500), ("VN", 300)])
    values = tmp_index.get_values("my_table", "grass_region")
    assert set(values) == {"ID", "TH", "VN"}


def test_get_values_with_counts(tmp_index):
    tmp_index.upsert("my_table", "grass_region", [("ID", 1000), ("TH", 500)])
    result = tmp_index.get_values_with_counts("my_table", "grass_region")
    assert result == [("ID", 1000), ("TH", 500)]


def test_value_exists(tmp_index):
    tmp_index.upsert("my_table", "grass_region", [("ID", 1000)])
    assert tmp_index.value_exists("my_table", "grass_region", "ID") is True
    assert tmp_index.value_exists("my_table", "grass_region", "XX") is False


def test_upsert_overwrites(tmp_index):
    tmp_index.upsert("my_table", "col", [("a", 10)])
    tmp_index.upsert("my_table", "col", [("b", 20)])
    values = tmp_index.get_values("my_table", "col")
    assert values == ["b"]
