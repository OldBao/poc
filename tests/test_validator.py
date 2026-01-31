import pytest
from src.validator import SQLValidator
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
    idx.upsert(
        "mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live",
        "grass_region",
        [("ID", 1000), ("TH", 500)],
    )
    return idx


def test_valid_sql_passes(registry, value_index):
    sql = """
    SELECT grass_region, avg(ads_rev_usd)
    FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
    WHERE grass_region = 'ID'
    GROUP BY 1
    """
    validator = SQLValidator(registry=registry, value_index=value_index)
    errors = validator.validate(sql)
    assert errors == []


def test_syntax_error_detected(registry, value_index):
    sql = "SELEC * FORM table"
    validator = SQLValidator(registry=registry, value_index=value_index)
    errors = validator.validate(sql)
    assert any("syntax" in e.lower() for e in errors)


def test_unknown_table_detected(registry, value_index):
    sql = """
    SELECT * FROM nonexistent_schema.fake_table
    WHERE grass_region = 'ID'
    """
    validator = SQLValidator(registry=registry, value_index=value_index)
    errors = validator.validate(sql)
    assert any("table" in e.lower() for e in errors)


def test_validator_extracts_tables_from_layered_snippets(tmp_path):
    """Validator finds tables from source-level snippet files."""
    from unittest.mock import MagicMock

    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    snippet_path = str(tmp_path / "snippets" / "layer1" / "test.sql")
    (metrics_dir / "test.yaml").write_text(f"""
metric:
  name: Test
  type: simple
  sources:
    - id: src1
      table: db.known_table
      snippet: {snippet_path}
      columns: {{value: v, date: d, region: r}}
  dimensions:
    required: []
    optional: []
""")

    snippets_dir = tmp_path / "snippets" / "layer1"
    snippets_dir.mkdir(parents=True)
    (snippets_dir / "test.sql").write_text(
        "SELECT v FROM db.snippet_table WHERE d BETWEEN date '2025-01-01' AND date '2025-01-31'"
    )

    reg = MetricRegistry(metrics_dir=str(metrics_dir))
    reg.load()
    vi = MagicMock()
    vi.get_all_values_for_column.return_value = set()

    validator = SQLValidator(registry=reg, value_index=vi)

    assert "db.known_table" in validator._known_tables
    assert "db.snippet_table" in validator._known_tables


def test_invalid_filter_value_detected(registry, value_index):
    sql = """
    SELECT grass_region, avg(ads_rev_usd)
    FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
    WHERE grass_region = 'XX'
    GROUP BY 1
    """
    validator = SQLValidator(registry=registry, value_index=value_index)
    errors = validator.validate(sql)
    assert any("XX" in e for e in errors)
