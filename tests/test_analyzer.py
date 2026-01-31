import json
from unittest.mock import MagicMock
from src.importer.analyzer import SQLAnalyzer
from src.llm_backend import strip_fences


def _make_backend(response_text: str):
    """Create a mock backend whose generate_json_list returns parsed JSON."""
    backend = MagicMock()
    backend.generate_json_list.return_value = json.loads(strip_fences(response_text))
    return backend


def test_analyzer_extracts_metrics_from_sql():
    sample_sql = """
    select avg(a1) as dau from traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
    where grass_date between date '2025-11-01' and date '2025-11-30' and tz_type = 'local'
    """

    response = '''```json
    [
        {
            "name": "DAU",
            "aliases": ["daily active users"],
            "type": "simple",
            "table": "traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live",
            "columns": {"value": "a1", "date": "grass_date", "region": "grass_region"},
            "filters": ["tz_type = 'local'"],
            "aggregation": "avg",
            "snippet": "SELECT substr(cast(grass_date as varchar), 1, 7) AS period, grass_region, avg(a1) AS dau FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}' AND tz_type = 'local' GROUP BY 1, 2",
            "notes": ""
        }
    ]
    ```'''

    backend = _make_backend(response)
    analyzer = SQLAnalyzer(backend=backend)
    results = analyzer.analyze_sql(sample_sql)

    assert len(results) >= 1
    assert results[0]["name"] == "DAU"
    assert "snippet" in results[0]


def test_analyzer_extracts_from_doc():
    sample_doc = "DAU means daily active users. We compute it as avg(a1) from the platform active churn table."

    response = '''```json
    [
        {
            "name": "DAU",
            "aliases": ["daily active users"],
            "type": "simple",
            "notes": "Computed as avg(a1) from platform active churn table"
        }
    ]
    ```'''

    backend = _make_backend(response)
    analyzer = SQLAnalyzer(backend=backend)
    results = analyzer.analyze_doc(sample_doc)

    assert len(results) >= 1
    assert results[0]["name"] == "DAU"
