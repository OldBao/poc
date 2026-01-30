import pytest
from src.autotest.loader import BenchmarkLoader, BenchmarkCase


def test_load_cases_from_yaml(tmp_path):
    yaml_content = """
cases:
  - id: dau_id_monthly
    question: "ID market DAU in November 2025"
    expected_sql: |
      SELECT grass_region, avg(a1) AS dau
      FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
      WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30'
      GROUP BY 1
    tags: [traffic, dau]
  - id: gmv_th
    question: "TH GMV in December 2025"
    expected_sql: |
      SELECT sum(gmv) FROM orders WHERE market = 'TH'
    tags: [commerce, gmv]
"""
    f = tmp_path / "benchmark.yaml"
    f.write_text(yaml_content)

    loader = BenchmarkLoader(str(f))
    cases = loader.load()

    assert len(cases) == 2
    assert cases[0].id == "dau_id_monthly"
    assert cases[0].question == "ID market DAU in November 2025"
    assert "avg(a1)" in cases[0].expected_sql
    assert cases[0].tags == ["traffic", "dau"]


def test_filter_by_tags(tmp_path):
    yaml_content = """
cases:
  - id: case_a
    question: "Q A"
    expected_sql: "SELECT 1"
    tags: [ads]
  - id: case_b
    question: "Q B"
    expected_sql: "SELECT 2"
    tags: [traffic]
"""
    f = tmp_path / "benchmark.yaml"
    f.write_text(yaml_content)

    loader = BenchmarkLoader(str(f))
    cases = loader.load(tags=["ads"])
    assert len(cases) == 1
    assert cases[0].id == "case_a"


def test_filter_by_id(tmp_path):
    yaml_content = """
cases:
  - id: case_a
    question: "Q A"
    expected_sql: "SELECT 1"
    tags: []
  - id: case_b
    question: "Q B"
    expected_sql: "SELECT 2"
    tags: []
"""
    f = tmp_path / "benchmark.yaml"
    f.write_text(yaml_content)

    loader = BenchmarkLoader(str(f))
    cases = loader.load(case_id="case_b")
    assert len(cases) == 1
    assert cases[0].id == "case_b"
