import pytest
from unittest.mock import patch, MagicMock
from src.autotest.__main__ import parse_args


def test_parse_default_args():
    args = parse_args([])
    assert args.benchmark == "tests/benchmark.yaml"
    assert args.max_retries == 3
    assert args.max_llm_calls == 50
    assert args.no_repair is False
    assert args.dry_run is False
    assert args.tags is None
    assert args.id is None


def test_parse_custom_args():
    args = parse_args([
        "--benchmark", "custom.yaml",
        "--tags", "ads,traffic",
        "--id", "my_case",
        "--max-retries", "5",
        "--max-llm-calls", "100",
        "--no-repair",
        "--dry-run",
    ])
    assert args.benchmark == "custom.yaml"
    assert args.tags == "ads,traffic"
    assert args.id == "my_case"
    assert args.max_retries == 5
    assert args.max_llm_calls == 100
    assert args.no_repair is True
    assert args.dry_run is True
