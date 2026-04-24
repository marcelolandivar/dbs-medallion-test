"""
Unit tests for the Great Expectations suite helper functions.
No Spark or Databricks runtime required — pure Python.
Run with: pytest tests/test_gx_suite.py -v
"""
import json
import pytest


SAMPLE_PASS_RESULT = {
    "success": True,
    "results": [
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "order_id"}
            },
            "result": {"unexpected_count": 0, "unexpected_percent": 0.0}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1000, "max_value": 10_000_000}
            },
            "result": {"observed_value": 52_400}
        }
    ]
}

SAMPLE_FAIL_RESULT = {
    "success": False,
    "results": [
        {
            "success": False,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "order_id"}
            },
            "result": {"unexpected_count": 120, "unexpected_percent": 18.5}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1000, "max_value": 10_000_000}
            },
            "result": {"observed_value": 648}
        }
    ]
}


# ── helpers (mirrors the logic in 03_gx_validate and 04_ai_agent_analyse) ──

def get_failed_expectations(result: dict) -> list:
    return [r for r in result["results"] if not r["success"]]


def classify_severity_heuristic(failed: list) -> str:
    """
    Rule-based fallback used in tests (mirrors the LLM prompt intent).
    critical  → any failure with unexpected_percent > 10
    warning   → failures below that threshold
    """
    for f in failed:
        pct = f.get("result", {}).get("unexpected_percent", 0)
        if pct > 10:
            return "critical"
    return "warning"


# ── tests ───────────────────────────────────────────────────────────────────

def test_no_failures_on_pass_result():
    assert get_failed_expectations(SAMPLE_PASS_RESULT) == []


def test_detects_one_failure():
    failed = get_failed_expectations(SAMPLE_FAIL_RESULT)
    assert len(failed) == 1


def test_failure_expectation_type():
    failed = get_failed_expectations(SAMPLE_FAIL_RESULT)
    assert failed[0]["expectation_config"]["expectation_type"] == \
        "expect_column_values_to_not_be_null"


def test_critical_severity_on_high_null_rate():
    failed = get_failed_expectations(SAMPLE_FAIL_RESULT)
    assert classify_severity_heuristic(failed) == "critical"


def test_warning_severity_on_low_null_rate():
    low_fail = [{
        "success": False,
        "expectation_config": {"expectation_type": "expect_column_values_to_not_be_null"},
        "result": {"unexpected_percent": 2.0}
    }]
    assert classify_severity_heuristic(low_fail) == "warning"


def test_passed_result_is_truthy():
    assert SAMPLE_PASS_RESULT["success"] is True


def test_failed_result_is_falsy():
    assert SAMPLE_FAIL_RESULT["success"] is False
