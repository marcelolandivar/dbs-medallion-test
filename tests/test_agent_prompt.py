"""
Validates that the agent response parser handles both well-formed and
edge-case LLM outputs correctly. No real API calls — mock-based only.
Run with: pytest tests/test_agent_prompt.py -v
"""
import json
import pytest
from unittest.mock import MagicMock, patch

REQUIRED_KEYS = {"severity", "root_cause", "summary", "remediation", "affected_downstream"}

VALID_RESPONSES = [
    {
        "severity": "critical",
        "root_cause": "schema_change",
        "summary": "18.5% null order_id values after schema migration.",
        "remediation": "Check upstream ETL column mapping in transform_silver notebook.",
        "affected_downstream": ["main.gold.daily_orders", "BI dashboard: Revenue Overview"]
    },
    {
        "severity": "warning",
        "root_cause": "data_drift",
        "summary": "Amount distribution shifted — mean dropped 40% vs 7-day baseline.",
        "remediation": "Review source system pricing changes with the product team.",
        "affected_downstream": []
    },
]

EDGE_CASES = [
    # Missing optional list key
    {
        "severity": "warning",
        "root_cause": "unknown",
        "summary": "Unclassified anomaly detected.",
        "remediation": "Investigate manually.",
    },
]


# ── parser (mirrors 04_ai_agent_analyse) ─────────────────────────────────────

def parse_agent_response(raw: str) -> dict:
    parsed = json.loads(raw)
    parsed.setdefault("affected_downstream", [])
    return parsed


# ── tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("payload", VALID_RESPONSES)
def test_valid_response_has_required_keys(payload):
    result = parse_agent_response(json.dumps(payload))
    assert REQUIRED_KEYS.issubset(result.keys()), \
        f"Missing keys: {REQUIRED_KEYS - result.keys()}"


@pytest.mark.parametrize("payload", VALID_RESPONSES)
def test_severity_is_valid(payload):
    result = parse_agent_response(json.dumps(payload))
    assert result["severity"] in ("critical", "warning", "passed")


@pytest.mark.parametrize("payload", VALID_RESPONSES)
def test_affected_downstream_is_list(payload):
    result = parse_agent_response(json.dumps(payload))
    assert isinstance(result["affected_downstream"], list)


def test_missing_affected_downstream_defaults_to_empty_list():
    payload = EDGE_CASES[0]
    result = parse_agent_response(json.dumps(payload))
    assert result["affected_downstream"] == []


def test_summary_within_length_limit():
    for payload in VALID_RESPONSES:
        result = parse_agent_response(json.dumps(payload))
        assert len(result["summary"]) <= 120, \
            f"Summary too long ({len(result['summary'])} chars): {result['summary']}"


def test_invalid_json_raises():
    with pytest.raises(json.JSONDecodeError):
        parse_agent_response("not valid json")
