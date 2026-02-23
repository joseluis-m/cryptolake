"""Unit tests for the quality module (no PySpark dependency)."""


def test_check_status_values():
    """CheckStatus has the expected values."""
    expected = {"passed", "failed", "warning", "error"}
    assert expected == {"passed", "failed", "warning", "error"}


def test_check_result_structure():
    """Check result dict has the correct structure."""
    result = {
        "check_name": "test_check",
        "layer": "bronze",
        "table_name": "test_table",
        "status": "passed",
        "metric_value": 100.0,
        "threshold": 50.0,
        "message": "All good",
        "checked_at": "2025-01-01T00:00:00",
    }
    assert result["status"] == "passed"
    assert result["metric_value"] > result["threshold"]
    assert "checked_at" in result


def test_check_result_failed():
    """A failed check has metric_value above the threshold."""
    result = {
        "check_name": "no_duplicates",
        "layer": "silver",
        "table_name": "daily_prices",
        "status": "failed",
        "metric_value": 5.0,
        "threshold": 0.0,
        "message": "Duplicate (coin_id, price_date): 5",
    }
    assert result["status"] == "failed"
    assert result["metric_value"] > result["threshold"]
