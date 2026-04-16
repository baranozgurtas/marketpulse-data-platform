"""
MarketPulse — Unit Tests for Transform Logic
Tests core transformation functions without requiring Spark/Snowflake.
Run: pytest tests/test_transforms.py
"""

import pytest
import math


# -----------------------------------------------
# Pure function versions of transform logic
# (These mirror the Spark job logic for testability)
# -----------------------------------------------

def compute_return(current_close, prev_close):
    """Simple daily return."""
    if prev_close is None or prev_close <= 0:
        return None
    return (current_close - prev_close) / prev_close


def compute_log_return(current_close, prev_close):
    """Log daily return."""
    if prev_close is None or prev_close <= 0:
        return None
    return math.log(current_close / prev_close)


def compute_high_low_range(high, low, close):
    """Intraday range normalized by close."""
    if close is None or close <= 0:
        return None
    return (high - low) / close


def compute_max_drawdown(current_close, rolling_max):
    """Drawdown from rolling peak."""
    if rolling_max is None or rolling_max <= 0:
        return None
    return (current_close - rolling_max) / rolling_max


def classify_volatility_regime(vol_30d):
    """Classify volatility into regime buckets."""
    if vol_30d is None:
        return None
    if vol_30d < 0.02:
        return "LOW"
    elif vol_30d < 0.05:
        return "NORMAL"
    elif vol_30d < 0.08:
        return "HIGH"
    else:
        return "EXTREME"


def deduplicate_records(records, key_fields):
    """
    Keep only the last record (by insertion order) per key.
    Mirrors Spark's row_number() dedup pattern.
    """
    seen = {}
    for record in records:
        key = tuple(record[f] for f in key_fields)
        seen[key] = record  # Last one wins
    return list(seen.values())


# -----------------------------------------------
# Tests
# -----------------------------------------------

class TestReturns:
    def test_positive_return(self):
        assert compute_return(105, 100) == pytest.approx(0.05)

    def test_negative_return(self):
        assert compute_return(95, 100) == pytest.approx(-0.05)

    def test_zero_prev_close(self):
        assert compute_return(100, 0) is None

    def test_none_prev_close(self):
        assert compute_return(100, None) is None

    def test_log_return_positive(self):
        result = compute_log_return(105, 100)
        assert result == pytest.approx(math.log(1.05))

    def test_log_return_symmetry(self):
        """Log returns should be roughly symmetric for small changes."""
        up = compute_log_return(101, 100)
        down = compute_log_return(99, 100)
        assert abs(up + down) < 0.001


class TestHighLowRange:
    def test_normal_range(self):
        assert compute_high_low_range(110, 90, 100) == pytest.approx(0.2)

    def test_zero_range(self):
        assert compute_high_low_range(100, 100, 100) == pytest.approx(0.0)

    def test_zero_close(self):
        assert compute_high_low_range(110, 90, 0) is None


class TestMaxDrawdown:
    def test_at_peak(self):
        """No drawdown when current = peak."""
        assert compute_max_drawdown(100, 100) == pytest.approx(0.0)

    def test_drawdown(self):
        """10% drawdown."""
        assert compute_max_drawdown(90, 100) == pytest.approx(-0.1)

    def test_deep_drawdown(self):
        """50% drawdown."""
        assert compute_max_drawdown(50, 100) == pytest.approx(-0.5)


class TestVolatilityRegime:
    def test_low(self):
        assert classify_volatility_regime(0.01) == "LOW"

    def test_normal(self):
        assert classify_volatility_regime(0.03) == "NORMAL"

    def test_high(self):
        assert classify_volatility_regime(0.06) == "HIGH"

    def test_extreme(self):
        assert classify_volatility_regime(0.10) == "EXTREME"

    def test_none(self):
        assert classify_volatility_regime(None) is None

    def test_boundary_low_normal(self):
        assert classify_volatility_regime(0.02) == "NORMAL"

    def test_boundary_normal_high(self):
        assert classify_volatility_regime(0.05) == "HIGH"

    def test_boundary_high_extreme(self):
        assert classify_volatility_regime(0.08) == "EXTREME"


class TestDeduplication:
    def test_no_duplicates(self):
        records = [
            {"asset_id": "btc", "date": "2024-01-01", "close": 42000},
            {"asset_id": "eth", "date": "2024-01-01", "close": 2200},
        ]
        result = deduplicate_records(records, ["asset_id", "date"])
        assert len(result) == 2

    def test_with_duplicates(self):
        """Later record should win (simulating latest ingestion)."""
        records = [
            {"asset_id": "btc", "date": "2024-01-01", "close": 42000},
            {"asset_id": "btc", "date": "2024-01-01", "close": 42500},  # Updated
        ]
        result = deduplicate_records(records, ["asset_id", "date"])
        assert len(result) == 1
        assert result[0]["close"] == 42500

    def test_mixed(self):
        records = [
            {"asset_id": "btc", "date": "2024-01-01", "close": 42000},
            {"asset_id": "btc", "date": "2024-01-02", "close": 43000},
            {"asset_id": "btc", "date": "2024-01-01", "close": 42100},  # Dupe
            {"asset_id": "eth", "date": "2024-01-01", "close": 2200},
        ]
        result = deduplicate_records(records, ["asset_id", "date"])
        assert len(result) == 3
