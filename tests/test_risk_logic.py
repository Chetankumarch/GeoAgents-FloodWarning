import pytest

from src.core.risk_logic import classify_gauge


@pytest.fixture
def thresholds():
    return {
        "rainfall_mm_72h": {"low": 0, "medium": 50, "high": 150},
        "river_stage_ratio": {"low": 0.7, "medium": 0.9, "high": 1.0},
    }


def test_risk_high_by_rain(thresholds):
    res = classify_gauge(
        gauge_id="123",
        stage_ft=5,
        flood_stage_ft=20,
        rain_mm_72h=200,
        thresholds=thresholds,
    )
    assert res["risk"] == "HIGH"
    assert res["rain_risk"] == "HIGH"


def test_risk_high_by_stage(thresholds):
    res = classify_gauge(
        gauge_id="123",
        stage_ft=10,
        flood_stage_ft=10,
        rain_mm_72h=10,
        thresholds=thresholds,
    )
    assert res["risk"] == "HIGH"
    assert res["stage_risk"] == "HIGH"


def test_risk_medium(thresholds):
    res = classify_gauge(
        gauge_id="123",
        stage_ft=7,
        flood_stage_ft=10,
        rain_mm_72h=60,
        thresholds=thresholds,
    )
    assert res["risk"] == "MEDIUM"
