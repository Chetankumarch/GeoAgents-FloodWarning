"""
Rule-based flood risk logic for MVP1.
"""

from __future__ import annotations

from typing import Any, Dict


def _risk_from_rain(rain_mm: float, thresholds: Dict[str, float]) -> str:
    """
    Classify risk based on 72h rainfall.
    """
    if rain_mm is None:
        return "UNKNOWN"
    if rain_mm <= thresholds["medium"]:
        return "LOW" if rain_mm <= thresholds["low"] else "MEDIUM"
    return "HIGH" if rain_mm > thresholds["high"] else "MEDIUM"


def _risk_from_stage(stage_ft: float, flood_stage_ft: float, thresholds: Dict[str, float]) -> str:
    """
    Classify risk based on river stage ratio to flood stage.
    """
    if stage_ft is None or flood_stage_ft is None or flood_stage_ft == 0:
        return "UNKNOWN"
    ratio = stage_ft / flood_stage_ft
    if ratio <= thresholds["medium"]:
        return "LOW" if ratio <= thresholds["low"] else "MEDIUM"
    return "HIGH" if ratio > thresholds["high"] else "MEDIUM"


def classify_gauge(
    gauge_id: str,
    stage_ft: float,
    flood_stage_ft: float,
    rain_mm_72h: float,
    thresholds: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Classify a single gauge into LOW/MEDIUM/HIGH risk.

    Args:
        gauge_id: Gauge identifier.
        stage_ft: Latest stage in feet.
        flood_stage_ft: Configured flood stage for the gauge.
        rain_mm_72h: Forecast rainfall over next 72h in millimeters.
        thresholds: Dict containing rainfall_mm_72h and river_stage_ratio thresholds.

    Returns:
        Dict with gauge_id, risk, rain_risk, stage_risk, ratio, and inputs.
    """
    rain_thresholds = thresholds["rainfall_mm_72h"]
    stage_thresholds = thresholds["river_stage_ratio"]

    rain_risk = _risk_from_rain(rain_mm_72h, rain_thresholds)
    stage_risk = _risk_from_stage(stage_ft, flood_stage_ft, stage_thresholds)

    risk_order = ["LOW", "MEDIUM", "HIGH"]
    # Combine by taking the max severity among known risks; UNKNOWN treated as lowest.
    def rank(r: str) -> int:
        return risk_order.index(r) if r in risk_order else -1

    combined = max(rain_risk, stage_risk, key=rank)

    return {
        "gauge_id": gauge_id,
        "risk": combined,
        "rain_risk": rain_risk,
        "stage_risk": stage_risk,
        "stage_ratio": (stage_ft / flood_stage_ft) if stage_ft is not None and flood_stage_ft else None,
        "inputs": {
            "stage_ft": stage_ft,
            "flood_stage_ft": flood_stage_ft,
            "rain_mm_72h": rain_mm_72h,
        },
    }


def classify_all(
    usgs_data: Dict[str, Dict[str, Any]],
    rainfall_data: Dict[str, Dict[str, Any]],
    gauge_config: Dict[str, Any],
    thresholds: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Classify all gauges using USGS and NWS inputs.
    """
    # Map gauge_id -> flood_stage_ft for quick lookup
    flood_stage_lookup = {
        str(item["id"]): item.get("flood_stage_ft")
        for item in gauge_config.get("gauges", [])
        if "id" in item
    }

    results: Dict[str, Dict[str, Any]] = {}
    for gauge_id, obs in usgs_data.items():
        flood_stage = flood_stage_lookup.get(gauge_id)
        rain_entry = rainfall_data.get(gauge_id, {})
        rain_mm = rain_entry.get("rain_72h_mm")
        stage_ft = obs.get("stage_ft")
        results[gauge_id] = classify_gauge(
            gauge_id=gauge_id,
            stage_ft=stage_ft,
            flood_stage_ft=flood_stage,
            rain_mm_72h=rain_mm,
            thresholds=thresholds,
        )
    return results
