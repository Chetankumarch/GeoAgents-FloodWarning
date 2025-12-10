"""
NWS/NOAA weather ingestion for MVP1.

Fetches gridpoint forecast metadata and quantitative precipitation forecasts
to compute 72-hour expected rainfall for configured gauges.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

logger = logging.getLogger(__name__)

POINTS_URL_TEMPLATE = "https://api.weather.gov/points/{lat},{lon}"
REQUEST_TIMEOUT = 10


def get_point_metadata(lat: float, lon: float) -> Dict[str, Any]:
    """
    Resolve NWS grid metadata for a latitude/longitude.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        Dict with grid_id, grid_x, grid_y, and forecast_url.
    """
    url = POINTS_URL_TEMPLATE.format(lat=lat, lon=lon)
    logger.info("Fetching NWS point metadata for (%s, %s)", lat, lon)
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("NWS point metadata request failed for (%s,%s): %s", lat, lon, exc)
        raise

    try:
        payload = resp.json()
    except ValueError as exc:
        logger.error("Invalid JSON from NWS point metadata: %s", exc)
        raise

    props = payload.get("properties") or {}
    grid_id = props.get("gridId")
    grid_x = props.get("gridX")
    grid_y = props.get("gridY")
    forecast_url = props.get("forecastGridData")

    if not (grid_id and grid_x is not None and grid_y is not None and forecast_url):
        msg = f"Incomplete point metadata for ({lat},{lon})"
        logger.error(msg)
        raise ValueError(msg)

    return {
        "grid_id": grid_id,
        "grid_x": grid_x,
        "grid_y": grid_y,
        "forecast_url": forecast_url,
    }


def _parse_valid_time(valid_time: str) -> Tuple[datetime, timedelta]:
    """
    Parse NWS validTime strings like '2024-02-10T00:00:00+00:00/PT1H'.
    """
    try:
        start_str, duration_str = valid_time.split("/")
        start_dt = datetime.fromisoformat(start_str)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        hours = 0
        if duration_str.startswith("PT") and duration_str.endswith("H"):
            hours = float(duration_str[2:-1])
        elif duration_str.startswith("PT") and duration_str.endswith("M"):
            # Minutes duration; convert to hours
            hours = float(duration_str[2:-1]) / 60.0
        duration = timedelta(hours=hours)
        return start_dt, duration
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to parse validTime %s: %s", valid_time, exc)
        raise


def fetch_nws_forecast(forecast_url: str) -> Dict[str, Any]:
    """
    Fetch gridpoint forecast data from NWS.

    Args:
        forecast_url: URL from NWS point metadata (forecastGridData).

    Returns:
        Dict containing parsed quantitative precipitation (qpf) periods and raw payload.
    """
    logger.info("Fetching NWS forecast from %s", forecast_url)
    try:
        resp = requests.get(forecast_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("NWS forecast request failed: %s", exc)
        raise

    try:
        payload = resp.json()
    except ValueError as exc:
        logger.error("Invalid JSON from NWS forecast: %s", exc)
        raise

    props = payload.get("properties") or {}
    qpf = props.get("quantitativePrecipitation", {})
    pop = props.get("probabilityOfPrecipitation", {})

    qpf_entries: List[Dict[str, Any]] = []
    for item in qpf.get("values", []) or []:
        valid_time = item.get("validTime")
        value_mm = item.get("value")  # kg/m^2 ~ mm
        if valid_time is None or value_mm is None:
            continue
        try:
            start_dt, duration = _parse_valid_time(valid_time)
        except Exception:
            continue
        qpf_entries.append(
            {
                "start": start_dt,
                "duration": duration,
                "value_mm": float(value_mm) if value_mm is not None else None,
            }
        )

    pop_entries: List[Dict[str, Any]] = []
    for item in pop.get("values", []) or []:
        valid_time = item.get("validTime")
        value_pct = item.get("value")
        if valid_time is None:
            continue
        try:
            start_dt, duration = _parse_valid_time(valid_time)
        except Exception:
            continue
        pop_entries.append(
            {
                "start": start_dt,
                "duration": duration,
                "probability_pct": value_pct,
            }
        )

    if not qpf_entries:
        logger.warning("No QPF entries parsed from NWS forecast at %s", forecast_url)

    return {"qpf": qpf_entries, "pop": pop_entries, "raw": payload}


def compute_72h_rain_mm(forecast: Dict[str, Any]) -> float:
    """
    Compute total expected precipitation over the next 72 hours.

    Assumptions:
    - quantitativePrecipitation values are mm over the interval in validTime.
    - Rainfall is evenly distributed across each interval; overlapping portions
      with the next 72 hours window are proportionally counted.
    """
    window_start = datetime.now(timezone.utc)
    window_end = window_start + timedelta(hours=72)
    total_mm = 0.0

    for entry in forecast.get("qpf", []) or []:
        start = entry.get("start")
        duration: Optional[timedelta] = entry.get("duration")
        value_mm = entry.get("value_mm")
        if start is None or duration is None or value_mm is None:
            continue
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        end = start + duration

        overlap_start = max(start, window_start)
        overlap_end = min(end, window_end)
        if overlap_start >= overlap_end:
            continue
        interval_hours = duration.total_seconds() / 3600
        overlap_hours = (overlap_end - overlap_start).total_seconds() / 3600
        if interval_hours <= 0:
            continue
        # Pro-rate rainfall for overlapping hours.
        total_mm += value_mm * (overlap_hours / interval_hours)

    return total_mm


def _load_gauge_config(config_path: str) -> List[Dict[str, Any]]:
    """Load gauge metadata from YAML config."""
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Gauge config not found at {config_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    gauges = cfg.get("gauges", [])
    if not isinstance(gauges, list):
        raise ValueError("Invalid gauge list in config")
    return gauges


def fetch_all_gauges_forecast(config_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Fetch 72-hour precipitation forecasts for all gauges in config.

    Args:
        config_path: Path to YAML config with a `gauges` list.

    Returns:
        Dict keyed by gauge_id containing rain_72h_mm and raw forecast data.
    """
    gauges = _load_gauge_config(config_path)
    results: Dict[str, Dict[str, Any]] = {}

    for gauge in gauges:
        gauge_id = str(gauge.get("id"))
        lat = gauge.get("latitude")
        lon = gauge.get("longitude")
        if not gauge_id or lat is None or lon is None:
            logger.error("Skipping gauge with missing fields in config: %s", gauge)
            continue

        try:
            meta = get_point_metadata(lat, lon)
            forecast = fetch_nws_forecast(meta["forecast_url"])
            rain_72h = compute_72h_rain_mm(forecast)
            results[gauge_id] = {
                "gauge_id": gauge_id,
                "lat": lat,
                "lon": lon,
                "rain_72h_mm": rain_72h,
                "raw": forecast.get("raw"),
            }
            logger.info(
                "Gauge %s 72h rain estimate: %.2f mm", gauge_id, rain_72h
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to fetch forecast for gauge %s: %s", gauge_id, exc)
            results[gauge_id] = {"gauge_id": gauge_id, "error": str(exc)}

    return results


# Example usage (for debugging/manual run):
# from src.data_ingestion.nws_fetch import fetch_all_gauges_forecast
# data = fetch_all_gauges_forecast("config/sacramento_gauges.yml")
# print(data.get("11425500"))
