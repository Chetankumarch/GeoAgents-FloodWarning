"""
USGS hydrology data ingestion for MVP1.

Fetches recent river stage and discharge for Sacramento-area gauges using the
USGS Water Services API.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import requests
import yaml

logger = logging.getLogger(__name__)

USGS_IV_URL = "https://waterservices.usgs.gov/nwis/iv/"
USGS_PARAMETERS = "00060,00065"  # 00060=discharge (cfs), 00065=gage height (ft)
REQUEST_TIMEOUT = 10


def fetch_usgs_gauge_data(gauge_id: str) -> Dict[str, Any]:
    """
    Fetch recent stage/discharge for a USGS gauge.

    Args:
        gauge_id: USGS site ID (e.g., "11447650").

    Returns:
        Dict with gauge_id, timestamp, stage_ft, discharge_cfs, and raw payload.

    Raises:
        requests.RequestException on network issues.
        ValueError if expected data is missing.
    """
    params = {
        "sites": gauge_id,
        "parameterCd": USGS_PARAMETERS,
        "format": "json",
        "siteStatus": "all",
    }
    logger.info("Fetching USGS data for gauge %s", gauge_id)
    try:
        resp = requests.get(USGS_IV_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("USGS request failed for gauge %s: %s", gauge_id, exc)
        raise

    try:
        payload = resp.json()
    except ValueError as exc:
        logger.error("Invalid JSON from USGS for gauge %s: %s", gauge_id, exc)
        raise

    time_series: List[Dict[str, Any]] = payload.get("value", {}).get("timeSeries", [])
    if not time_series:
        msg = f"No timeSeries data returned for gauge {gauge_id}"
        logger.error(msg)
        raise ValueError(msg)

    stage_ft = None
    discharge_cfs = None
    latest_ts = None

    for series in time_series:
        variable_info = series.get("variable", {})
        variable_codes = variable_info.get("variableCode", [])
        param_code = variable_codes[0].get("value") if variable_codes else None

        values = series.get("values", [])
        if not values:
            continue
        entries = values[0].get("value", [])
        if not entries:
            continue
        latest_entry = entries[-1]
        try:
            value = float(latest_entry.get("value"))
        except (TypeError, ValueError):
            continue
        timestamp = latest_entry.get("dateTime")

        if param_code == "00065":
            stage_ft = value
            latest_ts = timestamp or latest_ts
        elif param_code == "00060":
            discharge_cfs = value
            latest_ts = timestamp or latest_ts

    if stage_ft is None and discharge_cfs is None:
        msg = f"Missing stage/discharge values for gauge {gauge_id}"
        logger.error(msg)
        raise ValueError(msg)

    result = {
        "gauge_id": gauge_id,
        "timestamp": latest_ts,
        "stage_ft": stage_ft,
        "discharge_cfs": discharge_cfs,
        "raw": payload,
    }
    logger.info(
        "Fetched gauge %s | stage_ft=%s | discharge_cfs=%s",
        gauge_id,
        stage_ft,
        discharge_cfs,
    )
    return result


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


def fetch_all_gauges(config_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Fetch USGS data for all gauges in the provided config.

    Args:
        config_path: Path to YAML config with a `gauges` list.

    Returns:
        Dict keyed by gauge_id containing fetch results or error info.
    """
    gauges = _load_gauge_config(config_path)
    results: Dict[str, Dict[str, Any]] = {}

    for gauge in gauges:
        gauge_id = str(gauge.get("id"))
        if not gauge_id:
            logger.error("Skipping gauge with missing id in config: %s", gauge)
            continue
        try:
            results[gauge_id] = fetch_usgs_gauge_data(gauge_id)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to fetch gauge %s: %s", gauge_id, exc)
            results[gauge_id] = {"gauge_id": gauge_id, "error": str(exc)}

    return results
