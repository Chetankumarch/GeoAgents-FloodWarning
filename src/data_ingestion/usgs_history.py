"""
Historical hydrology ingestion using USGS daily values (DV) service.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests
import yaml

logger = logging.getLogger(__name__)

USGS_DV_URL = "https://waterservices.usgs.gov/nwis/dv/"
DV_PARAMS = "00060,00065"  # discharge (cfs), gage height (ft)
REQUEST_TIMEOUT = 20


def fetch_usgs_historical_data(gauge_id: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch historical daily stage/discharge for a gauge.

    Args:
        gauge_id: USGS site ID.
        start: ISO date string (YYYY-MM-DD).
        end: ISO date string (YYYY-MM-DD).

    Returns:
        DataFrame with columns: timestamp (datetime), stage_ft, discharge_cfs.
        Returns empty DataFrame on failure.
    """
    params = {
        "format": "json",
        "sites": gauge_id,
        "startDT": start,
        "endDT": end,
        "parameterCd": DV_PARAMS,
    }
    logger.info("Fetching historical USGS DV data for %s (%s to %s)", gauge_id, start, end)
    try:
        resp = requests.get(USGS_DV_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.error("Historical fetch failed for %s: %s", gauge_id, exc)
        return pd.DataFrame(columns=["timestamp", "stage_ft", "discharge_cfs"])

    series = payload.get("value", {}).get("timeSeries", [])
    rows = []
    for s in series:
        var_codes = s.get("variable", {}).get("variableCode", [])
        param_code = var_codes[0].get("value") if var_codes else None
        values = s.get("values", [])
        if not values:
            continue
        for entry in values[0].get("value", []):
            ts = entry.get("dateTime")
            try:
                val = float(entry.get("value"))
            except (TypeError, ValueError):
                continue
            if param_code == "00065":
                rows.append({"timestamp": ts, "stage_ft": val, "discharge_cfs": None})
            elif param_code == "00060":
                rows.append({"timestamp": ts, "stage_ft": None, "discharge_cfs": val})

    if not rows:
        logger.warning("No historical rows parsed for %s", gauge_id)
        return pd.DataFrame(columns=["timestamp", "stage_ft", "discharge_cfs"])

    df = pd.DataFrame(rows)
    # Combine stage/discharge rows by timestamp
    df = (
        df.groupby("timestamp", as_index=False)
        .agg({"stage_ft": "max", "discharge_cfs": "max"})
        .sort_values("timestamp")
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _load_gauges(config_path: str) -> Dict[str, Any]:
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Gauge config not found at {config_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    gauges = cfg.get("gauges", [])
    if not isinstance(gauges, list):
        raise ValueError("Invalid gauge list in config")
    return {"gauges": gauges}


def fetch_all_historical(config_path: str, years_back: int = 5) -> Dict[str, str]:
    """
    Fetch historical daily data for all gauges and save to CSV.

    Args:
        config_path: Path to YAML with gauge list.
        years_back: Number of years back from today to fetch.

    Returns:
        Dict mapping gauge_id -> saved CSV path (or error message).
    """
    cfg = _load_gauges(config_path)
    gauges = cfg["gauges"]

    end_date = date.today()
    start_date = end_date - timedelta(days=years_back * 365)

    out_dir = Path("data/history")
    out_dir.mkdir(parents=True, exist_ok=True)

    results: Dict[str, str] = {}
    for gauge in gauges:
        gauge_id = str(gauge.get("id"))
        if not gauge_id:
            logger.error("Skipping gauge with missing id: %s", gauge)
            continue
        try:
            df = fetch_usgs_historical_data(
                gauge_id=gauge_id,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
            )
            csv_path = out_dir / f"{gauge_id}_daily.csv"
            df.to_csv(csv_path, index=False)
            results[gauge_id] = str(csv_path)
            logger.info("Saved historical data for %s to %s", gauge_id, csv_path)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed historical fetch for %s: %s", gauge_id, exc)
            results[gauge_id] = f"error: {exc}"

    return results
