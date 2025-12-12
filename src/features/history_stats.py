"""
Historical statistics for hydrology time series.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)


def compute_historical_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute per-gauge historical statistics.

    Returns a dict with:
      - monthly_stage: mean/std/median per month (1-12)
      - monthly_discharge: mean/std per month (if present)
      - stage_percentiles: p50, p85, p95 across all data
    """
    if df.empty:
        return {}

    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["month"] = df["timestamp"].dt.month

    stage_group = df.groupby("month")["stage_ft"]
    monthly_stage = {
        int(m): {
            "mean": float(stage_group.mean().get(m, float("nan"))),
            "std": float(stage_group.std().get(m, float("nan"))),
            "median": float(stage_group.median().get(m, float("nan"))),
        }
        for m in sorted(df["month"].unique())
    }

    monthly_discharge = None
    if "discharge_cfs" in df:
        discharge_group = df.groupby("month")["discharge_cfs"]
        monthly_discharge = {
            int(m): {
                "mean": float(discharge_group.mean().get(m, float("nan"))),
                "std": float(discharge_group.std().get(m, float("nan"))),
            }
            for m in sorted(df["month"].unique())
        }

    percentiles = {}
    if "stage_ft" in df:
        stage_vals = df["stage_ft"].dropna()
        if not stage_vals.empty:
            for p in (50, 85, 95):
                percentiles[f"p{p}"] = float(stage_vals.quantile(p / 100.0))

    stats: Dict[str, Any] = {
        "monthly_stage": monthly_stage,
        "stage_percentiles": percentiles,
    }
    if monthly_discharge is not None:
        stats["monthly_discharge"] = monthly_discharge
    return stats


def compute_stats_for_all(history_dir: str) -> Dict[str, Dict[str, Any]]:
    """
    Compute stats for all gauge history CSVs in a directory.

    Args:
        history_dir: Path to directory containing *_daily.csv files.

    Returns:
        Dict keyed by gauge_id with computed statistics.
    """
    base = Path(history_dir)
    if not base.exists():
        raise FileNotFoundError(f"History directory not found: {history_dir}")

    results: Dict[str, Dict[str, Any]] = {}
    for csv_path in base.glob("*_daily.csv"):
        gauge_id = csv_path.stem.replace("_daily", "")
        try:
            df = pd.read_csv(csv_path)
            stats = compute_historical_stats(df)
            results[gauge_id] = stats
            logger.info("Computed stats for %s", gauge_id)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to compute stats for %s: %s", gauge_id, exc)
            results[gauge_id] = {"error": str(exc)}
    return results
