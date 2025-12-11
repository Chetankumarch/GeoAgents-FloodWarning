"""
CLI entrypoint for GeoAgents-FloodWarning MVP1.

Usage:
    python -m src.cli.run_mvp1 --gauges config/sacramento_gauges.yml --thresholds config/thresholds.yml
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any, Dict

from src.core.risk_logic import classify_all
from src.data_ingestion.nws_fetch import fetch_all_gauges_forecast
from src.data_ingestion.usgs_fetch import fetch_all_gauges
from src.utils.config import load_yaml
from src.utils.logger import configure_logging


def run(gauge_config_path: str, thresholds_path: str) -> Dict[str, Any]:
    """Orchestrate data fetch and risk classification."""
    gauge_cfg = load_yaml(gauge_config_path)
    thresholds = load_yaml(thresholds_path)

    usgs_data = fetch_all_gauges(gauge_config_path)
    rainfall = fetch_all_gauges_forecast(gauge_config_path)

    risks = classify_all(
        usgs_data=usgs_data,
        rainfall_data=rainfall,
        gauge_config=gauge_cfg,
        thresholds=thresholds,
    )
    return {
        "gauges": risks,
        "usgs": usgs_data,
        "rainfall": rainfall,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MVP1 risk assessment.")
    parser.add_argument(
        "--gauges",
        default="config/sacramento_gauges.yml",
        help="Path to gauge config YAML.",
    )
    parser.add_argument(
        "--thresholds",
        default="config/thresholds.yml",
        help="Path to thresholds config YAML.",
    )
    parser.add_argument(
        "--loglevel",
        default=None,
        help="Logging level (e.g., INFO, DEBUG). Overrides LOGLEVEL env.",
    )
    args = parser.parse_args()

    configure_logging(level=args.loglevel)
    logging.getLogger(__name__).info("Starting MVP1 run")
    result = run(args.gauges, args.thresholds)
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
