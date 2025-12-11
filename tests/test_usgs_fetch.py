import json
from types import SimpleNamespace

import pytest

from src.data_ingestion.usgs_fetch import fetch_usgs_gauge_data


class DummyResponse(SimpleNamespace):
    def raise_for_status(self):
        if getattr(self, "status_code", 200) >= 400:
            raise ValueError("bad status")


@pytest.fixture
def mock_usgs_response(monkeypatch):
    payload = {
        "value": {
            "timeSeries": [
                {
                    "variable": {"variableCode": [{"value": "00065"}]},
                    "values": [
                        {
                            "value": [
                                {"value": "8.5", "dateTime": "2024-02-10T12:00:00.000-08:00"}
                            ]
                        }
                    ],
                },
                {
                    "variable": {"variableCode": [{"value": "00060"}]},
                    "values": [
                        {
                            "value": [
                                {"value": "15000", "dateTime": "2024-02-10T12:00:00.000-08:00"}
                            ]
                        }
                    ],
                },
            ]
        }
    }

    def fake_get(url, params=None, timeout=10):
        return DummyResponse(
            status_code=200,
            json=lambda: json.loads(json.dumps(payload)),  # return a deep copy
            text="",
        )

    monkeypatch.setattr("requests.get", fake_get)


def test_fetch_usgs_gauge_data_parses_values(mock_usgs_response):
    result = fetch_usgs_gauge_data("11425500")
    assert result["gauge_id"] == "11425500"
    assert result["stage_ft"] == 8.5
    assert result["discharge_cfs"] == 15000.0
    assert result["timestamp"] is not None
