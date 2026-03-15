from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.services.query_service import get_repository


class FakeMarketRepository:
    def fetch_spot(self, **kwargs):
        return [{"market_ts_jst": "2025-04-01T00:00:00", "area_name": "東京", "area_price_jpy_per_kwh": 15.41}]

    def fetch_intraday(self, **kwargs):
        return [{"market_ts_jst": "2025-04-01T00:00:00", "average_price_jpy_per_kwh": 13.9}]

    def fetch_reserve_ratio(self, **kwargs):
        return [{"market_ts_jst": "2026-03-15T00:30:00", "area_name": "北海道", "block_reserve_rate_pct": 19.41}]

    def fetch_intertie_flow(self, **kwargs):
        return []

    def fetch_trunk_flow(self, **kwargs):
        return []

    def fetch_generation_unit(self, **kwargs):
        return []


def test_market_endpoints_return_json_and_csv() -> None:
    app.dependency_overrides[get_repository] = lambda: FakeMarketRepository()
    client = TestClient(app)
    assert client.get("/spot").status_code == 200
    assert client.get("/intraday").json()[0]["average_price_jpy_per_kwh"] == 13.9
    csv_response = client.get("/reserve-ratio?format=csv")
    assert csv_response.status_code == 200
    assert "text/csv" in csv_response.headers["content-type"]
    app.dependency_overrides.clear()
