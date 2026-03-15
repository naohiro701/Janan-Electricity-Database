from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.services.query_service import get_repository


class FakeGridRepository:
    def fetch_spot(self, **kwargs):
        return []

    def fetch_intraday(self, **kwargs):
        return []

    def fetch_reserve_ratio(self, **kwargs):
        return []

    def fetch_intertie_flow(self, **kwargs):
        return [{"market_ts_jst": "2026-03-15T00:00:00", "asset_name": "北海道・本州間電力連系設備", "actual_flow_mw": 120.5}]

    def fetch_trunk_flow(self, **kwargs):
        return [{"market_ts_jst": "2026-03-15T00:00:00", "area_name": "北海道", "asset_name": "北本連系幹線", "flow_mw": 100.0}]

    def fetch_generation_unit(self, **kwargs):
        return [{"market_ts_jst": "2026-03-15T00:30:00", "asset_code": "P001|1号機", "generation_kwh": 1000.0}]


def test_grid_endpoints_return_payloads() -> None:
    app.dependency_overrides[get_repository] = lambda: FakeGridRepository()
    client = TestClient(app)
    assert client.get("/intertie-flow").json()[0]["actual_flow_mw"] == 120.5
    assert client.get("/trunk-flow").json()[0]["asset_name"] == "北本連系幹線"
    assert client.get("/generation-unit?format=csv").status_code == 200
    app.dependency_overrides.clear()
