from __future__ import annotations

from contextlib import nullcontext
from datetime import date

from config import load_sources
from loaders.db import load_dataset
from parsers.jepx_spot import parse as parse_jepx_spot
from tests.conftest import load_fixture_text, make_collected_raw


class FakeResult:
    def __init__(self, *, one=None, many=None):
        self._one = one
        self._many = many or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.executed_many = []
        self._asset_seq = 300

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def transaction(self):
        return nullcontext(self)

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "SELECT area_id, area_name FROM dim_area" in sql:
            return FakeResult(many=[(1, "北海道"), (2, "東北"), (3, "東京"), (4, "中部"), (5, "北陸"), (6, "関西"), (7, "中国"), (8, "四国"), (9, "九州")])
        if "INSERT INTO source_snapshot" in sql:
            return FakeResult(one=(101,))
        if "INSERT INTO dim_market" in sql:
            return FakeResult(one=(201,))
        if "INSERT INTO dim_asset" in sql:
            self._asset_seq += 1
            return FakeResult(one=(self._asset_seq,))
        return FakeResult()

    def executemany(self, sql, params_seq):
        self.executed_many.append((sql, list(params_seq)))


def test_load_dataset_upserts_snapshot_and_fact(monkeypatch) -> None:
    raw = load_fixture_text("jepx_spot_sample.csv").encode("cp932")
    parsed = parse_jepx_spot(raw, biz_date=date(2025, 4, 1))
    collected = make_collected_raw("jepx_spot", "jepx_spot_sample.csv", raw)
    fake_conn = FakeConnection()
    monkeypatch.setattr("loaders.db.psycopg.connect", lambda dsn: fake_conn)
    sources = load_sources()
    snapshot_id = load_dataset("postgresql://example", sources["jepx_spot"], collected, parsed, "a" * 64)
    assert snapshot_id == 101
    assert fake_conn.executed_many, "fact upsert should be executed"
    fact_sql, fact_records = fake_conn.executed_many[0]
    assert "fact_spot_30m" in fact_sql
    assert len(fact_records) == 18
