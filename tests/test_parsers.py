from __future__ import annotations

from datetime import date
import json

from parsers.jepx_intraday import parse as parse_jepx_intraday
from parsers.jepx_spot import parse as parse_jepx_spot
from parsers.occto_generation import parse as parse_occto_generation
from parsers.occto_intertie import parse as parse_occto_intertie
from parsers.occto_reserve import parse as parse_occto_reserve
from parsers.occto_trunk import parse as parse_occto_trunk
from tests.conftest import (
    FIXTURE_DIR,
    build_generation_bytes,
    build_intertie_bytes,
    build_reserve_zip_bytes,
    build_trunk_bytes,
    load_fixture_text,
)


def test_parse_jepx_spot_returns_long_rows() -> None:
    raw = load_fixture_text("jepx_spot_sample.csv").encode("cp932")
    result = parse_jepx_spot(raw, biz_date=date(2025, 4, 1))
    assert result.frame.height == 18
    assert result.frame.columns[0] == "market_ts_jst"
    assert result.frame.row(0)[1] == "北海道"


def test_parse_jepx_intraday_returns_expected_columns() -> None:
    raw = load_fixture_text("jepx_intraday_sample.csv").encode("cp932")
    result = parse_jepx_intraday(raw, biz_date=date(2025, 4, 1))
    assert result.frame.height == 2
    assert "average_price_jpy_per_kwh" in result.frame.columns


def test_parse_occto_reserve_zip_extracts_block_csv() -> None:
    result = parse_occto_reserve(build_reserve_zip_bytes(), biz_date=date(2026, 3, 15))
    assert result.frame.height == 2
    assert result.metadata["zip_member"].startswith("広域予備率ブロック情報")


def test_parse_occto_intertie_maps_area_names() -> None:
    result = parse_occto_intertie(build_intertie_bytes(), biz_date=date(2026, 3, 15))
    first = result.frame.to_dicts()[0]
    assert first["source_area_name"] == "北海道"
    assert first["target_area_name"] == "東北"


def test_parse_occto_trunk_melts_slot_columns() -> None:
    result = parse_occto_trunk(build_trunk_bytes(), biz_date=date(2026, 3, 15))
    assert result.frame.height == 3
    assert result.frame.to_dicts()[0]["asset_code"] == "北海道|275kV|北本連系幹線"


def test_parse_occto_generation_melts_unit_rows() -> None:
    result = parse_occto_generation(build_generation_bytes(), biz_date=date(2026, 3, 15))
    assert result.frame.height == 3
    assert result.frame.to_dicts()[0]["plant_code"] == "P001"


def test_jepx_spot_matches_golden_file() -> None:
    raw = load_fixture_text("jepx_spot_sample.csv").encode("cp932")
    result = parse_jepx_spot(raw, biz_date=date(2025, 4, 1))
    actual = []
    for row in result.frame.head(9).to_dicts():
        normalized = dict(row)
        normalized["market_ts_jst"] = normalized["market_ts_jst"].isoformat()
        actual.append(normalized)
    expected = json.loads((FIXTURE_DIR / "golden" / "jepx_spot_2025_04_01.json").read_text(encoding="utf-8"))
    assert actual == expected
