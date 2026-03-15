from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json

import polars as pl
import psycopg

from collectors.base import CollectedRaw
from config import ROOT_DIR, SourceConfig
from parsers.common import ParseResult


SQL_DIR = ROOT_DIR / "sql"


@dataclass(slots=True)
class SnapshotRecord:
    source_name: str
    biz_date: Any
    mode: str
    domain: str
    page_url: str
    fetch_mode: str
    file_format: str
    raw_relpath: str
    raw_filename: str
    raw_sha256: str
    raw_size_bytes: int
    encoding: str
    fetched_at: Any
    expected_columns: str
    observed_columns: str
    schema_signature: str
    http_status: int | None
    content_type: str | None
    metadata: str


def upsert_snapshot(conn: psycopg.Connection[Any], record: SnapshotRecord) -> int:
    row = conn.execute(_load_sql("upsert_snapshot.sql"), record.__dict__).fetchone()
    if not row:
        raise RuntimeError("upsert_snapshot did not return snapshot_id")
    return int(row[0])


def upsert_dim_asset(conn: psycopg.Connection[Any], records: list[dict[str, Any]]) -> dict[str, int]:
    if not records:
        return {}
    sql = _load_sql("upsert_dim_asset.sql")
    asset_id_map: dict[str, int] = {}
    for record in _dedupe_records(records, key_fields=("asset_type", "asset_code")):
        row = conn.execute(sql, record).fetchone()
        if not row:
            raise RuntimeError(f"upsert_dim_asset failed for asset_code={record['asset_code']}")
        asset_id_map[record["asset_code"]] = int(row[0])
    return asset_id_map


def upsert_dim_market(
    conn: psycopg.Connection[Any],
    market_code: str,
    market_name: str,
    description: str = "",
) -> int:
    row = conn.execute(
        _load_sql("upsert_dim_market.sql"),
        {"market_code": market_code, "market_name": market_name, "description": description},
    ).fetchone()
    if not row:
        raise RuntimeError(f"upsert_dim_market failed for market_code={market_code}")
    return int(row[0])


def upsert_fact(conn: psycopg.Connection[Any], sql_file: str, records: list[dict[str, Any]]) -> None:
    if not records:
        raise RuntimeError(f"no fact records prepared for {sql_file}")
    conn.executemany(_load_sql(sql_file), records)


def load_dataset(
    dsn: str,
    source: SourceConfig,
    collected: CollectedRaw,
    parsed: ParseResult,
    schema_signature: str,
) -> int:
    if not dsn:
        raise RuntimeError("DATABASE_URL is required for DB loading")

    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            area_ids = _load_area_ids(conn)
            snapshot_id = upsert_snapshot(
                conn,
                SnapshotRecord(
                    source_name=source.source_name,
                    biz_date=collected.biz_date,
                    mode=collected.mode,
                    domain=source.domain,
                    page_url=source.page_url,
                    fetch_mode=source.fetch_mode,
                    file_format=source.file_format,
                    raw_relpath=_relative_path(collected.saved_path),
                    raw_filename=collected.raw_filename,
                    raw_sha256=collected.raw_sha256,
                    raw_size_bytes=collected.raw_size_bytes,
                    encoding=parsed.encoding or collected.encoding,
                    fetched_at=collected.fetched_at,
                    expected_columns=json.dumps(source.expected_columns, ensure_ascii=False),
                    observed_columns=json.dumps(parsed.raw_columns, ensure_ascii=False),
                    schema_signature=schema_signature,
                    http_status=collected.http_status,
                    content_type=collected.content_type,
                    metadata=json.dumps(collected.metadata | parsed.metadata, ensure_ascii=False),
                ),
            )
            _load_fact_rows(conn, source, parsed.frame, area_ids, snapshot_id, collected)
            return snapshot_id


def _load_fact_rows(
    conn: psycopg.Connection[Any],
    source: SourceConfig,
    frame: pl.DataFrame,
    area_ids: dict[str, int],
    snapshot_id: int,
    collected: CollectedRaw,
) -> None:
    if source.source_name == "jepx_spot":
        market_id = upsert_dim_market(conn, "jepx_spot", "JEPX Spot", "JEPX spot market")
        records = []
        for row in frame.iter_rows(named=True):
            records.append(
                {
                    **row,
                    "snapshot_id": snapshot_id,
                    "market_id": market_id,
                    "area_id": area_ids[row["area_name"]],
                    "fetched_at": collected.fetched_at,
                }
            )
        upsert_fact(conn, source.loader["fact_sql"].split("/")[-1], records)
        return

    if source.source_name == "jepx_intraday":
        market_id = upsert_dim_market(conn, "jepx_intraday", "JEPX Intraday", "JEPX intraday market")
        records = []
        for row in frame.iter_rows(named=True):
            records.append(
                {
                    **row,
                    "snapshot_id": snapshot_id,
                    "market_id": market_id,
                    "contract_count": int(row["contract_count"] or 0),
                    "fetched_at": collected.fetched_at,
                }
            )
        upsert_fact(conn, source.loader["fact_sql"].split("/")[-1], records)
        return

    if source.source_name == "occto_reserve_ratio":
        records = []
        for row in frame.iter_rows(named=True):
            records.append(
                {
                    **row,
                    "snapshot_id": snapshot_id,
                    "area_id": area_ids[row["area_name"]],
                    "block_no": int(row["block_no"]),
                    "fetched_at": collected.fetched_at,
                }
            )
        upsert_fact(conn, source.loader["fact_sql"].split("/")[-1], records)
        return

    if source.source_name == "occto_intertie":
        assets = upsert_dim_asset(
            conn,
            [
                {
                    "asset_type": "intertie",
                    "asset_code": row["asset_code"],
                    "asset_name": row["asset_name"],
                    "area_id": None,
                    "from_area_id": area_ids.get(row["source_area_name"]),
                    "to_area_id": area_ids.get(row["target_area_name"]),
                    "voltage_kv": None,
                    "source_type": None,
                    "metadata": json.dumps(
                        {
                            "source_area_name": row["source_area_name"],
                            "target_area_name": row["target_area_name"],
                        },
                        ensure_ascii=False,
                    ),
                }
                for row in frame.iter_rows(named=True)
            ],
        )
        records = []
        for row in frame.iter_rows(named=True):
            records.append(
                {
                    "market_ts_jst": row["market_ts_jst"],
                    "snapshot_id": snapshot_id,
                    "asset_id": assets[row["asset_code"]],
                    "actual_flow_mw": row["actual_flow_mw"],
                    "fetched_at": collected.fetched_at,
                }
            )
        upsert_fact(conn, source.loader["fact_sql"].split("/")[-1], records)
        return

    if source.source_name == "occto_trunk":
        assets = upsert_dim_asset(
            conn,
            [
                {
                    "asset_type": "trunk_line",
                    "asset_code": row["asset_code"],
                    "asset_name": row["asset_name"],
                    "area_id": area_ids[row["area_name"]],
                    "from_area_id": None,
                    "to_area_id": None,
                    "voltage_kv": row["voltage_kv"],
                    "source_type": None,
                    "metadata": json.dumps({"positive_direction_label": row["positive_direction_label"]}, ensure_ascii=False),
                }
                for row in frame.iter_rows(named=True)
            ],
        )
        records = []
        for row in frame.iter_rows(named=True):
            records.append(
                {
                    "market_ts_jst": row["market_ts_jst"],
                    "snapshot_id": snapshot_id,
                    "asset_id": assets[row["asset_code"]],
                    "area_id": area_ids[row["area_name"]],
                    "flow_mw": row["flow_mw"],
                    "fetched_at": collected.fetched_at,
                }
            )
        upsert_fact(conn, source.loader["fact_sql"].split("/")[-1], records)
        return

    if source.source_name == "occto_generation_unit":
        assets = upsert_dim_asset(
            conn,
            [
                {
                    "asset_type": "generation_unit",
                    "asset_code": row["asset_code"],
                    "asset_name": row["asset_name"],
                    "area_id": area_ids[row["area_name"]],
                    "from_area_id": None,
                    "to_area_id": None,
                    "voltage_kv": None,
                    "source_type": row["source_type"],
                    "metadata": json.dumps(
                        {
                            "plant_code": row["plant_code"],
                            "plant_name": row["plant_name"],
                            "unit_name": row["unit_name"],
                        },
                        ensure_ascii=False,
                    ),
                }
                for row in frame.iter_rows(named=True)
            ],
        )
        records = []
        for row in frame.iter_rows(named=True):
            records.append(
                {
                    "market_ts_jst": row["market_ts_jst"],
                    "snapshot_id": snapshot_id,
                    "asset_id": assets[row["asset_code"]],
                    "area_id": area_ids[row["area_name"]],
                    "generation_kwh": row["generation_kwh"],
                    "daily_total_kwh": row["daily_total_kwh"],
                    "updated_at_jst": row["updated_at_jst"],
                    "fetched_at": collected.fetched_at,
                }
            )
        upsert_fact(conn, source.loader["fact_sql"].split("/")[-1], records)
        return

    raise RuntimeError(f"unsupported source_name for DB loading: {source.source_name}")


def _load_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def _load_area_ids(conn: psycopg.Connection[Any]) -> dict[str, int]:
    rows = conn.execute("SELECT area_id, area_name FROM dim_area").fetchall()
    return {str(area_name): int(area_id) for area_id, area_name in rows}


def _relative_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT_DIR))
    except ValueError:
        return str(path)


def _dedupe_records(records: list[dict[str, Any]], key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    output: list[dict[str, Any]] = []
    for record in records:
        key = tuple(record[field] for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        output.append(record)
    return output
