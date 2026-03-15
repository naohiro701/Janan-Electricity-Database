from __future__ import annotations

from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo
import logging

from collectors.base import CollectedRaw, run_collector
from collectors.jepx_intraday import collect as collect_jepx_intraday
from collectors.jepx_spot import collect as collect_jepx_spot
from collectors.occto_generation import collect as collect_occto_generation
from collectors.occto_intertie import collect as collect_occto_intertie
from collectors.occto_reserve import collect as collect_occto_reserve
from collectors.occto_trunk import collect as collect_occto_trunk
from config import DATABASE_URL, load_sources
from loaders.db import load_dataset
from parsers.common import ParseResult
from parsers.jepx_intraday import parse as parse_jepx_intraday
from parsers.jepx_spot import parse as parse_jepx_spot
from parsers.occto_generation import parse as parse_occto_generation
from parsers.occto_intertie import parse as parse_occto_intertie
from parsers.occto_reserve import parse as parse_occto_reserve
from parsers.occto_trunk import parse as parse_occto_trunk
from validators.common import run_validations
from validators.config import build_validation_plan


JST = ZoneInfo("Asia/Tokyo")

COLLECTORS: dict[str, Callable] = {
    "jepx_spot": collect_jepx_spot,
    "jepx_intraday": collect_jepx_intraday,
    "occto_reserve_ratio": collect_occto_reserve,
    "occto_intertie": collect_occto_intertie,
    "occto_trunk": collect_occto_trunk,
    "occto_generation_unit": collect_occto_generation,
}

PARSERS: dict[str, Callable[..., ParseResult]] = {
    "jepx_spot": parse_jepx_spot,
    "jepx_intraday": parse_jepx_intraday,
    "occto_reserve_ratio": parse_occto_reserve,
    "occto_intertie": parse_occto_intertie,
    "occto_trunk": parse_occto_trunk,
    "occto_generation_unit": parse_occto_generation,
}


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sources = load_sources()
    selected_names = list(sources) if args.sources == "all" else [value.strip() for value in args.sources.split(",")]
    selected_sources = [sources[name] for name in selected_names]
    dates = _resolve_dates(args.mode, args.date, args.from_date, args.to_date)

    for biz_date in dates:
        for source in selected_sources:
            raw_items = run_collector(source, biz_date, args.mode, COLLECTORS[source.source_name])
            for raw_item in raw_items:
                parsed = PARSERS[source.source_name](raw_item.body, biz_date=biz_date)
                plan = build_validation_plan(source)
                schema_signature = run_validations(
                    parsed.frame,
                    source_name=source.source_name,
                    observed_columns=parsed.raw_columns,
                    expected_columns=plan.expected_columns,
                    primary_key=plan.primary_key,
                    validations=plan.validations,
                    schema_patterns=plan.schema_patterns,
                )
                if args.skip_db:
                    logging.info(
                        "skip_db source_name=%s biz_date=%s mode=%s",
                        source.source_name,
                        biz_date.isoformat(),
                        args.mode,
                    )
                    continue
                snapshot_id = load_dataset(args.database_url, source, raw_item, parsed, schema_signature)
                logging.info(
                    "loaded snapshot_id=%s source_name=%s biz_date=%s mode=%s",
                    snapshot_id,
                    source.source_name,
                    biz_date.isoformat(),
                    args.mode,
                )


def _parse_args(argv: list[str] | None) -> Any:
    parser = ArgumentParser(description="Run the public power data ingestion pipeline.")
    parser.add_argument("--mode", choices=("now", "daily", "backfill"), required=True)
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--from", dest="from_date", help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="YYYY-MM-DD")
    parser.add_argument("--sources", default="all", help="Comma separated source_name list or 'all'")
    parser.add_argument("--database-url", default=DATABASE_URL)
    parser.add_argument("--skip-db", action="store_true")
    return parser.parse_args(argv)


def _resolve_dates(mode: str, date_arg: str | None, from_arg: str | None, to_arg: str | None) -> list[date]:
    if mode == "now":
        return [_parse_iso_date(date_arg) if date_arg else datetime.now(tz=JST).date()]
    if mode == "daily":
        return [_parse_iso_date(date_arg) if date_arg else (datetime.now(tz=JST).date() - timedelta(days=1))]
    if not from_arg or not to_arg:
        raise RuntimeError("--mode backfill requires --from and --to")
    start = _parse_iso_date(from_arg)
    end = _parse_iso_date(to_arg)
    if end < start:
        raise RuntimeError("--to must be on or after --from")
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _parse_iso_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()
