from __future__ import annotations

from datetime import date

import pytest

from config import load_sources
from parsers.jepx_intraday import parse as parse_jepx_intraday
from parsers.jepx_spot import parse as parse_jepx_spot
from parsers.occto_generation import parse as parse_occto_generation
from parsers.occto_intertie import parse as parse_occto_intertie
from parsers.occto_reserve import parse as parse_occto_reserve
from parsers.occto_trunk import parse as parse_occto_trunk
from tests.conftest import (
    build_generation_bytes,
    build_intertie_bytes,
    build_reserve_zip_bytes,
    build_trunk_bytes,
    load_fixture_text,
)
from validators.common import ValidationError, run_validations
from validators.config import build_validation_plan


def test_validator_accepts_all_source_samples() -> None:
    sources = load_sources()
    cases = [
        ("jepx_spot", parse_jepx_spot(load_fixture_text("jepx_spot_sample.csv").encode("cp932"), biz_date=date(2025, 4, 1))),
        ("jepx_intraday", parse_jepx_intraday(load_fixture_text("jepx_intraday_sample.csv").encode("cp932"), biz_date=date(2025, 4, 1))),
        ("occto_reserve_ratio", parse_occto_reserve(build_reserve_zip_bytes(), biz_date=date(2026, 3, 15))),
        ("occto_intertie", parse_occto_intertie(build_intertie_bytes(), biz_date=date(2026, 3, 15))),
        ("occto_trunk", parse_occto_trunk(build_trunk_bytes(), biz_date=date(2026, 3, 15))),
        ("occto_generation_unit", parse_occto_generation(build_generation_bytes(), biz_date=date(2026, 3, 15))),
    ]
    for source_name, parsed in cases:
        plan = build_validation_plan(sources[source_name])
        signature = run_validations(
            parsed.frame,
            source_name=source_name,
            observed_columns=parsed.raw_columns,
            expected_columns=plan.expected_columns,
            primary_key=plan.primary_key,
            validations=plan.validations,
            schema_patterns=plan.schema_patterns,
        )
        assert len(signature) == 64


def test_validator_detects_schema_drift_failure() -> None:
    sources = load_sources()
    parsed = parse_jepx_intraday(load_fixture_text("jepx_intraday_sample.csv").encode("cp932"), biz_date=date(2025, 4, 1))
    plan = build_validation_plan(sources["jepx_intraday"])
    with pytest.raises(ValidationError, match="schema drift"):
        run_validations(
            parsed.frame,
            source_name="jepx_intraday",
            observed_columns=parsed.raw_columns[:-1],
            expected_columns=plan.expected_columns,
            primary_key=plan.primary_key,
            validations=plan.validations,
            schema_patterns=plan.schema_patterns,
        )


def test_validator_detects_primary_key_duplicate() -> None:
    sources = load_sources()
    parsed = parse_occto_intertie(build_intertie_bytes(), biz_date=date(2026, 3, 15))
    duplicated = parsed.frame.vstack(parsed.frame.head(1))
    plan = build_validation_plan(sources["occto_intertie"])
    with pytest.raises(ValidationError, match="duplicate"):
        run_validations(
            duplicated,
            source_name="occto_intertie",
            observed_columns=parsed.raw_columns,
            expected_columns=plan.expected_columns,
            primary_key=plan.primary_key,
            validations=plan.validations,
            schema_patterns=plan.schema_patterns,
        )
