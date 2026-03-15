from __future__ import annotations

from datetime import timedelta
from hashlib import sha256
from typing import Any
import re

import polars as pl


class ValidationError(RuntimeError):
    """Raised when a dataset fails an explicit data quality rule."""


def validate_schema(
    observed_columns: list[str],
    expected_columns: list[str],
    source_name: str,
    schema_patterns: dict[str, str] | None = None,
) -> str:
    missing = [column for column in expected_columns if column not in observed_columns]
    compiled_patterns = [re.compile(pattern) for pattern in (schema_patterns or {}).values()]
    unexpected = [
        column
        for column in observed_columns
        if column not in expected_columns and not any(pattern.fullmatch(column) for pattern in compiled_patterns)
    ]
    if missing or unexpected:
        raise ValidationError(
            f"{source_name} schema drift detected; missing={missing or '[]'} unexpected={unexpected or '[]'}"
        )
    return sha256(",".join(observed_columns).encode("utf-8")).hexdigest()


def validate_unique(frame: pl.DataFrame, columns: list[str], source_name: str) -> None:
    if not columns:
        return
    duplicated = (
        frame.group_by(columns)
        .len()
        .filter(pl.col("len") > 1)
    )
    if not duplicated.is_empty():
        sample = duplicated.head(5).to_dicts()
        raise ValidationError(f"{source_name} primary key duplicate detected on {columns}: {sample}")


def validate_not_null(frame: pl.DataFrame, columns: list[str], source_name: str) -> None:
    for column in columns:
        if column not in frame.columns:
            continue
        null_rows = frame.filter(pl.col(column).is_null() | (pl.col(column).cast(pl.Utf8) == ""))
        if not null_rows.is_empty():
            raise ValidationError(f"{source_name} not_null check failed for column={column}")


def validate_range(frame: pl.DataFrame, range_rules: dict[str, dict[str, float]], source_name: str) -> None:
    for column, rule in range_rules.items():
        if column not in frame.columns:
            continue
        predicate = pl.lit(False)
        if "min" in rule:
            predicate = predicate | (pl.col(column) < rule["min"])
        if "max" in rule:
            predicate = predicate | (pl.col(column) > rule["max"])
        outliers = frame.filter(predicate)
        if not outliers.is_empty():
            sample = outliers.select(column).head(5).to_series().to_list()
            raise ValidationError(f"{source_name} range check failed for column={column}: {sample}")


def validate_missing_dates(frame: pl.DataFrame, ts_col: str, frequency_minutes: int, source_name: str) -> None:
    if ts_col not in frame.columns or frame.is_empty():
        return
    timestamps = sorted(set(frame.get_column(ts_col).to_list()))
    if len(timestamps) <= 1:
        return
    expected_delta = timedelta(minutes=frequency_minutes)
    gaps = []
    for previous, current in zip(timestamps, timestamps[1:]):
        if current - previous != expected_delta:
            gaps.append((previous, current))
    if gaps:
        raise ValidationError(f"{source_name} missing date/time gap detected: {gaps[:5]}")


def run_validations(
    frame: pl.DataFrame,
    *,
    source_name: str,
    observed_columns: list[str],
    expected_columns: list[str],
    primary_key: list[str],
    validations: dict[str, Any],
    schema_patterns: dict[str, str] | None = None,
) -> str:
    signature = validate_schema(observed_columns, expected_columns, source_name, schema_patterns=schema_patterns)
    validate_unique(frame, primary_key, source_name)
    validate_not_null(frame, validations.get("not_null", []), source_name)
    validate_range(frame, validations.get("range", {}), source_name)
    missing_dates = validations.get("missing_dates", {})
    frequency_minutes = missing_dates.get("frequency_minutes")
    if frequency_minutes:
        validate_missing_dates(frame, "market_ts_jst", int(frequency_minutes), source_name)
    return signature
