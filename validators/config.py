from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config import SourceConfig


@dataclass(slots=True)
class SourceValidationPlan:
    expected_columns: list[str]
    primary_key: list[str]
    validations: dict[str, Any]
    schema_patterns: dict[str, str] | None


def build_validation_plan(source: SourceConfig) -> SourceValidationPlan:
    return SourceValidationPlan(
        expected_columns=source.expected_columns,
        primary_key=source.primary_key,
        validations=source.validations,
        schema_patterns=source.schema_patterns,
    )
