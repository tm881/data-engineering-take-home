from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

import pandas as pd


ValidMaskFn = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class TableCheck:
    name: str
    description: str
    valid_mask_fn: ValidMaskFn


@dataclass
class CleanResult:
    cleaned_df: pd.DataFrame
    summary_df: pd.DataFrame


def check_duplicates(
    *,
    subset: Iterable[str],
    keep: str = "first",
    name: str | None = None,
) -> TableCheck:
    subset = list(subset)
    return TableCheck(
        name=name or f"drop_duplicate_{'_'.join(subset)}",
        description=f"Keep only the {keep} row for duplicate keys on {subset}.",
        valid_mask_fn=lambda df: ~df.duplicated(subset=subset, keep=keep),
    )


def check_not_null(*, columns: Iterable[str], name: str | None = None) -> TableCheck:
    columns = list(columns)
    return TableCheck(
        name=name or f"require_not_null_{'_'.join(columns)}",
        description=f"Require non-null values in {columns}.",
        valid_mask_fn=lambda df: df[columns].notna().all(axis=1),
    )


def check_non_negative(
    *, column: str, allow_zero: bool = True, name: str | None = None
) -> TableCheck:
    comparator = ">=" if allow_zero else ">"
    threshold = 0
    return TableCheck(
        name=name or f"require_{column}_{'non_negative' if allow_zero else 'positive'}",
        description=f"Require {column} {comparator} {threshold}.",
        valid_mask_fn=(
            (lambda df: df[column] >= 0)
            if allow_zero
            else (lambda df: df[column] > 0)
        ),
    )


def check_valid_window(
    *,
    start_col: str,
    end_col: str,
    allow_open_end: bool = True,
    name: str | None = None,
) -> TableCheck:
    def valid_mask(df: pd.DataFrame) -> pd.Series:
        end_after_start = df[end_col] > df[start_col]
        if allow_open_end:
            return df[end_col].isna() | end_after_start
        return df[end_col].notna() & end_after_start

    return TableCheck(
        name=name or f"require_valid_window_{start_col}_{end_col}",
        description=(
            f"Require valid windows where {end_col} is null or later than {start_col}."
            if allow_open_end
            else f"Require {end_col} later than {start_col}."
        ),
        valid_mask_fn=valid_mask,
    )


def check_comparison(
    *,
    left_col: str,
    right_col: str,
    op: str,
    name: str | None = None,
) -> TableCheck:
    operations: dict[str, Callable[[pd.Series, pd.Series], pd.Series]] = {
        "<=": lambda left, right: left <= right,
        "<": lambda left, right: left < right,
        ">=": lambda left, right: left >= right,
        ">": lambda left, right: left > right,
    }
    if op not in operations:
        raise ValueError(f"Unsupported comparison operator: {op}")

    return TableCheck(
        name=name or f"require_{left_col}_{op}_{right_col}".replace("=", "eq"),
        description=f"Require {left_col} {op} {right_col}.",
        valid_mask_fn=lambda df: operations[op](df[left_col], df[right_col]),
    )


def check_custom(
    *, name: str, description: str, valid_mask_fn: ValidMaskFn
) -> TableCheck:
    return TableCheck(name=name, description=description, valid_mask_fn=valid_mask_fn)


def clean_dataframe(
    df: pd.DataFrame,
    *,
    checks: list[TableCheck],
    sort_by: list[str] | None = None,
    ascending: list[bool] | bool | None = None,
) -> CleanResult:
    working_df = df.copy()
    summary_rows: list[dict[str, object]] = []

    if sort_by is not None:
        working_df = working_df.sort_values(sort_by, ascending=ascending).copy()

    for check in checks:
        before_count = len(working_df)
        valid_mask = check.valid_mask_fn(working_df).fillna(False)
        working_df = working_df.loc[valid_mask].copy()
        after_count = len(working_df)
        summary_rows.append(
            {
                "check_name": check.name,
                "description": check.description,
                "rows_before": before_count,
                "rows_removed": before_count - after_count,
                "rows_after": after_count,
            }
        )

    return CleanResult(
        cleaned_df=working_df,
        summary_df=pd.DataFrame(summary_rows),
    )
