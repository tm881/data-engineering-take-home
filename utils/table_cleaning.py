from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from utils.dataframe_cleaner import (
    TableCheck,
    check_comparison,
    check_custom,
    check_duplicates,
    check_non_negative,
    check_not_null,
    check_valid_window,
    clean_dataframe,
)


TECH_MAP = {
    "LTE": "4G",
    "4g": "4G",
    "4G": "4G",
    "lte": "4G",
    "5g": "5G",
    "5G": "5G",
    "NR": "5G",
    "HSPA+": "3G",
    "CDMA": "3G",
    "GSM": "3G",
}


PLAN_REASON_RANK = {
    "activation": 3,
    "upgrade": 3,
    "swap": 3,
    "profile_move": 3,
    "ACT": 2,
    "FIX": 1,
}


@dataclass
class TableCleanResult:
    cleaned_df: pd.DataFrame
    summary_df: pd.DataFrame
    checks: list[TableCheck]


def utility_checks_reference() -> pd.DataFrame:
    rows = [
        {
            "check_type": "check_duplicates",
            "what_it_ensures": "Keeps one row per duplicate key according to the selected keep rule.",
        },
        {
            "check_type": "check_not_null",
            "what_it_ensures": "Requires non-null values in join-critical or required columns.",
        },
        {
            "check_type": "check_non_negative",
            "what_it_ensures": "Requires numeric values to be non-negative or strictly positive.",
        },
        {
            "check_type": "check_valid_window",
            "what_it_ensures": "Requires temporal windows to end after they begin, with optional open-ended rows.",
        },
        {
            "check_type": "check_comparison",
            "what_it_ensures": "Requires one column to satisfy a comparison against another column.",
        },
        {
            "check_type": "check_custom",
            "what_it_ensures": "Allows table-specific business rules not covered by the generic checks.",
        },
    ]
    return pd.DataFrame(rows)


def usage_table_checks() -> list[TableCheck]:
    return [
        check_duplicates(
            subset=["sid"],
            keep="first",
            name="keep_latest_sid_after_sort",
        ),
        check_not_null(columns=["evt_dttm"], name="require_evt_dttm"),
        check_non_negative(column="mb", allow_zero=True, name="require_non_negative_mb"),
        check_comparison(
            left_col="evt_dttm",
            right_col="ld_dttm",
            op="<=",
            name="require_evt_dttm_lte_ld_dttm",
        ),
    ]


def profile_installation_table_checks() -> list[TableCheck]:
    return [
        check_not_null(columns=["pid", "asset_id", "beg_dttm"], name="require_profile_join_keys"),
        check_valid_window(
            start_col="beg_dttm",
            end_col="end_dttm",
            allow_open_end=True,
            name="require_valid_profile_window",
        ),
    ]


def sim_card_plan_history_table_checks() -> list[TableCheck]:
    return [
        check_not_null(columns=["asset_id", "bundle_id", "eff_dttm"], name="require_plan_join_keys"),
        check_valid_window(
            start_col="eff_dttm",
            end_col="x_dttm",
            allow_open_end=True,
            name="require_valid_plan_window",
        ),
    ]


def rate_card_table_checks() -> list[TableCheck]:
    return [
        check_not_null(
            columns=["bundle_id", "cc1", "cc2", "beg_dttm", "rt_amt", "curr_cd"],
            name="require_rate_join_keys",
        ),
        check_custom(
            name="require_usd_currency",
            description="Require curr_cd to equal USD.",
            valid_mask_fn=lambda df: df["curr_cd"].eq("USD"),
        ),
        check_non_negative(
            column="rt_amt",
            allow_zero=True,
            name="require_non_negative_rt_amt",
        ),
        check_valid_window(
            start_col="beg_dttm",
            end_col="end_dttm",
            allow_open_end=True,
            name="require_valid_rate_window",
        ),
    ]


def clean_usage_table(df: pd.DataFrame) -> TableCleanResult:
    checks = usage_table_checks()
    result = clean_dataframe(
        df,
        sort_by=["sid", "ld_dttm"],
        ascending=[True, False],
        checks=checks,
    )
    cleaned_df = result.cleaned_df.copy()
    cleaned_df["tech_clean"] = cleaned_df["tech"].map(TECH_MAP)
    cleaned_df["usage_date"] = cleaned_df["evt_dttm"].dt.floor("D")
    return TableCleanResult(cleaned_df=cleaned_df, summary_df=result.summary_df, checks=checks)


def clean_profile_installation_table(df: pd.DataFrame) -> TableCleanResult:
    checks = profile_installation_table_checks()
    result = clean_dataframe(df, checks=checks)
    return TableCleanResult(
        cleaned_df=result.cleaned_df,
        summary_df=result.summary_df,
        checks=checks,
    )


def clean_sim_card_plan_history_table(df: pd.DataFrame) -> TableCleanResult:
    checks = sim_card_plan_history_table_checks()
    result = clean_dataframe(df, checks=checks)
    cleaned_df = result.cleaned_df.copy()
    cleaned_df["why_rank"] = cleaned_df["why_cd"].map(PLAN_REASON_RANK).fillna(0)
    return TableCleanResult(
        cleaned_df=cleaned_df,
        summary_df=result.summary_df,
        checks=checks,
    )


def clean_rate_card_table(df: pd.DataFrame) -> TableCleanResult:
    checks = rate_card_table_checks()
    result = clean_dataframe(df, checks=checks)
    return TableCleanResult(
        cleaned_df=result.cleaned_df,
        summary_df=result.summary_df,
        checks=checks,
    )
