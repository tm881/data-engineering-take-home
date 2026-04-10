from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from utils.table_cleaning import (
    clean_profile_installation_table,
    clean_rate_card_table,
    clean_sim_card_plan_history_table,
    clean_usage_table,
)


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


def load_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    usage = pd.read_parquet(DATA_DIR / "usage_events.parquet")
    profiles = pd.read_parquet(DATA_DIR / "profile_installation.parquet")
    plans = pd.read_parquet(DATA_DIR / "sim_card_plan_history.parquet")
    rates = pd.read_parquet(DATA_DIR / "rate_card.parquet")
    return usage, profiles, plans, rates


def clean_usage(usage: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    duplicate_count = int(usage.duplicated(subset=["sid"], keep="last").sum())
    usage = clean_usage_table(usage).cleaned_df
    return usage, duplicate_count


def clean_profiles(profiles: pd.DataFrame) -> pd.DataFrame:
    return clean_profile_installation_table(profiles).cleaned_df


def clean_plans(plans: pd.DataFrame) -> pd.DataFrame:
    return clean_sim_card_plan_history_table(plans).cleaned_df


def clean_rates(rates: pd.DataFrame) -> pd.DataFrame:
    return clean_rate_card_table(rates).cleaned_df


def resolve_profile_asset(
    usage_row: pd.Series, profiles: pd.DataFrame
) -> tuple[float | None, str]:
    candidates = profiles[
        (profiles["pid"] == usage_row["pid"])
        & (profiles["beg_dttm"] <= usage_row["evt_dttm"])
        & (
            profiles["end_dttm"].isna()
            | (usage_row["evt_dttm"] < profiles["end_dttm"])
        )
    ].sort_values(
        ["beg_dttm", "crt_dttm", "end_dttm", "asset_id"],
        ascending=[False, False, False, False],
    )

    if candidates.empty:
        return None, "no_profile_match"

    winner = candidates.iloc[0]
    if len(candidates) > 1:
        return float(winner["asset_id"]), "overlap_resolved_latest_window"
    return float(winner["asset_id"]), "profile_match"


def resolve_bundle(
    asset_id: float | None, evt_dttm: pd.Timestamp, plans: pd.DataFrame
) -> tuple[float | None, str]:
    if asset_id is None:
        return None, "no_asset"

    candidates = plans[
        (plans["asset_id"] == int(asset_id))
        & (plans["eff_dttm"] <= evt_dttm)
        & (plans["x_dttm"].isna() | (evt_dttm < plans["x_dttm"]))
    ].sort_values(
        ["why_rank", "eff_dttm", "upd_dttm", "bundle_id"],
        ascending=[False, False, False, False],
    )

    if candidates.empty:
        return None, "no_plan_match"

    winner = candidates.iloc[0]
    if len(candidates) > 1:
        return float(winner["bundle_id"]), "plan_overlap_resolved_reason_rank"
    return float(winner["bundle_id"]), "plan_match"


def resolve_rate(usage_row: pd.Series, bundle_id: float | None, rates: pd.DataFrame) -> tuple[float | None, str]:
    if bundle_id is None:
        return None, "no_bundle"
    if pd.isna(usage_row["cc1"]):
        return None, "missing_cc1"

    candidates = rates[
        (rates["bundle_id"] == int(bundle_id))
        & (rates["beg_dttm"] <= usage_row["usage_date"])
        & (rates["end_dttm"].isna() | (usage_row["usage_date"] < rates["end_dttm"]))
        & (rates["cc1"] == int(usage_row["cc1"]))
        & (rates["cc2"] == int(usage_row["cc2"]))
    ]

    if candidates.empty:
        return None, "no_geo_rate_match"

    if pd.notna(usage_row["tech_clean"]):
        exact = candidates[candidates["tech_cd"] == usage_row["tech_clean"]].sort_values(
            ["prio_nbr", "rt_amt"], ascending=[False, True]
        )
        if not exact.empty:
            return float(exact.iloc[0]["rt_amt"]), "exact_geo_exact_tech"

    fallback = candidates[candidates["tech_cd"].isna()].sort_values(
        ["prio_nbr", "rt_amt"], ascending=[False, True]
    )
    if not fallback.empty:
        return float(fallback.iloc[0]["rt_amt"]), "exact_geo_fallback_tech"

    return None, "no_tech_rate_match"


def build_linked_usage(
    usage: pd.DataFrame, profiles: pd.DataFrame, plans: pd.DataFrame, rates: pd.DataFrame
) -> pd.DataFrame:
    linked_rows = []

    for _, row in usage.sort_values(["pid", "evt_dttm", "sid"]).iterrows():
        asset_id, profile_resolution = resolve_profile_asset(row, profiles)
        bundle_id, plan_resolution = resolve_bundle(asset_id, row["evt_dttm"], plans)
        rate, rate_resolution = resolve_rate(row, bundle_id, rates)

        linked_rows.append(
            {
                "sid": int(row["sid"]),
                "pid": int(row["pid"]),
                "asset_id": asset_id,
                "bundle_id": bundle_id,
                "evt_dttm": row["evt_dttm"],
                "usage_date": row["usage_date"],
                "mb": float(row["mb"]),
                "cc1": None if pd.isna(row["cc1"]) else int(row["cc1"]),
                "cc2": int(row["cc2"]),
                "tech_raw": row["tech"],
                "tech_clean": row["tech_clean"],
                "src_nm": row["src_nm"],
                "profile_resolution": profile_resolution,
                "plan_resolution": plan_resolution,
                "rate_resolution": rate_resolution,
                "rate_per_mb_usd": rate,
                "cost_usd": None if rate is None else float(row["mb"]) * rate,
            }
        )

    return pd.DataFrame(linked_rows)


def build_usage_with_sim(usage: pd.DataFrame, profiles: pd.DataFrame) -> pd.DataFrame:
    usage_profile_candidates = usage.merge(
        profiles,
        on="pid",
        how="left",
        suffixes=("", "_profile"),
    )

    usage_profile_candidates = usage_profile_candidates[
        (usage_profile_candidates["beg_dttm"] <= usage_profile_candidates["evt_dttm"])
        & (
            usage_profile_candidates["end_dttm"].isna()
            | (usage_profile_candidates["evt_dttm"] < usage_profile_candidates["end_dttm"])
        )
    ].copy()

    return (
        usage_profile_candidates.sort_values(
            ["sid", "beg_dttm", "crt_dttm", "end_dttm", "asset_id"],
            ascending=[True, False, False, False, False],
        )
        .drop_duplicates(subset=["sid"], keep="first")
        [["sid", "pid", "asset_id", "mb"]]
        .rename(columns={"asset_id": "sim_card_id"})
    )


def render_chart(daily_usage: pd.DataFrame) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(daily_usage["usage_date"], daily_usage["total_usage_mb"], marker="o", linewidth=2)
    ax.set_title("Total Usage (MB) Per Day")
    ax.set_xlabel("Usage Date")
    ax.set_ylabel("Total Usage (MB)")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(ROOT / "total_usage_per_day.png", dpi=200)
    plt.close(fig)
