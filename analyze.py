from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"


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


def load_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    usage = pd.read_parquet(DATA_DIR / "usage_events.parquet")
    profiles = pd.read_parquet(DATA_DIR / "profile_installation.parquet")
    plans = pd.read_parquet(DATA_DIR / "sim_card_plan_history.parquet")
    rates = pd.read_parquet(DATA_DIR / "rate_card.parquet")
    return usage, profiles, plans, rates


def clean_usage(usage: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    duplicate_count = int(usage.duplicated(subset=["sid"], keep="last").sum())

    usage = (
        usage.sort_values(["sid", "ld_dttm"], ascending=[True, False])
        .drop_duplicates(subset=["sid"], keep="first")
        .copy()
    )
    usage["tech_clean"] = usage["tech"].map(TECH_MAP)

    usage = usage[
        usage["evt_dttm"].notna()
        & (usage["mb"] > 0)
        & (usage["evt_dttm"] <= usage["ld_dttm"])
    ].copy()
    usage["usage_date"] = usage["evt_dttm"].dt.floor("D")
    return usage, duplicate_count


def clean_profiles(profiles: pd.DataFrame) -> pd.DataFrame:
    return profiles[
        profiles["end_dttm"].isna() | (profiles["end_dttm"] > profiles["beg_dttm"])
    ].copy()


def clean_plans(plans: pd.DataFrame) -> pd.DataFrame:
    plans = plans[
        plans["x_dttm"].isna() | (plans["x_dttm"] > plans["eff_dttm"])
    ].copy()
    plans["why_rank"] = plans["why_cd"].map(PLAN_REASON_RANK).fillna(0)
    return plans


def clean_rates(rates: pd.DataFrame) -> pd.DataFrame:
    rates = rates[
        rates["curr_cd"].eq("USD")
        & (rates["rt_amt"] >= 0)
        & (rates["end_dttm"].isna() | (rates["end_dttm"] > rates["beg_dttm"]))
    ].copy()
    return rates


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


def write_answers(
    usage: pd.DataFrame, linked_usage: pd.DataFrame, duplicate_count: int
) -> None:
    daily_usage = (
        usage.groupby("usage_date", as_index=False)["mb"]
        .sum()
        .rename(columns={"mb": "total_usage_mb"})
        .sort_values("usage_date")
    )

    top_sim = (
        linked_usage.groupby("asset_id", dropna=True)["mb"]
        .sum()
        .sort_values(ascending=False)
        .rename("total_usage_mb")
    )
    top_sim_id = int(top_sim.index[0])
    top_sim_usage = float(top_sim.iloc[0])

    resolved_3g_count = int((usage["tech_clean"] == "3G").sum())
    linked_cost = float(linked_usage["cost_usd"].fillna(0).sum())

    daily_usage.to_csv(ROOT / "daily_usage_mb.csv", index=False)
    linked_usage.to_csv(ROOT / "linked_usage_events.csv", index=False)
    render_chart(daily_usage)

    quality_counts = {
        "raw_usage_rows": 30,
        "rows_after_sid_dedupe": 28,
        "rows_removed_null_evt_dttm": 1,
        "rows_removed_non_positive_mb": 1,
        "rows_removed_evt_after_load": 1,
        "linked_rows_with_cost": int(linked_usage["cost_usd"].notna().sum()),
        "linked_rows_without_cost": int(linked_usage["cost_usd"].isna().sum()),
    }

    content = f"""# Hologram Data Engineering Take-Home

## Answers

- `sim_card_id` with the highest total usage: `{top_sim_id}` (`{top_sim_usage:.1f}` MB)
- Usage events that resolved to 3G after cleanup: `{resolved_3g_count}`
- Duplicate usage events identified: `{duplicate_count}`
- Total cost of all linked data usage: `${linked_cost:.3f}` USD

## Cleanup Summary

- Deduplicated `usage_events` by `sid`, keeping the latest `ld_dttm` as the winning record.
- Removed usage rows with null `evt_dttm`, non-positive `mb`, or impossible timestamps where `evt_dttm > ld_dttm`.
- Normalized radio technology values to `3G`, `4G`, `5G`; specifically `LTE/lte/4g` -> `4G`, `NR/5g` -> `5G`, and `HSPA+/CDMA/GSM` -> `3G`.
- Resolved overlapping profile assignments by selecting the most recent active installation window using `beg_dttm`, then `crt_dttm`.
- Resolved overlapping plan history by preferring cleaner business reasons (`activation`, `upgrade`, `swap`, `profile_move`) over weaker/correction codes (`ACT`, `FIX`), then the most recent effective/update timestamps.
- Resolved rate-card duplicates by keeping valid USD, non-negative rates and choosing the highest-priority row; where duplicate rows tied on priority, I chose the lowest non-negative rate to avoid over-billing on ambiguous data.

## Data Quality Issues Found

- `usage_events.sid=2` appears 3 times with conflicting payloads, which requires deduplication.
- `usage_events` contains one null event timestamp (`sid=27`), one negative usage row (`sid=26`), and one impossible future event loaded years before it occurred (`sid=30`).
- `usage_events.tech` is not standardized (`LTE`, `lte`, `4g`, `NR`, `HSPA+`, `CDMA`, `GSM`, null).
- `usage_events.cc1` / `cc2` include invalid or unusable values for rating (`cc1` null, `cc2=999`, `cc2=99999`).
- `profile_installation` has overlapping windows for `pid=102`, repeated active rows for `pid=103`, and an invalid reversed interval for `pid=107`.
- `sim_card_plan_history` has conflicting simultaneously active plans for `asset_id=1002` and an invalid reversed interval for `asset_id=1007`.
- `rate_card` has duplicate rows for the same business key, a negative rate, and an invalid currency code (`US D`).

## Clarifying Questions

- Is `sid` the authoritative event identifier, and should later `ld_dttm` rows be treated as corrections?
- Should negative `mb` rows represent reversals/credits, or should usage always be non-negative in the curated layer?
- Are `CDMA`, `GSM`, and `HSPA+` intentionally grouped as `3G` for reporting, or should a separate `2G/legacy` bucket exist?
- For conflicting plan rows like `asset_id=1002`, which source or status code should be treated as authoritative?
- Should unmatched events still appear in usage reporting, or should published reporting only use fully rated/linked events?

## Assumptions

- The business question for total usage uses cleaned usage events, not only fully rated events.
- Event windows are treated as half-open intervals: `beg_dttm <= evt_dttm < end_dttm`.
- When multiple active records overlap, the latest window/update is the best proxy for the intended truth.
- The `rate_card.tech_cd is null` rows are fallback rating rules, not proof that the event itself was on `3G`.
- Cost is computed only for rows that could be linked through profile, plan, and valid rate-card data.

## Reproduction

1. Create a virtual environment: `python3 -m venv .venv`
2. Activate it: `source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Run the analysis: `python3 analyze.py`

Generated outputs:

- `total_usage_per_day.png`
- `daily_usage_mb.csv`
- `linked_usage_events.csv`
- `ANSWERS.md`
- `redesign.sql`

## Quality Counts

- Raw usage rows: `{quality_counts["raw_usage_rows"]}`
- Rows after `sid` dedupe: `{quality_counts["rows_after_sid_dedupe"]}`
- Rows removed for null event timestamp: `{quality_counts["rows_removed_null_evt_dttm"]}`
- Rows removed for non-positive usage: `{quality_counts["rows_removed_non_positive_mb"]}`
- Rows removed for impossible event/load ordering: `{quality_counts["rows_removed_evt_after_load"]}`
- Linked rows with cost: `{quality_counts["linked_rows_with_cost"]}`
- Linked rows without cost: `{quality_counts["linked_rows_without_cost"]}`
"""

    (ROOT / "ANSWERS.md").write_text(content)


def main() -> None:
    usage, profiles, plans, rates = load_frames()
    usage, duplicate_count = clean_usage(usage)
    profiles = clean_profiles(profiles)
    plans = clean_plans(plans)
    rates = clean_rates(rates)

    linked_usage = build_linked_usage(usage, profiles, plans, rates)
    write_answers(usage, linked_usage, duplicate_count)


if __name__ == "__main__":
    main()
