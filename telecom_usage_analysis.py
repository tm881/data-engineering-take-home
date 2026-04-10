from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.analysis_helpers import (
    build_linked_usage,
    build_usage_with_sim,
    clean_plans,
    clean_profiles,
    clean_rates,
    clean_usage,
    load_frames,
    render_chart,
)


ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = ROOT / "outputs"


def build_daily_usage(usage_clean: pd.DataFrame) -> pd.DataFrame:
    return (
        usage_clean.groupby("usage_date", as_index=False)["mb"]
        .sum()
        .rename(columns={"mb": "total_usage_mb"})
        .sort_values("usage_date")
    )


def build_sim_usage(usage_clean: pd.DataFrame, profile_installation_clean: pd.DataFrame) -> pd.DataFrame:
    usage_with_sim = build_usage_with_sim(usage_clean, profile_installation_clean)
    return (
        usage_with_sim.groupby("sim_card_id", dropna=True)["mb"]
        .sum()
        .reset_index()
        .rename(columns={"mb": "total_usage_mb"})
        .sort_values("total_usage_mb", ascending=False)
    )


def main() -> None:
    OUTPUTS_DIR.mkdir(exist_ok=True)

    usage_events, profile_installation, sim_card_plan_history, rate_card = load_frames()

    usage_clean, duplicate_usage_event_count = clean_usage(usage_events)
    profile_installation_clean = clean_profiles(profile_installation)
    sim_card_plan_history_clean = clean_plans(sim_card_plan_history)
    rate_card_clean = clean_rates(rate_card)

    # 1. Create a simple line chart showing Total Usage (MB) per day.
    daily_usage = build_daily_usage(usage_clean)
    daily_usage.to_csv(OUTPUTS_DIR / "daily_usage_mb.csv", index=False)
    render_chart(daily_usage)

    # 2. Which sim_card_id had the highest total usage?
    sim_usage = build_sim_usage(usage_clean, profile_installation_clean)
    top_sim_card = sim_usage.iloc[0]

    # 3. How many usage events resolved to 3G after any cleanup is finished?
    three_g_event_count = int((usage_clean["tech_clean"] == "3G").sum())

    # 4. How many duplicate usage events did you identify?
    duplicate_count = int(duplicate_usage_event_count)

    # 5. What is the cost of all data used in the linked data?
    linked_usage = build_linked_usage(
        usage_clean,
        profile_installation_clean,
        sim_card_plan_history_clean,
        rate_card_clean,
    )
    linked_usage.to_csv(OUTPUTS_DIR / "linked_usage_events.csv", index=False)
    total_linked_cost_usd = float(linked_usage["cost_usd"].fillna(0).sum())

    print("Hologram Data Engineering Take-Home")
    print("")
    print(f"Top sim_card_id by total usage: {int(top_sim_card['sim_card_id'])} ({float(top_sim_card['total_usage_mb']):.1f} MB)")
    print(f"Usage events resolved to 3G after cleanup: {three_g_event_count}")
    print(f"Duplicate usage events identified: {duplicate_count}")
    print(f"Total cost of linked data: ${total_linked_cost_usd:.3f} USD")


if __name__ == "__main__":
    main()
