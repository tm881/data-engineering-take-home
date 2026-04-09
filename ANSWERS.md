# Hologram Data Engineering Take-Home

## Answers

- `sim_card_id` with the highest total usage: `1001` (`160.0` MB)
- Usage events that resolved to 3G after cleanup: `2`
- Duplicate usage events identified: `2`
- Total cost of all linked data usage: `$11.138` USD

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

- Raw usage rows: `30`
- Rows after `sid` dedupe: `28`
- Rows removed for null event timestamp: `1`
- Rows removed for non-positive usage: `1`
- Rows removed for impossible event/load ordering: `1`
- Linked rows with cost: `19`
- Linked rows without cost: `6`
