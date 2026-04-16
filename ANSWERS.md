# Hologram Data Engineering Take-Home

## Environment Setup

Use `uv` to recreate the local environment:

```bash
brew install uv
uv sync
source .venv/bin/activate
```

For the notebook, select the registered kernel:

- `Python (.venv) data-eng-takehome`

## README Questions

### Create a simple line chart showing Total Usage (MB) per day.

- Chart file: [`total_usage_per_day.png`](total_usage_per_day.png)
- Source data: [`outputs/daily_usage_mb.csv`](outputs/daily_usage_mb.csv)
- Walkthrough: [`telecom_usage_analysis.ipynb`](telecom_usage_analysis.ipynb)

### Which `sim_card_id` had the highest total usage?

- Answer: `1001`
- Total usage: `160.0 MB`

### How many usage events resolved to 3G after any cleanup is finished?

- Answer: `2`

### How many duplicate usage events did you identify?

- Answer: `2`

### What is the cost of all data used in the linked data?

- Answer: `$11.138 USD`
- Linked output: [`outputs/linked_usage_events.csv`](outputs/linked_usage_events.csv)

### Review the provided ERD and describe how you would redesign the database to make the data cleaner, more reliable and useful.

- Updated ERD: [`ERD_updated.png`](ERD_updated.png)
- SQL model definition: [`redesign.sql`](redesign.sql)

## Quick Answers

- `sim_card_id` with the highest total usage: `1001` (`160.0` MB)
- Usage events that resolved to `3G` after cleanup: `2`
- Duplicate usage events identified: `2`
- Total cost of all data used in the linked data: `$11.138` USD

## Cleaning Approach

I used a shared cleaning layer so each raw table has a corresponding cleaned version plus a summary of applied checks. The utility supports generic checks like duplicates, null handling, non-negative numeric checks, valid time windows, and custom business rules.

Table-specific cleaned datasets:

- `usage_clean`
- `profile_installation_clean`
- `sim_card_plan_history_clean`
- `rate_card_clean`

## What Each Table Needed

- Line chart of total usage per day:
  - only `usage_events` cleanup was required
- `sim_card_id` with highest total usage:
  - `usage_clean`
  - `profile_installation_clean`
- `3G` usage event count:
  - only `usage_clean`
- duplicate usage-event count:
  - only raw `usage_events`
- total cost of linked data:
  - `usage_clean`
  - `profile_installation_clean`
  - `sim_card_plan_history_clean`
  - `rate_card_clean`

## Cleaning Summaries

### `usage_events`

- Kept the latest loaded record for duplicate `sid` values
- Required non-null `evt_dttm`
- Required `mb >= 0`
- Required `evt_dttm <= ld_dttm`

Result:

- rows before: `30`
- duplicate rows removed: `2`
- rows with null `evt_dttm` removed: `1`
- rows with negative `mb` removed: `1`
- rows with `evt_dttm > ld_dttm` removed: `1`
- rows after cleaning: `25`

### `profile_installation`

- Required non-null join keys: `pid`, `asset_id`, `beg_dttm`
- Required valid profile windows where `end_dttm` is null or later than `beg_dttm`

Result:

- rows before: `13`
- invalid window rows removed: `1`
- rows after cleaning: `12`

### `sim_card_plan_history`

- Required non-null join keys: `asset_id`, `bundle_id`, `eff_dttm`
- Required valid plan windows where `x_dttm` is null or later than `eff_dttm`

Result:

- rows before: `14`
- invalid window rows removed: `1`
- rows after cleaning: `13`

### `rate_card`

- Required non-null join keys used for rating
- Required `curr_cd = 'USD'`
- Required `rt_amt >= 0`
- Required valid rate windows where `end_dttm` is null or later than `beg_dttm`

Result:

- rows before: `16`
- non-USD rows removed: `1`
- negative-rate rows removed: `1`
- rows after cleaning: `14`

## Data Quality Issues Found

- `usage_events.sid=2` appears three times, so `sid` is not unique in the raw feed.
- `usage_events` contains one null `evt_dttm`.
- `usage_events` contains one negative `mb` value.
- `usage_events` contains one impossible event where `evt_dttm > ld_dttm`.
- `usage_events.tech` is inconsistent and required normalization into `3G`, `4G`, and `5G`.
- `profile_installation` contains an invalid reversed window and overlapping active windows.
- `sim_card_plan_history` contains an invalid reversed window and overlapping active rows.
- `rate_card` contains a non-USD currency value, duplicate business-key candidates, and a negative rate.

## Join / Rating Assumptions

- `sid` is treated as the event identifier for duplicate detection.
- When duplicate `sid` rows exist, the latest `ld_dttm` is treated as the winning version of that event.
- `0 MB` rows are allowed because zero usage can still be a valid event; negative usage is excluded.
- Time-window joins are treated as half-open intervals:
  - `start_dttm <= event_timestamp < end_dttm`
- When multiple profile rows match an event, the most recent valid profile window is selected.
- When multiple plan rows match an event, the highest-ranked / most recent valid plan row is selected.
- When multiple rate rows match, exact technology match is preferred over null-tech fallback.
- Cost is only assigned when an event can be fully linked and rated.

## Clarifying Questions

- Is `sid` the authoritative business key for a usage event, or can a later load represent a correction with materially different payload values?
- Should negative `mb` rows be treated as credits/reversals rather than excluded usage?
- Are `CDMA`, `GSM`, and `HSPA+` all intended to roll up to `3G`, or should legacy technologies be split further?
- For overlapping plan rows like `asset_id = 1002`, which source or reason code should be considered authoritative?
- Should unmatched usage rows be included in published cost reporting, or only in volume reporting?

## Redesign Summary

The redesign in [`redesign.sql`](redesign.sql) separates raw ingestion from curated analytical tables.

I also included an updated diagram version of the redesign in [`ERD_updated.png`](ERD_updated.png).

Key changes:

- keep a raw layer for all four source feeds so ingestion is lossless and auditable
- create curated dimensions for profiles, SIM cards, bundles, technologies, and networks
- model `profile -> sim_card` and `sim_card -> bundle` as temporal bridge tables with valid-from / valid-to windows
- store rate rules separately from usage facts so rating logic is explicit and maintainable
- publish a curated `fact_usage_event` table with linkage status and `cost_usd`

Keys, constraints, and indices:

- surrogate primary keys on curated dimensions and facts
- natural-key uniqueness on core business identifiers like profile, sim card, bundle, and `(cc1, cc2)` network
- window validity checks such as `valid_to > valid_from`
- non-negative numeric checks for usage and rates
- filtered uniqueness on the latest `sid` version in the curated usage fact
- lookup indexes on temporal bridge tables and rate-rule matching columns

Risks and tradeoffs:

- temporal bridge tables improve point-in-time correctness, but they make joins more complex
- keeping both raw and curated layers improves auditability, but increases storage and pipeline complexity
- stronger constraints will reject bad source rows earlier, which is good for quality but requires clear exception handling
- precomputing rated usage makes reporting easier, but requires re-rating logic if historical rate rules are corrected

## Reproduction

Notebook workflow:

- Open [`telecom_usage_analysis.ipynb`](telecom_usage_analysis.ipynb)
- Run the notebook top to bottom using the `.venv` kernel

Script workflow:

```bash
source .venv/bin/activate
python telecom_usage_analysis.py
```

Primary project files:

- [`telecom_usage_analysis.ipynb`](telecom_usage_analysis.ipynb)
- [`telecom_usage_analysis.py`](telecom_usage_analysis.py)
- [`utils/analysis_helpers.py`](utils/analysis_helpers.py)
- [`utils/dataframe_cleaner.py`](utils/dataframe_cleaner.py)
- [`utils/table_cleaning.py`](utils/table_cleaning.py)

`telecom_usage_analysis.py` is the sequential script version of the analysis, while `telecom_usage_analysis.ipynb` is the walkthrough version.

The redesign proposal is documented in [`redesign.sql`](redesign.sql) and visualized in [`ERD_updated.png`](ERD_updated.png).
