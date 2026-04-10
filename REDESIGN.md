# Data Model Redesign

## 1. High-Level Design Decisions

I approached the redesign by starting with the core failure modes in the source model rather than jumping straight to new tables.

The main issues in the original structure were:

- raw source data and trusted analytical data were effectively mixed together
- time-based relationships were implied, but not modeled clearly enough for reliable point-in-time joins
- pricing logic depended on several tables, but there was no clean, explicit path from usage event to SIM to plan to rate
- bad source rows could easily leak into reporting unless every downstream query reimplemented its own cleanup logic

Because of that, the redesign makes four architecture decisions:

1. Keep a raw layer.
   Raw ingestion should be lossless and auditable. If a source system sends duplicate events, invalid windows, or bad rates, we still want to preserve what arrived.

2. Add a curated analytical layer.
   Reporting and pricing should run on cleaned, standardized tables rather than directly on raw source feeds.

3. Make time-valid relationships explicit.
   The business logic depends on resolving "what was active at the event timestamp", so temporal bridge tables are a first-class part of the model.

4. Make pricing outcomes inspectable.
   The final usage fact should not only store usage and cost, but also enough resolution detail to explain why pricing succeeded, fell back, or failed.

This results in a model that is more reliable for analytics, easier to debug, and easier to evolve over time.

## 2. Step-by-Step Thought Process

### Step 1: Preserve the source feeds exactly as received

My first decision was to avoid over-cleaning at ingestion time.

That is why the redesign starts with four raw tables:

- `raw_usage_event`
- `raw_profile_installation`
- `raw_sim_card_plan_history`
- `raw_rate_card`

Why this is better:

- preserves source lineage
- allows replay or reprocessing if cleaning logic changes
- keeps bad source rows available for audit instead of silently dropping them
- separates operational ingestion concerns from analytical modeling concerns

This is especially useful here because the take-home data already showed:

- duplicate `sid` values
- invalid temporal windows
- non-USD and negative rate rows
- inconsistent technology values

If those records were discarded immediately, we would lose visibility into source quality.

### Step 2: Convert unstable source identifiers into stable dimensions

The next problem is that source identifiers are being reused across multiple contexts:

- `pid` identifies a profile
- `asset_id` identifies a SIM card
- `bundle_id` identifies a plan/bundle
- `(cc1, cc2)` identifies a network/geography combination
- `tech` / `tech_cd` identifies radio technology

Instead of referencing those raw values everywhere, I split them into curated dimensions:

- `dim_profile`
- `dim_sim_card`
- `dim_bundle`
- `dim_network`
- `dim_radio_technology`

Why this is better:

- gives the warehouse stable surrogate keys for joins
- keeps business keys (`*_nk`) separate from warehouse keys
- makes later enrichment easier without changing fact-table grain
- standardizes values like technology and network combinations

The `dim_radio_technology` table is especially important because the raw data uses inconsistent labels like `LTE`, `5g`, `CDMA`, `GSM`, and `HSPA+`. The redesigned model preserves the raw code while also storing a normalized generation like `3G`, `4G`, or `5G`.

### Step 3: Model profile-to-SIM assignment as a temporal bridge

Once the core dimensions exist, the next question is: how do we know which SIM was active for a usage event?

That relationship is not static. A profile can be associated with different SIM cards over time.

That is why the redesign introduces:

- `bridge_profile_sim_card`

This table stores:

- `profile_id`
- `sim_card_id`
- `valid_from`
- `valid_to`

Why this is better:

- makes the time window explicit instead of implicit
- supports correct point-in-time resolution for each usage event
- allows constraints like `valid_to > valid_from`
- creates a clean place to index profile-time lookups

This directly addresses the issue we saw in the source data where `profile_installation` had invalid or overlapping windows.

### Step 4: Model SIM-to-bundle assignment as another temporal bridge

After resolving the SIM, the next business question is which plan or bundle was active at that same time.

That is also a time-based relationship, so I modeled it as:

- `bridge_sim_card_bundle`

This table stores:

- `sim_card_id`
- `bundle_id`
- `valid_from`
- `valid_to`
- `reason_cd`
- `reason_rank`
- `updated_at`

Why this is better:

- keeps plan history separate from raw source structure
- makes bundle resolution deterministic even when source rows overlap
- preserves business tie-break context like reason code and update time
- supports a ranked resolution strategy for conflicting plan rows

The `reason_rank` field is an intentional design choice. In the take-home data, overlapping plan rows existed, so the redesigned model needs a deterministic way to decide which one wins.

### Step 5: Separate rate rules from usage events

The source model makes cost calculation harder than it needs to be because rate logic is mixed into a table that is not modeled as an explicit pricing component.

That is why the redesign introduces:

- `fact_rate_rule`

This table stores:

- `bundle_id`
- `network_id`
- `tech_id`
- `valid_from`
- `valid_to`
- `rate_per_mb_usd`
- `priority_nbr`
- `is_fallback`

Why this is better:

- pricing rules become explicit and queryable
- rates can be versioned over time
- exact matches and fallback rules are both represented clearly
- pricing logic can be tested independently from usage-volume logic

This is an important improvement because rating depends on multiple dimensions:

- active bundle
- network / geography
- technology
- event timestamp

Putting those rules into a dedicated pricing structure makes the model much easier to reason about.

### Step 6: Publish a curated usage fact with resolved business context

The final step is creating the main analytical fact:

- `fact_usage_event`

This is the table that analytics and reporting should use.

It stores:

- the cleaned event grain
- resolved profile, SIM, bundle, network, and technology keys
- event timestamp and load timestamp
- usage in MB
- the applied rate rule
- `cost_usd`
- resolution status fields

Why this is better:

- business users can report from one trusted fact table
- analysts do not need to reimplement temporal join logic in every query
- pricing results are precomputed but still inspectable
- failed matches are visible through status columns rather than disappearing silently

The resolution status fields are a key part of the design:

- `profile_resolution_status`
- `plan_resolution_status`
- `rate_resolution_status`

These make the pipeline observable. If an event could not be priced, we can tell whether the issue came from missing profile linkage, plan ambiguity, or rate mismatch.

## 3. Why This Model Is Better Than the Original

At a high level, the redesigned model is better because it separates concerns clearly.

The original structure was good enough to show the source relationships, but not strong enough to guarantee reliable analytical outcomes. The redesigned model improves that in a few ways:

- raw ingestion is preserved for audit
- curated analytics are based on cleaned and constrained tables
- temporal joins are explicit instead of hidden inside ad hoc logic
- pricing rules are modeled as first-class business rules
- the final fact table is both report-friendly and debuggable

That combination is what makes the model cleaner, more reliable, and more useful.

## 4. Keys, Constraints, and Indexing Philosophy

The redesign uses a few consistent modeling rules:

- surrogate primary keys on curated dimensions and facts
- unique business keys on the normalized source identifiers
- validity-window checks like `valid_to > valid_from`
- non-negative checks on usage and rate amounts
- filtered uniqueness so only one latest version of a given `sid` is active in reporting
- lookup indexes on temporal bridges and rate-rule access paths

Why these choices matter:

- keys make joins stable
- constraints catch bad curated data early
- indexes support the exact query patterns the model depends on
- filtered uniqueness avoids multiple "current" versions of the same usage event

## 5. Risks and Tradeoffs

This redesign is stronger, but it is not free.

Tradeoffs:

- temporal bridge tables improve correctness, but increase query and pipeline complexity
- keeping raw and curated layers improves auditability, but uses more storage
- precomputing rated usage simplifies reporting, but requires a re-rating strategy if historical rules change
- stronger curation rules improve trust, but require exception handling for rejected rows

There is also one important future enhancement:

- if the warehouse supports exclusion constraints or interval-overlap constraints, those should be added to the temporal bridges and rate rules to enforce non-overlapping active windows more strictly

## 6. Deliverables

The redesign is represented in two forms:

- diagram: [`ERD_updated.png`](/Users/taylormonticelli/Documents/company_tests/data-engineering-take-home/ERD_updated.png)
- SQL model: [`redesign.sql`](/Users/taylormonticelli/Documents/company_tests/data-engineering-take-home/redesign.sql)

The diagram is useful for explaining the architecture quickly, and the SQL is useful for showing the concrete implementation details.
