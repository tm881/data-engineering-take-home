-- Proposed redesign for the telecom usage data model.
-- Goal:
-- 1. Preserve raw source data exactly as received so ingestion is auditable.
-- 2. Create a curated layer that is easier to query and safer to trust.
-- 3. Make time-valid relationships explicit because the business logic depends on
--    "what was active at the event timestamp" for SIM assignment, plan assignment, and rating.
-- 4. Store enough resolution detail to explain why an event priced successfully or failed.

-- Raw layer ---------------------------------------------------------------
-- Keep source fidelity here. The raw layer is intentionally permissive:
-- it is meant to capture source records as-is, including bad or conflicting rows.
-- This lets us retain lineage, replay ingest, and audit source quality later.

create table raw_usage_event (
    -- Surrogate key for warehouse management. We do not rely on the source `sid`
    -- because duplicate or corrected versions of the same event can arrive over time.
    raw_usage_event_id bigint generated always as identity primary key,
    source_name varchar(200) not null,
    source_load_dttm timestamp not null,
    sid bigint not null,
    pid bigint,
    evt_dttm timestamp,
    mb numeric(18,6),
    cc1 integer,
    cc2 integer,
    tech varchar(50),
    apn_nm varchar(100),
    record_hash varchar(128),
    ingested_at timestamp not null default current_timestamp
);

-- Support source-level deduplication / latest-version lookup by `sid`.
create index idx_raw_usage_sid on raw_usage_event (sid, source_load_dttm desc);

create table raw_profile_installation (
    -- Raw profile-to-asset assignments are retained even if the source windows overlap
    -- or the end timestamp is invalid. Those issues get handled in the curated layer.
    raw_profile_installation_id bigint generated always as identity primary key,
    pid bigint,
    asset_id bigint,
    beg_dttm timestamp,
    end_dttm timestamp,
    src_cd varchar(50),
    crt_dttm timestamp,
    ingested_at timestamp not null default current_timestamp
);

create table raw_sim_card_plan_history (
    -- Raw SIM-to-plan history is also preserved as-is because source systems may send
    -- late corrections, conflicting intervals, or low-quality reason codes.
    raw_sim_card_plan_history_id bigint generated always as identity primary key,
    asset_id bigint,
    bundle_id bigint,
    eff_dttm timestamp,
    x_dttm timestamp,
    why_cd varchar(50),
    upd_dttm timestamp,
    ingested_at timestamp not null default current_timestamp
);

create table raw_rate_card (
    -- Keep all incoming rates, including non-USD or bad-rate rows, so pricing logic can be
    -- reproduced from the source feed before curated quality rules are applied.
    raw_rate_card_id bigint generated always as identity primary key,
    bundle_id bigint,
    cc1 integer,
    cc2 integer,
    tech_cd varchar(50),
    beg_dttm timestamp,
    end_dttm timestamp,
    rt_amt numeric(18,6),
    curr_cd varchar(10),
    prio_nbr integer,
    ingested_at timestamp not null default current_timestamp
);


-- Curated dimensions / bridges -------------------------------------------
-- The curated layer turns source business keys into stable dimensions plus explicit
-- time-valid bridge tables. This makes the data model easier to query, index, and explain.

create table dim_profile (
    -- `profile_nk` keeps the source/business identifier separate from the warehouse surrogate key.
    profile_id bigint generated always as identity primary key,
    profile_nk bigint not null unique
);

create table dim_sim_card (
    -- `sim_card_nk` corresponds to the source asset / SIM identifier.
    sim_card_id bigint generated always as identity primary key,
    sim_card_nk bigint not null unique
);

create table dim_bundle (
    -- Bundles are normalized into a dimension so plan history and pricing rules can reference
    -- a consistent bundle key even if additional bundle attributes are added later.
    bundle_id bigint generated always as identity primary key,
    bundle_nk bigint not null unique
);

create table dim_radio_technology (
    -- Keep both the raw code and a normalized generation.
    -- This supports reporting at the business-friendly generation level while still preserving
    -- the original source value for debugging and mapping updates.
    tech_id smallint generated always as identity primary key,
    raw_tech_cd varchar(50) not null unique,
    normalized_generation varchar(10) not null,
    check (normalized_generation in ('2G', '3G', '4G', '5G', 'UNKNOWN'))
);

create table dim_network (
    -- The source model identifies networks through (cc1, cc2). Breaking that pair into
    -- a dimension gives us a consistent join target for both usage facts and rate rules.
    network_id bigint generated always as identity primary key,
    cc1 integer not null,
    cc2 integer not null,
    unique (cc1, cc2)
);

-- Profile to SIM assignment over time.
create table bridge_profile_sim_card (
    -- This bridge exists because a profile can move between SIM cards over time.
    -- The time window is part of the business key for point-in-time resolution.
    bridge_profile_sim_card_id bigint generated always as identity primary key,
    profile_id bigint not null references dim_profile(profile_id),
    sim_card_id bigint not null references dim_sim_card(sim_card_id),
    valid_from timestamp not null,
    valid_to timestamp,
    src_cd varchar(50),
    created_at timestamp not null,
    check (valid_to is null or valid_to > valid_from),
    unique (profile_id, sim_card_id, valid_from)
);

-- This index supports "find the SIM active for a profile at event time".
create index idx_bridge_profile_sim_lookup
    on bridge_profile_sim_card (profile_id, valid_from, valid_to);

-- SIM to bundle / plan assignment over time.
create table bridge_sim_card_bundle (
    -- A SIM card can move between bundles over time, so this relationship also needs
    -- validity windows. `reason_rank` gives the pricing logic a deterministic tie-breaker
    -- when the source sends overlapping or competing plan records.
    bridge_sim_card_bundle_id bigint generated always as identity primary key,
    sim_card_id bigint not null references dim_sim_card(sim_card_id),
    bundle_id bigint not null references dim_bundle(bundle_id),
    valid_from timestamp not null,
    valid_to timestamp,
    reason_cd varchar(50),
    updated_at timestamp not null,
    reason_rank integer not null default 0,
    check (valid_to is null or valid_to > valid_from),
    unique (sim_card_id, bundle_id, valid_from)
);

-- This index supports "find the bundle active for a SIM at event time",
-- with secondary sort fields available for overlap resolution.
create index idx_bridge_sim_bundle_lookup
    on bridge_sim_card_bundle (sim_card_id, valid_from, valid_to, reason_rank, updated_at);

-- Rate rules by bundle, network, technology, and date.
create table fact_rate_rule (
    -- Rates are modeled as a fact-like table because they are time-bound business rules,
    -- not static attributes. The pricing process must find the rule that was effective
    -- for a given bundle/network/technology at event time.
    rate_rule_id bigint generated always as identity primary key,
    bundle_id bigint not null references dim_bundle(bundle_id),
    network_id bigint not null references dim_network(network_id),
    tech_id smallint references dim_radio_technology(tech_id),
    valid_from timestamp not null,
    valid_to timestamp,
    rate_per_mb_usd numeric(18,6) not null,
    priority_nbr integer not null default 100,
    -- `is_fallback` distinguishes exact technology matches from rules intended to serve
    -- as a controlled fallback when an exact tech-specific rule does not exist.
    is_fallback boolean not null default false,
    check (rate_per_mb_usd >= 0),
    check (valid_to is null or valid_to > valid_from)
);

-- Prevent duplicate exact rules at the same effective timestamp and priority.
create unique index uq_rate_rule_exact
    on fact_rate_rule (bundle_id, network_id, tech_id, valid_from, priority_nbr);

-- This index supports the pricing lookup path from bundle/network/date to the best rule.
create index idx_rate_rule_lookup
    on fact_rate_rule (bundle_id, network_id, valid_from, valid_to, priority_nbr, tech_id);


-- Curated usage fact ------------------------------------------------------
-- This fact stores the cleaned, latest version of each usage event plus the resolved
-- SIM, bundle, network, technology, and pricing outcome used for analytics.

create table fact_usage_event (
    usage_event_id bigint generated always as identity primary key,
    -- Keep source `sid` because analysts and operators will still need the original event id.
    sid bigint not null,
    -- Use curated surrogate keys here so downstream facts consistently point to dimensions.
    profile_id bigint references dim_profile(profile_id),
    sim_card_id bigint references dim_sim_card(sim_card_id),
    bundle_id bigint references dim_bundle(bundle_id),
    network_id bigint references dim_network(network_id),
    tech_id smallint references dim_radio_technology(tech_id),
    evt_dttm timestamp not null,
    ld_dttm timestamp not null,
    mb numeric(18,6) not null,
    apn_nm varchar(100),
    source_name varchar(200) not null,
    -- We keep non-latest versions in the fact when needed for audit, but enforce that only one
    -- row per `sid` can be marked as the current/latest version used for reporting.
    is_latest_sid_version boolean not null default true,
    -- Resolution status fields are kept directly on the fact because they make pipeline outcomes
    -- transparent. That is useful for monitoring quality and for explaining failed pricing.
    profile_resolution_status varchar(50) not null,
    plan_resolution_status varchar(50) not null,
    rate_resolution_status varchar(50) not null,
    rate_rule_id bigint references fact_rate_rule(rate_rule_id),
    cost_usd numeric(18,6),
    check (mb >= 0),
    check (ld_dttm >= evt_dttm)
);

-- Reporting should only ever see one "current" row for a given `sid`.
create unique index uq_fact_usage_latest_sid
    on fact_usage_event (sid)
    where is_latest_sid_version = true;

-- Main reporting index for time-series usage, SIM-level usage, and priced usage analysis.
create index idx_fact_usage_reporting
    on fact_usage_event (evt_dttm, sim_card_id, bundle_id, network_id, tech_id);

-- Quality/debugging index for investigating why linkage or pricing failed.
create index idx_fact_usage_resolution
    on fact_usage_event (profile_resolution_status, plan_resolution_status, rate_resolution_status);


-- Design notes / tradeoffs -----------------------------------------------
-- 1. The raw layer preserves bad source records so ingestion stays lossless and auditable.
-- 2. The curated layer enforces validity rules needed for reliable analysis and pricing.
-- 3. Temporal bridge tables make point-in-time joins explicit, but add complexity to query logic.
-- 4. Storing both resolution status and cost on fact_usage_event makes reporting easy and debugging possible.
-- 5. Additional no-overlap constraints would be ideal for temporal bridges and rate rules,
--    but implementation depends on warehouse support. In systems that support exclusion constraints
--    or interval uniqueness, those should be added.
-- 6. The curated layer intentionally separates business keys (`*_nk`) from warehouse surrogate keys
--    so the model can evolve without rewriting downstream joins when source identifiers change.
