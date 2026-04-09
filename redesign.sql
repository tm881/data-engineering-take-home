-- Proposed redesign for a cleaner telecom usage model.
-- The main idea is to keep raw ingestion immutable, then publish curated dimensions/facts
-- with explicit keys, valid-time constraints, and deterministic rating rules.

create table dim_profile (
    profile_id bigint primary key,
    profile_external_id bigint not null unique,
    created_at timestamp not null default current_timestamp
);

create table dim_sim_card (
    sim_card_id bigint primary key,
    asset_external_id bigint not null unique,
    created_at timestamp not null default current_timestamp
);

create table bridge_profile_sim_card_scd (
    bridge_id bigint primary key,
    profile_id bigint not null references dim_profile(profile_id),
    sim_card_id bigint not null references dim_sim_card(sim_card_id),
    valid_from timestamp not null,
    valid_to timestamp,
    source_system varchar(50) not null,
    created_at timestamp not null,
    check (valid_to is null or valid_to > valid_from),
    unique (profile_id, valid_from)
);

create index idx_profile_sim_card_lookup
    on bridge_profile_sim_card_scd (profile_id, valid_from, valid_to);

create table dim_bundle (
    bundle_id bigint primary key,
    bundle_external_id bigint not null unique,
    created_at timestamp not null default current_timestamp
);

create table bridge_sim_card_bundle_scd (
    bridge_id bigint primary key,
    sim_card_id bigint not null references dim_sim_card(sim_card_id),
    bundle_id bigint not null references dim_bundle(bundle_id),
    valid_from timestamp not null,
    valid_to timestamp,
    reason_code varchar(50) not null,
    updated_at timestamp not null,
    check (valid_to is null or valid_to > valid_from),
    unique (sim_card_id, valid_from)
);

create index idx_sim_card_bundle_lookup
    on bridge_sim_card_bundle_scd (sim_card_id, valid_from, valid_to);

create table dim_country_network (
    network_id bigint primary key,
    mcc integer not null,
    mnc integer not null,
    country_code varchar(3),
    operator_name varchar(100),
    unique (mcc, mnc)
);

create table dim_radio_technology (
    tech_id smallint primary key,
    tech_code varchar(10) not null unique,
    tech_generation varchar(5) not null,
    check (tech_generation in ('2G', '3G', '4G', '5G'))
);

create table dim_rate_plan (
    rate_plan_id bigint primary key,
    bundle_id bigint not null references dim_bundle(bundle_id),
    network_id bigint not null references dim_country_network(network_id),
    tech_id smallint references dim_radio_technology(tech_id),
    valid_from date not null,
    valid_to date,
    rate_per_mb numeric(12,6) not null,
    currency_code char(3) not null,
    priority_number integer not null default 100,
    is_fallback boolean not null default false,
    check (rate_per_mb >= 0),
    check (valid_to is null or valid_to > valid_from),
    check (currency_code ~ '^[A-Z]{3}$')
);

create unique index uq_rate_plan_exact
    on dim_rate_plan (bundle_id, network_id, tech_id, valid_from, priority_number);

create index idx_rate_plan_lookup
    on dim_rate_plan (bundle_id, network_id, valid_from, valid_to, priority_number);

create table fact_usage_event (
    usage_event_id bigint primary key,
    source_event_id bigint not null,
    profile_id bigint references dim_profile(profile_id),
    sim_card_id bigint references dim_sim_card(sim_card_id),
    bundle_id bigint references dim_bundle(bundle_id),
    network_id bigint references dim_country_network(network_id),
    tech_id smallint references dim_radio_technology(tech_id),
    evt_dttm timestamp not null,
    load_dttm timestamp not null,
    usage_mb numeric(18,6) not null,
    apn_name varchar(100),
    source_name varchar(200) not null,
    rating_status varchar(20) not null,
    rate_plan_id bigint references dim_rate_plan(rate_plan_id),
    cost_usd numeric(18,6),
    is_current_record boolean not null default true,
    check (usage_mb > 0),
    check (load_dttm >= evt_dttm),
    unique (source_event_id, is_current_record)
);

create index idx_usage_event_reporting
    on fact_usage_event (evt_dttm, sim_card_id, bundle_id, network_id, tech_id);

create index idx_usage_event_source
    on fact_usage_event (source_event_id, load_dttm desc);

-- Risks and tradeoffs:
-- 1. SCD bridges improve auditability and point-in-time correctness, but make joins more complex.
-- 2. Normalized dimensions reduce duplication, but require a stronger ingestion pipeline and reference data management.
-- 3. Enforcing uniqueness may reject bad source rows that are currently tolerated; raw landing tables should absorb those first.
-- 4. Storing rated facts improves reporting speed, but requires re-rating logic when historical rate cards change.
