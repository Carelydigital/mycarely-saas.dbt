-- my_dbt_project/models/lead_activities_agg.sql
{{ config(
    materialized='incremental',
    schema='public',
    unique_key='id'
) }}

-- Log input and event counts for debugging
{% if execute %}
    {% set lead_count_query %}
SELECT COUNT(*) AS lead_count
FROM {{ source('public', 'lead_stage_change_events') }}
    {% if is_incremental() %}
WHERE date::date > (SELECT COALESCE(MAX(activity_date), '1900-01-01') FROM {{ this }})
    {% endif %}
    {% endset %}
    {% set lead_count_result = run_query(lead_count_query) %}
    {% do log('Lead activities input count: ' ~ lead_count_result[0]['lead_count'], info=True) %}

    {% set event_count_query %}
SELECT COUNT(DISTINCT domain_userid) AS event_user_count
FROM {{ source('atomic', 'events') }}
WHERE event = 'page_view'
  AND useragent NOT ILIKE '%bot%'
          AND useragent NOT ILIKE '%spider%'
          AND useragent NOT ILIKE '%crawl%'
          AND refr_medium != 'internal'
    {% endset %}
    {% set event_count_result = run_query(event_count_query) %}
    {% do log('Unique domain_userid in events: ' ~ event_count_result[0]['event_user_count'], info=True) %}
    {% endif %}

WITH filtered_events AS (
    SELECT
    domain_userid,
    event_id,
    mkt_medium,
    mkt_source,
    mkt_campaign,
    collector_tstamp::date AS visit_date,
    page_urlpath,
    refr_medium,
    mkt_network,
    collector_tstamp,
    ROW_NUMBER() OVER (
    PARTITION BY domain_userid
    ORDER BY
    CASE WHEN (refr_medium IN ('cpc', 'ppc', 'paidsearch', 'display', 'social', 'search', 'email', '', 'unknown') AND COALESCE(mkt_network, '') != '')
    OR (refr_medium = 'paid') THEN 1 ELSE 2 END,
    collector_tstamp DESC,
    event_id::text DESC  -- Robust tiebreaker
    ) AS rn
    FROM {{ source('atomic', 'events') }}
    WHERE event = 'page_view'
    AND useragent NOT ILIKE '%bot%'
    AND useragent NOT ILIKE '%spider%'
    AND useragent NOT ILIKE '%crawl%'
    AND refr_medium != 'internal'
    ),
    latest_events AS (
    SELECT
    domain_userid,
    event_id,
    mkt_medium,
    mkt_source,
    mkt_campaign,
    visit_date,
    page_urlpath
    FROM filtered_events
    WHERE rn = 1
    ),
    lead_activities AS (
    SELECT DISTINCT
    company_domain,
    company_id,
    domain_userid,
    lead_id,
    person_id,
    date::date AS activity_date,
    lead_generator_id,
    lead_generator_name,
    lead_source_ehr_id,
    new_stage_id,
    new_stage_name,
    old_stage_id,
    old_stage_name,
    pipeline_id,
    pipeline_name,
    product_id,
    product_name,
    product_price,
    product_sku
    FROM {{ source('public', 'lead_stage_change_events') }}
    {% if is_incremental() %}
    WHERE date::date > (SELECT COALESCE(MAX(activity_date), '1900-01-01') FROM {{ this }})
    {% endif %}
    ),
    traffic_spend AS (
    SELECT DISTINCT
    spend_date,
    LOWER(col_4) AS mkt_campaign,
    spend::float / NULLIF(traffic, 0) AS spend_per_visit,
    col_1
    FROM {{ source('public_public', 'traffic_daily_agg') }}
    WHERE spend IS NOT NULL
    AND traffic > 0
    AND col_4 IS NOT NULL
    AND col_1 = 'unique'
    ),
    enriched_activities AS (
    SELECT
    la.company_id,
    LOWER(la.company_domain) AS company_name,
    LOWER(la.domain_userid) AS domain_userid,
    le.event_id,
    LOWER(le.mkt_medium) AS mkt_medium,
    LOWER(le.mkt_source) AS mkt_source,
    LOWER(le.mkt_campaign) AS mkt_campaign,
    le.visit_date,
    LOWER(le.page_urlpath) AS page_urlpath,
    COALESCE(ts.spend_per_visit, 0) AS mkt_spend,
    la.activity_date,
    la.lead_source_ehr_id,
    la.lead_generator_id,
    LOWER(la.lead_generator_name) AS lead_generator_name,
    la.old_stage_id,
    LOWER(la.old_stage_name) AS old_stage_name,
    la.new_stage_id,
    LOWER(la.new_stage_name) AS new_stage_name,
    la.pipeline_id,
    LOWER(la.pipeline_name) AS pipeline_name,
    la.product_id,
    LOWER(la.product_sku) AS product_sku,
    LOWER(la.product_name) AS product_name,
    LOWER(CAST(la.product_price AS TEXT)) AS product_price
    FROM lead_activities la
    LEFT JOIN latest_events le
    ON la.domain_userid = le.domain_userid
    LEFT JOIN traffic_spend ts
    ON LOWER(le.mkt_campaign) = ts.mkt_campaign
    AND le.visit_date = ts.spend_date
    AND ts.col_1 = 'unique'
    )
SELECT
    nextval('public_public.lead_activities_agg_id_seq') AS id,
    company_id,
    company_name,
    domain_userid,
    event_id,
    mkt_medium,
    mkt_source,
    mkt_campaign,
    visit_date,
    page_urlpath,
    mkt_spend,
    activity_date,
    lead_source_ehr_id,
    lead_generator_id,
    lead_generator_name,
    old_stage_id,
    old_stage_name,
    new_stage_id,
    new_stage_name,
    pipeline_id,
    pipeline_name,
    product_id,
    product_sku,
    product_name,
    product_price
FROM enriched_activities
WHERE new_stage_id IS NOT NULL
  AND new_stage_name IS NOT NULL
  AND pipeline_id IS NOT NULL
  AND pipeline_name IS NOT NULL