-- models/traffic_daily_agg.sql
{{ config(
    materialized='incremental',
    schema='public',
    unique_key='id'
) }}

WITH filtered_events AS (
    SELECT
        DATE(collector_tstamp) AS event_date,
        app_id,
        domain_userid,
        refr_medium,
        mkt_source,
        refr_source,
        mkt_network,
        mkt_campaign,
        mkt_term
    FROM {{ source('atomic', 'events') }}
    WHERE event = 'page_view'
      AND refr_medium != 'internal'
      AND useragent NOT ILIKE '%bot%'
      AND useragent NOT ILIKE '%spider%'
      AND useragent NOT ILIKE '%crawl%'
      {% if is_incremental() %}
        AND DATE(collector_tstamp) > (SELECT COALESCE(MAX(spend_date), '1900-01-01') FROM {{ this }})
      {% endif %}
),

classified_events AS (
    SELECT
        event_date,
        app_id,
        domain_userid,
        CASE
            WHEN (refr_medium IN ('cpc', 'ppc', 'paidsearch', 'display', 'social', 'search', 'email', '', 'unknown') AND NVL(mkt_network, '') <> '')
                 OR (refr_medium = 'paid') THEN 'paid'
            WHEN refr_medium IN ('display', 'social', 'search', 'email', '', 'unknown')
                 AND refr_medium <> 'paid'
                 AND NVL(mkt_network, '') = '' THEN 'organic'
            ELSE NULL
        END AS traffic_type,
        mkt_source,
        refr_source,
        mkt_network,
        mkt_campaign,
        mkt_term
    FROM filtered_events
),

event_traffic AS (
    SELECT
        event_date,
        app_id,
        traffic_type,
        CASE
            WHEN traffic_type = 'paid' THEN NVL(mkt_source, refr_source, mkt_network, 'unknown')
            WHEN traffic_type = 'organic' THEN NVL(mkt_source, refr_source, 'unknown')
            ELSE NULL
        END AS col_3,
        CASE
            WHEN traffic_type = 'paid' THEN NVL(mkt_campaign, 'unknown')
            WHEN traffic_type = 'organic' THEN NVL(mkt_term, mkt_campaign, 'unknown')
            ELSE NULL
        END AS col_4,
        COUNT(*) AS total_visits,
        COUNT(DISTINCT domain_userid) AS unique_visitors
    FROM classified_events
    WHERE traffic_type IS NOT NULL
    GROUP BY
        event_date,
        app_id,
        traffic_type,
        col_3,
        col_4
),

campaign_spend_data AS (
    SELECT
        cs.company_id,
        cs.company_domain,
        DATE(cs.spend_date) AS spend_date,
        cs.campaign_name,
        cs.spend
    FROM {{ source('public', 'campaign_spends') }} cs
    WHERE cs.spend IS NOT NULL
    {% if is_incremental() %}
        AND DATE(cs.spend_date) > (SELECT COALESCE(MAX(spend_date), '1900-01-01') FROM {{ this }})
    {% endif %}
),

combined_traffic AS (
    SELECT
        999 AS company_id,
        et.app_id AS company_domain,
        et.event_date AS spend_date,
        et.total_visits AS traffic,
        NVL(csd.spend, 0) AS spend,
        'all' AS col_1,
        et.traffic_type AS col_2,
        et.col_3,
        et.col_4,
        NULL AS col_5,
        NULL AS col_6
    FROM event_traffic et
    LEFT JOIN campaign_spend_data csd
        ON et.col_4 = csd.campaign_name
        AND et.event_date = csd.spend_date
        AND et.traffic_type = 'paid'

    UNION ALL

    SELECT
        999 AS company_id,
        et.app_id AS company_domain,
        et.event_date AS spend_date,
        et.unique_visitors AS traffic,
        NVL(csd.spend, 0) AS spend,
        'unique' AS col_1,
        et.traffic_type AS col_2,
        et.col_3,
        et.col_4,
        NULL AS col_5,
        NULL AS col_6
    FROM event_traffic et
    LEFT JOIN campaign_spend_data csd
        ON et.col_4 = csd.campaign_name
        AND et.event_date = csd.spend_date
        AND et.traffic_type = 'paid'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['company_domain', 'spend_date', 'col_1', 'col_2', 'col_3', 'col_4']) }} AS id,
    company_id,
    company_domain,
    spend_date,
    traffic,
    spend,
    col_1,
    col_2,
    col_3,
    col_4,
    col_5,
    col_6
FROM combined_traffic
