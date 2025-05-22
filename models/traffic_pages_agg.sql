{{ config(
    materialized='incremental',
    schema='public',
    unique_key='id'
) }}

WITH filtered_events AS (
    SELECT
        collector_tstamp::DATE AS event_date,
        app_id,
        domain_userid,
        page_urlpath
    FROM {{ source('atomic', 'events') }}
    WHERE event = 'page_view'
      AND refr_medium != 'internal'
      AND useragent NOT ILIKE '%bot%'
      AND useragent NOT ILIKE '%spider%'
      AND useragent NOT ILIKE '%crawl%'
      AND page_urlpath IS NOT NULL
      {% if is_incremental() %}
        AND collector_tstamp::DATE > (
            SELECT COALESCE(MAX(date), DATE '1900-01-01') FROM {{ this }}
        )
      {% endif %}
),

page_traffic AS (
    SELECT
        event_date AS date,
        app_id AS company_domain,
        page_urlpath,
        COUNT(*) AS total_visits,
        COUNT(DISTINCT domain_userid) AS unique_visits
    FROM filtered_events
    GROUP BY 1, 2, 3
),

labeled_visits AS (
    SELECT
        company_domain,
        date,
        page_urlpath,
        'all' AS type,
        total_visits AS traffic
    FROM page_traffic

    UNION ALL

    SELECT
        company_domain,
        date,
        page_urlpath,
        'unique' AS type,
        unique_visits AS traffic
    FROM page_traffic
),

final_rows AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY date, company_domain, page_urlpath, type) AS id,
        1 AS company_id,
        company_domain,
        date,
        type,
        page_urlpath,
        traffic
    FROM labeled_visits
)

SELECT
    id,
    date,
    type,
    page_urlpath,
    traffic,
    company_id,
    company_domain
FROM final_rows
