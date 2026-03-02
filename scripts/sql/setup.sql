-- Copyright 2026 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"agentic-audience-analytics","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';


USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS AME_AD_SALES_DEMO;

CREATE ROLE IF NOT EXISTS AME_AD_SALES_DEMO_ADMIN;

SET cur_user = (SELECT current_user());
GRANT ROLE AME_AD_SALES_DEMO_ADMIN TO USER IDENTIFIER($cur_user);

CREATE WAREHOUSE IF NOT EXISTS APP_WH 
    WITH WAREHOUSE_SIZE = 'LARGE' 
    AUTO_SUSPEND = 5
    AUTO_RESUME = TRUE;

GRANT USAGE ON WAREHOUSE APP_WH TO ROLE AME_AD_SALES_DEMO_ADMIN;

GRANT USAGE ON DATABASE AME_AD_SALES_DEMO TO ROLE AME_AD_SALES_DEMO_ADMIN;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.INGEST;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.HARMONIZED;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.GENERATE;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.ANALYSE;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.DCR_ANALYSIS;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.ACTIVATION;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.DATA_SHARING;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.APPS;

-- =============================================================================
-- TRANSFER SCHEMA OWNERSHIP TO ADMIN ROLE
-- =============================================================================

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.INGEST TO ROLE AME_AD_SALES_DEMO_ADMIN;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.HARMONIZED TO ROLE AME_AD_SALES_DEMO_ADMIN;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.GENERATE TO ROLE AME_AD_SALES_DEMO_ADMIN COPY CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE AME_AD_SALES_DEMO_ADMIN;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.DCR_ANALYSIS TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.ACTIVATION TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.DATA_SHARING TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.APPS TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

USE ROLE AME_AD_SALES_DEMO_ADMIN;

CREATE OR REPLACE STAGE AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = NONE RECORD_DELIMITER = '\n')
    DIRECTORY = (ENABLE = TRUE);

GRANT READ, WRITE ON STAGE AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS TO ROLE AME_AD_SALES_DEMO_ADMIN;




-- =============================================================================
-- LOAD PRE-GENERATED DATA FROM S3
-- =============================================================================
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA INGEST;

CREATE OR REPLACE STAGE AME_AD_SALES_DEMO.INGEST.S3_DATA
    URL = 's3://sfquickstarts/sfguide_mea_subscriber_analytics/'
    FILE_FORMAT = (
        TYPE = CSV
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        COMPRESSION = GZIP
    );

-- ---------------------------------------------------------------------------
-- 10-load-subscriber-profiles.sql
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES (
    UNIQUE_ID TEXT,
    PROFILE_ID TEXT,
    TIER TEXT,
    FULL_NAME TEXT,
    EMAIL TEXT,
    USERNAME TEXT,
    PRIMARY_MOBILE TEXT,
    IP_ADDRESS TEXT,
    SIGNUP_DATE DATE
);

COPY INTO AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES
FROM @AME_AD_SALES_DEMO.INGEST.S3_DATA/subscriber_full_profiles.csv.gz;

SELECT
    'Rows in INGEST.SUBSCRIBER_PROFILES' AS "Table created",
    COUNT(*) AS "Rows loaded"
FROM AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES;

-- ---------------------------------------------------------------------------
-- 11-mount-demographics-profiles-share.sql
-- ---------------------------------------------------------------------------
USE SCHEMA DATA_SHARING;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.DATA_SHARING.DEMOGRAPHICS_PROFILES (
    EMAIL TEXT,
    PRIMARY_MOBILE TEXT,
    IP_ADDRESS TEXT,
    FULL_NAME TEXT,
    LAD_CODE TEXT,
    AREA_NAME TEXT,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    AGE NUMBER,
    AGE_BAND TEXT,
    INCOME_LEVEL TEXT,
    EDUCATION_LEVEL TEXT,
    MARITAL_STATUS TEXT,
    FAMILY_STATUS TEXT,
    BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX FLOAT,
    BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY FLOAT,
    BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE FLOAT,
    BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST FLOAT
);

COPY INTO AME_AD_SALES_DEMO.DATA_SHARING.DEMOGRAPHICS_PROFILES
FROM @AME_AD_SALES_DEMO.INGEST.S3_DATA/subscriber_demographics_profiles.csv.gz;

SELECT
    'Rows in DATA_SHARING.DEMOGRAPHICS_PROFILES' AS "Table created",
    COUNT(*) AS "Rows loaded"
FROM AME_AD_SALES_DEMO.DATA_SHARING.DEMOGRAPHICS_PROFILES;

-- ---------------------------------------------------------------------------
-- 12-load-events.sql
-- ---------------------------------------------------------------------------
USE SCHEMA INGEST;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS (
    UNIQUE_ID TEXT,
    EVENT_ID TEXT,
    SESSION_ID TEXT,
    EVENT_TS TIMESTAMP_NTZ,
    EVENT_TYPE TEXT,
    DEVICE TEXT,
    PAGE_PATH TEXT,
    CONTENT_ID TEXT,
    CONTENT_TYPE TEXT,
    CONTENT_CATEGORY TEXT,
    ATTRIBUTES VARIANT
);

COPY INTO AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        TRY_PARSE_JSON($11)
    FROM @AME_AD_SALES_DEMO.INGEST.S3_DATA
)
PATTERN = '.*clickstream_events.*\.csv\.gz';

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.ADS_EVENTS (
    EVENT_ID TEXT,
    EVENT_TS TIMESTAMP_NTZ,
    EVENT_DATE DATE,
    CAMPAIGN_ID TEXT,
    CONTENT_CATEGORY TEXT,
    EVENT_TYPE TEXT,
    EVENT_WEIGHT FLOAT,
    UNIQUE_ID TEXT,
    DEVICE TEXT
);

COPY INTO AME_AD_SALES_DEMO.INGEST.ADS_EVENTS
FROM @AME_AD_SALES_DEMO.INGEST.S3_DATA/ads_events.csv.gz;

SELECT
    'Rows in INGEST.CLICKSTREAM_EVENTS' AS "Table created",
    COUNT(*) AS "Rows loaded"
FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS
UNION ALL
SELECT
    'Rows in INGEST.ADS_EVENTS' AS "Table created",
    COUNT(*) AS "Rows loaded"
FROM AME_AD_SALES_DEMO.INGEST.ADS_EVENTS;

INSERT INTO AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS (
  UNIQUE_ID, EVENT_TS, EVENT_TYPE, DEVICE, SESSION_ID, PAGE_PATH,
  CONTENT_ID, CONTENT_TYPE, CONTENT_CATEGORY, ATTRIBUTES
)
WITH cs AS (
  SELECT unique_id, MIN(event_ts) AS min_cs_ts
  FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS
  GROUP BY unique_id
), ad AS (
  SELECT unique_id, MIN(event_ts) AS min_ad_ts
  FROM AME_AD_SALES_DEMO.INGEST.ADS_EVENTS
  GROUP BY unique_id
), first_seen AS (
  SELECT
    sp.unique_id,
    LEAST(
      COALESCE(cs.min_cs_ts, '9999-12-31'::TIMESTAMP_NTZ),
      COALESCE(ad.min_ad_ts, '9999-12-31'::TIMESTAMP_NTZ)
    ) AS first_ts
  FROM AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES sp
  LEFT JOIN cs ON cs.unique_id = sp.unique_id
  LEFT JOIN ad ON ad.unique_id = sp.unique_id
  WHERE cs.min_cs_ts IS NOT NULL OR ad.min_ad_ts IS NOT NULL
)
SELECT
  fs.unique_id,
  fs.first_ts,
  'SIGN_UP',
  'Web',
  NULL,
  '/account/join',
  NULL,
  NULL,
  NULL,
  OBJECT_CONSTRUCT('source', 'derived_from_first_seen')
FROM first_seen fs
WHERE NOT EXISTS (
  SELECT 1
  FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS e
  WHERE e.unique_id = fs.unique_id
    AND e.event_type = 'SIGN_UP'
);

-- ---------------------------------------------------------------------------
-- 13-load-ad-campaigns-and-performance.sql
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.AD_CAMPAIGNS (
    CAMPAIGN_ID TEXT,
    ADVERTISER_NAME TEXT,
    VERTICAL TEXT,
    CONTENT_CATEGORY TEXT,
    RATE_TYPE TEXT,
    BOOKING_START DATE,
    BOOKING_END DATE,
    DAILY_IMPRESSION_CAP NUMBER,
    RATE_TYPE_DETAIL TEXT,
    BOOKED_CPM FLOAT,
    BOOKED_CTR FLOAT,
    CREATIVE_COUNT NUMBER
);

COPY INTO AME_AD_SALES_DEMO.INGEST.AD_CAMPAIGNS
FROM @AME_AD_SALES_DEMO.INGEST.S3_DATA/ad_campaigns.csv.gz;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.AD_PERFORMANCE_EVENTS (
    METRIC_DATE DATE,
    CAMPAIGN_ID TEXT,
    ADVERTISER_NAME TEXT,
    VERTICAL TEXT,
    CONTENT_CATEGORY TEXT,
    RATE_TYPE TEXT,
    IMPRESSIONS FLOAT,
    CLICKS FLOAT,
    CTR FLOAT,
    CPM FLOAT,
    SPEND FLOAT,
    CREATIVE_COUNT NUMBER,
    BOOKED_CPM FLOAT,
    BOOKED_CTR FLOAT,
    DAILY_IMPRESSION_CAP NUMBER
);

COPY INTO AME_AD_SALES_DEMO.INGEST.AD_PERFORMANCE_EVENTS
FROM @AME_AD_SALES_DEMO.INGEST.S3_DATA/ad_performance_events.csv.gz;

SELECT
    'Rows in INGEST.AD_CAMPAIGNS' AS "Table created",
    COUNT(*) AS "Rows loaded"
FROM AME_AD_SALES_DEMO.INGEST.AD_CAMPAIGNS
UNION ALL
SELECT
    'Rows in INGEST.AD_PERFORMANCE_EVENTS' AS "Table created",
    COUNT(*) AS "Rows loaded"
FROM AME_AD_SALES_DEMO.INGEST.AD_PERFORMANCE_EVENTS;

-- ---------------------------------------------------------------------------
-- Load demographics_distributions lookup (used by HARMONIZED layer)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.DEMOGRAPHICS_DISTRIBUTIONS (
    LAD_CODE TEXT,
    TOTAL_POPULATION NUMBER,
    RURAL_URBAN TEXT,
    INCOME_LEVEL TEXT,
    EDUCATION_LEVEL TEXT,
    EARLY_CHILDHOOD FLOAT,
    CHILDHOOD_ELEMENTARY_SCHOOL FLOAT,
    ADOLESCENSE_TEEN_YEARS FLOAT,
    YOUNG_ADULTS_EARLY FLOAT,
    PRIME_SETTLING_FAMILY_FORMATION FLOAT,
    ESTABLISHED_CAREER_MIDDLE_AGE FLOAT,
    PRE_RETIREMENT_EMPTY_NESTERS FLOAT,
    RETIREMENT_SENIOR_YEARS FLOAT,
    DEMO_MARITAL_SINGLE_NEVER_MARRIED FLOAT,
    DEMO_MARITAL_MARRIED_OR_CIVIL_PARTNERSHIP FLOAT,
    DEMO_MARITAL_DIVORCED_OR_SEPARATED FLOAT,
    DEMO_MARITAL_WIDOWED FLOAT,
    DEMO_FAMILY_COUPLES_NO_CHILDREN FLOAT,
    DEMO_FAMILY_COUPLES_WITH_DEPENDENT_CHILDREN FLOAT,
    DEMO_FAMILY_SINGLE_PARENT_WITH_DEPENDENT_CHILDREN FLOAT,
    DEMO_FAMILY_SINGLE_PERSON_HOUSEHOLD FLOAT,
    BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX FLOAT,
    BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY FLOAT,
    BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE FLOAT,
    BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST FLOAT
);

COPY INTO AME_AD_SALES_DEMO.INGEST.DEMOGRAPHICS_DISTRIBUTIONS
FROM @AME_AD_SALES_DEMO.INGEST.S3_DATA/demographics_distributions.csv.gz;

SELECT
    'Rows in INGEST.DEMOGRAPHICS_DISTRIBUTIONS' AS "Table created",
    COUNT(*) AS "Rows loaded"
FROM AME_AD_SALES_DEMO.INGEST.DEMOGRAPHICS_DISTRIBUTIONS;


-- ---------------------------------------------------------------------------
-- 14-enrich-subscriber-profiles.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA HARMONIZED;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED AS
WITH ingest_profiles AS (
    SELECT
        sp.unique_id,
        sp.profile_id,
        sp.full_name,
        sp.email,
        sp.username,
        sp.primary_mobile,
        sp.ip_address,
        sp.tier
    FROM AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES sp
),
demographics_ranked AS (
    SELECT
        dp.email,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(dp.email))
            ORDER BY dp.email
        ) AS email_rank,
        dp.* EXCLUDE (email)
    FROM AME_AD_SALES_DEMO.DATA_SHARING.DEMOGRAPHICS_PROFILES dp
    WHERE dp.email IS NOT NULL AND TRIM(dp.email) <> ''
),
demographics_dedup AS (
    SELECT
        d.email AS demographics_email,
        --d.unique_id AS demographic_unique_id,
        d.lad_code,
        d.area_name,
        d.latitude,
        d.longitude,
        d.age,
        d.age_band,
        d.education_level,
        d.income_level,
        d.marital_status,
        d.family_status,
        d.behavioral_digital_media_consumption_index,
        d.behavioral_fast_fashion_retail_propensity,
        d.behavioral_grocery_online_delivery_use,
        d.behavioral_financial_investment_interest,
        NULL::TIMESTAMP_NTZ AS demographics_generated_ts
    FROM demographics_ranked d
    WHERE d.email_rank = 1
),
joined AS (
    SELECT
        ip.unique_id,
        ip.profile_id,
        ip.full_name,
        ip.email,
        ip.username,
        ip.primary_mobile,
        ip.ip_address,
        ip.tier,
        NULL::TIMESTAMP_NTZ AS created_ts,
        --dd.demographic_unique_id,
        dd.lad_code,
        dd.area_name,
        dd.latitude,
        dd.longitude,
        dd.age,
        dd.age_band,
        dd.education_level,
        dd.income_level,
        dd.marital_status,
        dd.family_status,
        dd.behavioral_digital_media_consumption_index,
        dd.behavioral_fast_fashion_retail_propensity,
        dd.behavioral_grocery_online_delivery_use,
        dd.behavioral_financial_investment_interest,
        dd.demographics_generated_ts
    FROM ingest_profiles ip
    LEFT JOIN demographics_dedup dd
      ON LOWER(TRIM(ip.email)) = LOWER(TRIM(dd.demographics_email))
)
SELECT
    unique_id,
    profile_id,
    full_name,
    email,
    username,
    primary_mobile,
    ip_address,
    tier,
    created_ts,
    --demographic_unique_id,
    lad_code,
    area_name,
    latitude,
    longitude,
    age,
    age_band,
    education_level,
    income_level,
    marital_status,
    family_status,
    behavioral_digital_media_consumption_index,
    behavioral_fast_fashion_retail_propensity,
    behavioral_grocery_online_delivery_use,
    behavioral_financial_investment_interest,
    demographics_generated_ts,
    CURRENT_TIMESTAMP() AS generated_ts
FROM joined;

COMMENT ON TABLE AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED IS
    'Subscriber dimension combining identity attributes with demographic insight. Built from ingest identity feeds joined to shared demographic profiles via normalized email. Used as the conformed subscriber reference for harmonized, analyse and activation layers.';

COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.UNIQUE_ID IS
    'Deterministic subscriber identifier generated in the GENERATE layer and persisted through INGEST; primary key for joining to behavioural and ad delivery facts.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.PROFILE_ID IS
    'Human-readable profile identifier (SUB-*********) used by downstream applications and dashboards for drill-through.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.FULL_NAME IS
    'Faker-generated full name for the subscriber; retained for storytelling in demos.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.EMAIL IS
    'Primary contact email derived from synthetic identity generation; used to join to shared demographic datasets (normalized to lowercase for matching).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.USERNAME IS
    'Username alias for the subscriber created by the identity generator; useful for UI mock-ups.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.PRIMARY_MOBILE IS
    'UK formatted MSISDN for the subscriber; available for activation and churn modelling scenarios.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.IP_ADDRESS IS
    'Most recent IP address emitted by the identity generator; can support geo heuristics if desired.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.TIER IS
    'Current subscription tier (Ad-supported, Standard, Premium) inherited from the generating distributions.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.CREATED_TS IS
    'Placeholder for subscriber creation timestamp; populated as NULL in the synthetic dataset but reserved for real implementations.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.LAD_CODE IS
    'Assigned Local Authority District code representing the subscriber’s simulated home region (derived from LAD correlation distributions).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.AREA_NAME IS
    'Sample area name within the LAD chosen during demographic enrichment to provide localized storytelling.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.LATITUDE IS
    'Latitude coordinate for the selected LAD sample area, with slight noise injected to avoid clustering.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.LONGITUDE IS
    'Longitude coordinate for the selected LAD sample area, with slight noise injected to avoid clustering.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.AGE IS
    'Generated integer age selected by the LAD demographic profile UDF based on LAD age distributions.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.AGE_BAND IS
    'Life-stage label mapped from age distributions (e.g., Prime Settling/Family Formation, Retirement/Senior Years).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.EDUCATION_LEVEL IS
    'Most-likely education attainment inferred from LAD-specific demographic distributions.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.INCOME_LEVEL IS
    'Income band estimated from LAD demographics, used for regional ad rate modelling.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.MARITAL_STATUS IS
    'Marital status category sampled from LAD probability weights (e.g., Single, Married, Widowed).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.FAMILY_STATUS IS
    'Household composition indicator (e.g., No Kids, Young Family) derived from LAD distributions.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX IS
    'Gaussian-noised propensity index (0-1) indicating likelihood of heavy digital media consumption within the assigned LAD.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY IS
    'Gaussian-noised propensity index for fast-fashion interest; feeds marketing segmentation examples.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE IS
    'Gaussian-noised propensity index for online grocery usage, vital for CPG activation demos.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST IS
    'Gaussian-noised propensity index for financial investment interest; supports finance advertiser scenarios.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.DEMOGRAPHICS_GENERATED_TS IS
    'Timestamp placeholder for demographic enrichment execution; kept for consistency with production workflows.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED.GENERATED_TS IS
    'Load timestamp indicating when the harmonized subscriber profile snapshot was last regenerated.';


SELECT
    'Rows in HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED' AS "Table created",
    COUNT(*) AS "Rows generated"
FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED;



-- ---------------------------------------------------------------------------
-- 15-aggregate-ad-performance.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;

SET AD_CAMPAIGN_START_DATE = DATEADD(month, -24, CURRENT_DATE());
SET AD_CAMPAIGN_END_DATE = CURRENT_DATE();

/*
    Publish aggregated daily ad performance into HARMONIZED.
    Metrics are sourced from the INGEST layer while campaign metadata
    (personas/devices) is hydrated from the GENERATE catalogue.
*/
USE SCHEMA HARMONIZED;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG AS
WITH params AS (
    SELECT
        TO_DATE($AD_CAMPAIGN_START_DATE) AS start_date,
        TO_DATE($AD_CAMPAIGN_END_DATE) AS end_date
),
source AS (
    SELECT ape.*
    FROM AME_AD_SALES_DEMO.INGEST.AD_PERFORMANCE_EVENTS ape
    CROSS JOIN params
    WHERE ape.metric_date BETWEEN params.start_date AND params.end_date
),
base AS (
    SELECT
        src.metric_date AS report_date,
        src.campaign_id,
        src.advertiser_name,
        src.vertical,
        src.content_category,
        src.rate_type,
        SUM(src.impressions) AS impressions,
        SUM(src.clicks) AS clicks,
        ROUND(CASE WHEN SUM(src.impressions) > 0 THEN SUM(src.clicks) / SUM(src.impressions) ELSE 0 END, 6) AS ctr,
        ROUND(SUM(src.spend), 4) AS spend,
        ROUND(CASE WHEN SUM(src.impressions) > 0 THEN (SUM(src.spend) * 1000) / SUM(src.impressions) ELSE 0 END, 4) AS effective_cpm,
        MAX(src.booked_cpm) AS booked_cpm,
        MAX(src.booked_ctr) AS booked_ctr,
        MAX(src.daily_impression_cap) AS daily_impression_cap,
        MAX(src.creative_count) AS creative_count
    FROM source src
    GROUP BY
        src.metric_date,
        src.campaign_id,
        src.advertiser_name,
        src.vertical,
        src.content_category,
        src.rate_type
),
persona_signals AS (
    SELECT
        ce.unique_id,
        LOWER(
            COALESCE(
                NULLIF(ce.content_category, ''),
                NULLIF(ce.content_type, ''),
                ce.attributes:content_category::STRING,
                ce.attributes:content_type::STRING,
                ce.attributes:page_name::STRING,
                ce.event_type
            )
        ) AS content_tag,
        COUNT(*) AS event_count
    FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
    WHERE ce.unique_id IS NOT NULL
    GROUP BY ce.unique_id,
             LOWER(
                 COALESCE(
                     NULLIF(ce.content_category, ''),
                     NULLIF(ce.content_type, ''),
                     ce.attributes:content_category::STRING,
                     ce.attributes:content_type::STRING,
                     ce.attributes:page_name::STRING,
                     ce.event_type
                 )
             )
),
persona_ranked AS (
    SELECT
        ps.unique_id,
        ps.content_tag,
        ps.event_count,
        ROW_NUMBER() OVER (
            PARTITION BY ps.unique_id
            ORDER BY ps.event_count DESC, ps.content_tag
        ) AS rank_in_segment
    FROM persona_signals ps
),
persona_lookup AS (
    SELECT
        pr.unique_id,
        pr.content_tag,
        CASE
            WHEN pr.content_tag ILIKE '%sport%' THEN 'SPORTS_ENGAGED'
            WHEN pr.content_tag ILIKE '%live%' THEN 'LIVE_EVENT_FOLLOWER'
            WHEN pr.content_tag ILIKE '%kids%' OR pr.content_tag ILIKE '%family%' OR pr.content_tag ILIKE '%animation%' THEN 'FAMILY_HOUSEHOLD'
            WHEN pr.content_tag ILIKE '%documentary%' OR pr.content_tag ILIKE '%knowledge%' OR pr.content_tag ILIKE '%docu%' THEN 'KNOWLEDGE_SEEKER'
            WHEN pr.content_tag ILIKE '%lifestyle%' OR pr.content_tag ILIKE '%wellness%' THEN 'LIFESTYLE_MINDED'
            WHEN pr.content_tag ILIKE '%reality%' THEN 'REALITY_FAN'
            WHEN pr.content_tag ILIKE '%original%' OR pr.content_tag ILIKE '%drama%' THEN 'PREMIUM_DRAMA'
            ELSE 'GENERAL_STREAMER'
        END AS derived_persona
    FROM persona_ranked pr
    WHERE pr.rank_in_segment = 1
),
ads_rollup AS (
    SELECT
        ae.event_date AS report_date,
        ae.campaign_id,
        ARRAY_AGG(DISTINCT COALESCE(pl.derived_persona, 'GENERAL_STREAMER')) AS persona_list,
        ARRAY_AGG(DISTINCT COALESCE(ae.device, 'UNKNOWN')) AS device_list,
        COUNT(DISTINCT ae.unique_id) AS audience_reach
    FROM AME_AD_SALES_DEMO.INGEST.ADS_EVENTS ae
    LEFT JOIN persona_lookup pl
      ON pl.unique_id = ae.unique_id
    GROUP BY ae.event_date, ae.campaign_id
)
SELECT
    base.report_date,
    base.campaign_id,
    base.advertiser_name,
    base.vertical,
    base.content_category,
    base.rate_type,
    base.impressions,
    base.clicks,
    base.ctr,
    base.spend,
    base.effective_cpm,
    base.booked_cpm,
    base.booked_ctr,
    base.daily_impression_cap,
    base.creative_count,
    COALESCE(ar.persona_list, ARRAY_CONSTRUCT('GENERAL_STREAMER')) AS target_personas,
    COALESCE(ar.device_list, ARRAY_CONSTRUCT('UNKNOWN')) AS target_devices,
    COALESCE(ar.audience_reach, 0) AS observed_unique_subscribers,
    CURRENT_TIMESTAMP() AS generated_ts
FROM base
LEFT JOIN ads_rollup ar
  ON base.report_date = ar.report_date
 AND base.campaign_id = ar.campaign_id;

COMMENT ON TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG IS
    'Daily advertising performance fact table consolidating impressions, clicks, spend, and device/persona targeting attributes. Derived from ingest events with persona inference sourced from observed clickstream behaviour. Acts as the core fact for harmonized ad analytics.';

COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.REPORT_DATE IS
    'Date key representing the delivery day for aggregated campaign performance.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.CAMPAIGN_ID IS
    'Synthetic campaign identifier for sold ad inventory; join key to campaign dimension data.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.ADVERTISER_NAME IS
    'Advertiser label assigned during campaign generation (auto, retail, finance etc.).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.VERTICAL IS
    'Industry vertical associated with the campaign (used for segmentation and rate card summaries).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.CONTENT_CATEGORY IS
    'Editorial category the campaign targeted (e.g., Sports, Lifestyle).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.RATE_TYPE IS
    'Commercial rate type (CPM, Fixed, Hybrid) chosen during campaign synthesis.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.IMPRESSIONS IS
    'Total delivered impressions for the campaign on report_date.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.CLICKS IS
    'Total delivered clicks for the campaign on report_date.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.CTR IS
    'Click-through rate computed as clicks / impressions with guard against divide-by-zero.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.SPEND IS
    'Synthetic spend (in currency units) derived from CPM and impression delivery with controlled random variation.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.EFFECTIVE_CPM IS
    'Effective CPM calculated from spend and impressions (spend * 1000 / impressions).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.BOOKED_CPM IS
    'Booked CPM at the campaign contract level—used to compare against delivered CPM.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.BOOKED_CTR IS
    'Target CTR specified in the campaign metadata.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.DAILY_IMPRESSION_CAP IS
    'Maximum impression allowance per day according to campaign setup.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.CREATIVE_COUNT IS
    'Number of creatives associated with the campaign (used for pacing logic and reporting context).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.TARGET_PERSONAS IS
    'Array of inferred personas the campaign reached on report_date based on subscriber behaviour. Acts as behavioural targeting evidence rather than planned targeting.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.TARGET_DEVICES IS
    'Array of devices observed for the campaign on report_date; useful for device-mix analysis.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.OBSERVED_UNIQUE_SUBSCRIBERS IS
    'Distinct subscriber count reached by the campaign on report_date after deduping impression/click events.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG.GENERATED_TS IS
    'Timestamp when the harmonized daily aggregation was last regenerated.';



-- ---------------------------------------------------------------------------
-- 16-calculate-ad-sales-regional-rates.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;

-- Regional sales effectiveness derived entirely from INGEST-layer datasets.
USE SCHEMA HARMONIZED;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES AS
WITH daily_metrics AS (
    SELECT
        metric_date AS report_date,
        campaign_id,
        content_category,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM AME_AD_SALES_DEMO.INGEST.AD_PERFORMANCE_EVENTS
    GROUP BY 1, 2, 3
),
ads_region_raw AS (
    SELECT
        ae.event_date AS report_date,
        ae.campaign_id,
        COALESCE(spe.lad_code, 'UNKNOWN') AS lad_code,
        SUM(COALESCE(ae.event_weight, 1)) AS total_weight,
        COUNT(DISTINCT ae.unique_id) AS unique_subscribers
    FROM AME_AD_SALES_DEMO.INGEST.ADS_EVENTS ae
    LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
      ON spe.unique_id = ae.unique_id
    GROUP BY 1, 2, 3
),
ads_lad_share AS (
    SELECT
        arr.report_date,
        arr.campaign_id,
        arr.lad_code,
        arr.total_weight,
        arr.unique_subscribers,
        SUM(arr.total_weight) OVER (PARTITION BY arr.report_date, arr.campaign_id) AS campaign_weight_total
    FROM ads_region_raw arr
),
allocation AS (
    SELECT
        dm.report_date,
        dm.campaign_id,
        dm.content_category,
        COALESCE(al.lad_code, 'UNKNOWN') AS lad_code,
        dm.impressions * COALESCE(
            CASE WHEN al.campaign_weight_total > 0 THEN al.total_weight / al.campaign_weight_total END,
            1.0
        ) AS impressions,
        dm.clicks * COALESCE(
            CASE WHEN al.campaign_weight_total > 0 THEN al.total_weight / al.campaign_weight_total END,
            1.0
        ) AS clicks,
        dm.spend * COALESCE(
            CASE WHEN al.campaign_weight_total > 0 THEN al.total_weight / al.campaign_weight_total END,
            1.0
        ) AS spend,
        COALESCE(al.total_weight, 0) AS subscriber_touchpoints,
        COALESCE(al.unique_subscribers, 0) AS unique_subscribers
    FROM daily_metrics dm
    LEFT JOIN ads_lad_share al
      ON dm.report_date = al.report_date
     AND dm.campaign_id = al.campaign_id
),
aggregated AS (
    SELECT
        content_category,
        lad_code,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(subscriber_touchpoints) AS subscriber_touchpoints,
        SUM(unique_subscribers) AS unique_subscribers
    FROM allocation
    GROUP BY 1, 2
),
region_lookup AS (
    SELECT
        COALESCE(TRIM(UPPER(lad_code)), 'UNKNOWN') AS lad_code,
        COALESCE(NULLIF(TRIM(rural_urban), ''), 'UNKNOWN') AS rural_urban,
        COALESCE(NULLIF(TRIM(income_level), ''), 'UNKNOWN') AS income_level
    FROM AME_AD_SALES_DEMO.INGEST.DEMOGRAPHICS_DISTRIBUTIONS
    UNION
    SELECT 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'
)
SELECT
    agg.content_category,
    agg.lad_code,
    COALESCE(rl.rural_urban, 'UNKNOWN') AS rural_urban,
    COALESCE(rl.income_level, 'UNKNOWN') AS income_level,
    agg.impressions,
    agg.clicks,
    agg.spend,
    CASE WHEN agg.impressions > 0 THEN agg.clicks / agg.impressions ELSE 0 END AS ctr,
    CASE WHEN agg.impressions > 0 THEN (agg.spend * 1000) / agg.impressions ELSE 0 END AS ecpm,
    agg.subscriber_touchpoints,
    agg.impressions AS sampled_impressions,
    agg.unique_subscribers,
    CURRENT_TIMESTAMP() AS generated_ts
FROM aggregated agg
LEFT JOIN region_lookup rl
  ON rl.lad_code = agg.lad_code;

COMMENT ON TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES IS
    'Regional ad effectiveness mart distributing campaign delivery metrics across LAD geographies using observed ad events. Supports sales planning, pricing and geo-performance analyses.';

COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.CONTENT_CATEGORY IS
    'Content category dimension for the aggregated metrics (Sports, Lifestyle, etc.).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.LAD_CODE IS
    'Local Authority District code used to represent subscriber geography; derived from enriched subscriber profiles.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.RURAL_URBAN IS
    'Rural/urban classification placeholder (set to UNKNOWN in synthetic data but reserved for future enrichment).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.INCOME_LEVEL IS
    'Aggregated income band for the LAD based on subscriber profile enrichment.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.IMPRESSIONS IS
    'Campaign impressions allocated to the LAD using weighted distribution of ad events.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.CLICKS IS
    'Campaign clicks allocated to the LAD using weighted distribution of ad events.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.SPEND IS
    'Attributed spend allocated to the LAD consistent with impression allocation.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.CTR IS
    'Derived CTR for the LAD (clicks / impressions) after allocation.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.ECPM IS
    'Effective CPM for the LAD (spend * 1000 / impressions) to support regional price benchmarking.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.SUBSCRIBER_TOUCHPOINTS IS
    'Weighted audience touch count based on allocated ad event weights (proxy for exposures).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.SAMPLED_IMPRESSIONS IS
    'Alias for impressions retained for backwards compatibility with earlier scripts.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.UNIQUE_SUBSCRIBERS IS
    'Distinct subscribers estimated for the LAD via allocated ad event deduplication.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES.GENERATED_TS IS
    'Timestamp when the regional aggregation table was last refreshed.';



-- ---------------------------------------------------------------------------
-- 17-create-harmonized-marts.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA HARMONIZED;

/*
    AD_PERFORMANCE: Monthly campaign roll-ups for reporting.
*/
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE AS
WITH base AS (
    SELECT
        DATE_TRUNC('MONTH', report_date) AS report_month,
        campaign_id,
        advertiser_name,
        vertical,
        content_category,
        rate_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        ROUND(CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) / SUM(impressions) ELSE 0 END, 6) AS ctr,
        SUM(spend) AS spend,
        ROUND(CASE WHEN SUM(impressions) > 0 THEN (SUM(spend) * 1000) / SUM(impressions) ELSE 0 END, 4) AS ecpm,
        MAX(booked_cpm) AS booked_cpm,
        MAX(booked_ctr) AS booked_ctr,
        MAX(daily_impression_cap) AS peak_daily_cap,
        MAX(creative_count) AS creative_count,
        COUNT(DISTINCT report_date) AS active_days,
        SUM(observed_unique_subscribers) AS observed_unique_subscribers
    FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG
    GROUP BY
        DATE_TRUNC('MONTH', report_date),
        campaign_id,
        advertiser_name,
        vertical,
        content_category,
        rate_type
), persona_rollup AS (
    SELECT
        DATE_TRUNC('MONTH', report_date) AS report_month,
        campaign_id,
        advertiser_name,
        vertical,
        content_category,
        rate_type,
        ARRAY_AGG(DISTINCT COALESCE(NULLIF(persona.value::STRING, ''), 'UNKNOWN')) AS persona_list
    FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG,
         LATERAL FLATTEN(input => target_personas, outer => TRUE) persona
    GROUP BY 1,2,3,4,5,6
), device_rollup AS (
    SELECT
        DATE_TRUNC('MONTH', report_date) AS report_month,
        campaign_id,
        advertiser_name,
        vertical,
        content_category,
        rate_type,
        ARRAY_AGG(DISTINCT COALESCE(NULLIF(device.value::STRING, ''), 'UNKNOWN')) AS device_list
    FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG,
         LATERAL FLATTEN(input => target_devices, outer => TRUE) device
    GROUP BY 1,2,3,4,5,6
)
SELECT
    base.report_month,
    base.campaign_id,
    base.advertiser_name,
    base.vertical,
    base.content_category,
    base.rate_type,
    base.impressions,
    base.clicks,
    base.ctr,
    base.spend,
    base.ecpm,
    base.booked_cpm,
    base.booked_ctr,
    base.peak_daily_cap,
    base.creative_count,
    COALESCE(pr.persona_list, ARRAY_CONSTRUCT()) AS target_personas,
    COALESCE(dr.device_list, ARRAY_CONSTRUCT()) AS target_devices,
    base.active_days,
    base.observed_unique_subscribers,
    CURRENT_TIMESTAMP() AS generated_ts
FROM base
LEFT JOIN persona_rollup pr USING (report_month, campaign_id, advertiser_name, vertical, content_category, rate_type)
LEFT JOIN device_rollup dr USING (report_month, campaign_id, advertiser_name, vertical, content_category, rate_type);

COMMENT ON TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE IS
    'Monthly ad performance mart summarising campaign delivery metrics, targeting context and reach. Supports executive dashboards and pacing reviews.';

COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.REPORT_MONTH IS
    'Month (truncated date) for aggregated performance; derived from report_date in the daily fact.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.CAMPAIGN_ID IS
    'Campaign identifier from the sold inventory catalogue.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.ADVERTISER_NAME IS
    'Advertiser label associated with the campaign.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.VERTICAL IS
    'Industry vertical classification for the campaign.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.CONTENT_CATEGORY IS
    'Editorial content category targeted by the campaign.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.RATE_TYPE IS
    'Contractual rate type (CPM, Fixed, Hybrid).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.IMPRESSIONS IS
    'Monthly sum of impressions delivered.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.CLICKS IS
    'Monthly sum of clicks delivered.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.CTR IS
    'Monthly CTR (clicks / impressions) for the campaign.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.SPEND IS
    'Monthly spend allocated to the campaign (synthetic currency).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.ECPM IS
    'Effective CPM for the month (spend * 1000 / impressions).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.BOOKED_CPM IS
    'Highest booked CPM across days in the month.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.BOOKED_CTR IS
    'Highest booked CTR target across days in the month.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.PEAK_DAILY_CAP IS
    'Maximum daily impression cap observed in the month (pacing parameter).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.CREATIVE_COUNT IS
    'Maximum creative count observed in the month (approximation of creative mix).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.TARGET_PERSONAS IS
    'Array of personas reached within the month based on daily aggregates.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.TARGET_DEVICES IS
    'Array of devices observed for the campaign within the month.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.ACTIVE_DAYS IS
    'Number of distinct report_date values contributing to the month (measure of delivery continuity).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.OBSERVED_UNIQUE_SUBSCRIBERS IS
    'Sum of daily reach counts within the month (may over-count due to daily dedupe).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE.GENERATED_TS IS
    'Timestamp when the monthly ad performance mart was refreshed.';

/*
    AD_RATES: Generalized rate card by category and LAD region.
*/
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_RATES AS
SELECT
    content_category,
    lad_code,
    rural_urban,
    income_level,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(spend) AS spend,
    ROUND(CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) / SUM(impressions) ELSE 0 END, 6) AS ctr,
    ROUND(CASE WHEN SUM(impressions) > 0 THEN (SUM(spend) * 1000) / SUM(impressions) ELSE 0 END, 4) AS ecpm,
    SUM(subscriber_touchpoints) AS subscriber_touchpoints,
    SUM(sampled_impressions) AS sampled_impressions,
    SUM(unique_subscribers) AS unique_subscribers,
    CURRENT_TIMESTAMP() AS generated_ts
FROM AME_AD_SALES_DEMO.HARMONIZED.AD_SALES_REGION_CATEGORY_RATES
GROUP BY
    content_category,
    lad_code,
    rural_urban,
    income_level;

COMMENT ON TABLE AME_AD_SALES_DEMO.HARMONIZED.AD_RATES IS
    'Rate card style mart summarising impressions, spend and rate metrics by content category and LAD. Built from the regional performance table for downstream pricing simulations.';

COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.CONTENT_CATEGORY IS
    'Content category dimension inherited from campaign targeting.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.LAD_CODE IS
    'Local Authority District code representing geographic slice of delivery.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.RURAL_URBAN IS
    'Rural/urban classification placeholder (currently UNKNOWN but kept for production alignment).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.INCOME_LEVEL IS
    'Income band aggregated at the LAD level from subscriber enrichment.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.IMPRESSIONS IS
    'Total impressions allocated to the LAD and content category.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.CLICKS IS
    'Total clicks allocated to the LAD and content category.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.SPEND IS
    'Total spend allocated to the LAD and content category.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.CTR IS
    'Click-through rate for the LAD/category segment.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.ECPM IS
    'Effective CPM for the LAD/category segment (spend * 1000 / impressions).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.SUBSCRIBER_TOUCHPOINTS IS
    'Total weighted subscriber touches contributing to the LAD/category segment.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.SAMPLED_IMPRESSIONS IS
    'Alias for impressions retained for compatibility.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.UNIQUE_SUBSCRIBERS IS
    'Distinct subscriber count allocated to the LAD/category segment.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AD_RATES.GENERATED_TS IS
    'Timestamp when the aggregated rate mart was generated.';

/*
    AGGREGATED_BEHAVIORAL_LOGS: Subscriber-level behavioural features.
*/
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS AS
WITH behavior_events AS (
    SELECT
        ce.unique_id,
        ce.event_ts,
        ce.event_type,
        ce.content_category,
        ce.content_type,
        ce.page_path,
        LOWER(
            COALESCE(
                NULLIF(ce.content_category, ''),
                NULLIF(ce.content_type, ''),
                NULLIF(ce.page_path, ''),
                ce.attributes:content_category::STRING,
                ce.attributes:content_type::STRING,
                ce.attributes:page_name::STRING,
                ce.event_type
            )
        ) AS content_tag,
        DATE_TRUNC('MONTH', ce.event_ts) AS event_month,
        DATE_TRUNC('DAY', ce.event_ts) AS event_day,
        -- Identify viewing events (content consumption events)
        CASE 
            WHEN ce.event_type IN ('PLAY_START', 'PLAY_STOP', 'PLAY_RESUME', 'PAUSE', 'CLICK_CONTENT', 'BROWSE_PAGE', 'ADD_TO_MY_LIST', 'SKIP_INTRO')
            THEN 1 
            ELSE 0 
        END AS is_content_view
    FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
    WHERE ce.unique_id IS NOT NULL
),
content_view_events AS (
    SELECT
        unique_id,
        content_category,
        page_path,
        content_type
    FROM behavior_events
    WHERE is_content_view = 1
        AND (content_category IS NOT NULL OR page_path IS NOT NULL OR content_type IS NOT NULL)
),
content_view_categories AS (
    SELECT
        unique_id,
        ARRAY_AGG(DISTINCT content_category) AS content_view_categories
    FROM content_view_events
    WHERE content_category IS NOT NULL AND content_category <> ''
    GROUP BY unique_id
),
content_paths AS (
    SELECT
        unique_id,
        ARRAY_AGG(DISTINCT page_path) AS content_paths
    FROM content_view_events
    WHERE page_path IS NOT NULL AND page_path <> ''
    GROUP BY unique_id
),
content_view_counts AS (
    SELECT
        unique_id,
        COUNT(*) AS content_views_count
    FROM behavior_events
    WHERE is_content_view = 1
    GROUP BY unique_id
),
persona_scores AS (
    SELECT
        unique_id,
        content_tag,
        COUNT(*) AS event_count
    FROM behavior_events
    WHERE content_tag IS NOT NULL AND content_tag <> ''
    GROUP BY unique_id, content_tag
),
persona_ranked AS (
    SELECT
        ps.unique_id,
        ps.content_tag,
        ps.event_count,
        SUM(ps.event_count) OVER (PARTITION BY ps.unique_id) AS total_events,
        ROW_NUMBER() OVER (
            PARTITION BY ps.unique_id
            ORDER BY ps.event_count DESC, ps.content_tag
        ) AS rank_in_segment
    FROM persona_scores ps
),
persona_inference AS (
    SELECT
        pr.unique_id,
        pr.content_tag AS primary_content_tag,
        CASE
            WHEN pr.content_tag ILIKE '%sport%' THEN 'SPORTS_ENGAGED'
            WHEN pr.content_tag ILIKE '%live%' THEN 'LIVE_EVENT_FOLLOWER'
            WHEN pr.content_tag ILIKE '%kids%' OR pr.content_tag ILIKE '%family%' OR pr.content_tag ILIKE '%animation%' THEN 'FAMILY_HOUSEHOLD'
            WHEN pr.content_tag ILIKE '%documentary%' OR pr.content_tag ILIKE '%knowledge%' OR pr.content_tag ILIKE '%docu%' THEN 'KNOWLEDGE_SEEKER'
            WHEN pr.content_tag ILIKE '%lifestyle%' OR pr.content_tag ILIKE '%wellness%' THEN 'LIFESTYLE_MINDED'
            WHEN pr.content_tag ILIKE '%reality%' THEN 'REALITY_FAN'
            WHEN pr.content_tag ILIKE '%original%' OR pr.content_tag ILIKE '%drama%' THEN 'PREMIUM_DRAMA'
            ELSE 'GENERAL_STREAMER'
        END AS derived_persona,
        pr.total_events
    FROM persona_ranked pr
    WHERE pr.rank_in_segment = 1
),
profile_summary AS (
    SELECT
        be.unique_id,
        COALESCE(pi.derived_persona, 'GENERAL_STREAMER') AS persona,
        COALESCE(pi.primary_content_tag, 'unknown') AS primary_content_tag
    FROM (SELECT DISTINCT unique_id FROM behavior_events) be
    LEFT JOIN persona_inference pi USING (unique_id)
),
monthly_visits AS (
    SELECT
        unique_id,
        AVG(visit_days)::FLOAT AS avg_visit_days
    FROM (
        SELECT
            unique_id,
            event_month,
            COUNT(DISTINCT event_day) AS visit_days
        FROM behavior_events
        GROUP BY unique_id, event_month
    )
    GROUP BY unique_id
),
login_stats AS (
    SELECT
        unique_id,
        COUNT(DISTINCT event_day) AS active_days,
        MIN(event_day) AS first_day,
        MAX(event_day) AS last_day
    FROM behavior_events
    GROUP BY unique_id
),
content_categories AS (
    SELECT
        unique_id,
        ARRAY_AGG(DISTINCT content_tag) AS content_categories
    FROM behavior_events
    WHERE content_tag IS NOT NULL AND content_tag <> ''
    GROUP BY unique_id
),
event_counts AS (
    SELECT
        unique_id,
        COUNT(*) AS total_events,
        MAX(event_ts) AS last_event_ts
    FROM behavior_events
    GROUP BY unique_id
)
SELECT
    ps.unique_id,
    ps.persona,
    ps.primary_content_tag,
    COALESCE(cc.content_categories, ARRAY_CONSTRUCT()) AS content_categories,
    -- Content view metrics (separated from other events)
    COALESCE(cvc.content_views_count, 0) AS content_views_count,
    COALESCE(cvcat.content_view_categories, ARRAY_CONSTRUCT()) AS content_view_categories,
    COALESCE(cp.content_paths, ARRAY_CONSTRUCT()) AS content_paths,
    ROUND(COALESCE(mv.avg_visit_days, 0), 2) AS avg_site_visits_per_month,
    CASE
        WHEN ls.first_day IS NULL OR ls.last_day IS NULL THEN 0
        ELSE ROUND(
            ls.active_days::FLOAT /
            NULLIF(((DATEDIFF(day, ls.first_day, ls.last_day) + 1)::FLOAT / 7.0), 0),
            2
        )
    END AS login_frequency_per_week,
    COALESCE(ec.total_events, 0) AS total_events,
    ec.last_event_ts,
    CURRENT_TIMESTAMP() AS generated_ts
FROM profile_summary ps
LEFT JOIN content_categories cc ON cc.unique_id = ps.unique_id
LEFT JOIN content_view_counts cvc ON cvc.unique_id = ps.unique_id
LEFT JOIN content_view_categories cvcat ON cvcat.unique_id = ps.unique_id
LEFT JOIN content_paths cp ON cp.unique_id = ps.unique_id
LEFT JOIN monthly_visits mv ON mv.unique_id = ps.unique_id
LEFT JOIN login_stats ls ON ls.unique_id = ps.unique_id
LEFT JOIN event_counts ec ON ec.unique_id = ps.unique_id;

COMMENT ON TABLE AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS IS
    'Subscriber-level behavioural feature mart summarising clickstream intensity, engagement recency and inferred personas. Acts as the behavioural dimension for feature engineering and dashboards.';

COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.UNIQUE_ID IS
    'Subscriber unique identifier; join key to SUBSCRIBER_PROFILE_ENRICHED and analytic feature tables.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.PERSONA IS
    'Behaviourally inferred persona label based on dominant content interactions (e.g., SPORTS_ENGAGED).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.PRIMARY_CONTENT_TAG IS
    'Primary content tag representing the dominant genre/theme of the subscriber’s consumption.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.CONTENT_CATEGORIES IS
    'Array of distinct content tags consumed by the subscriber (normalised lower-case tokens).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.CONTENT_VIEWS_COUNT IS
    'Total count of content viewing events (PLAY_START, PLAY_STOP, CLICK_CONTENT, BROWSE_PAGE, etc.) separated from other event types.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.CONTENT_VIEW_CATEGORIES IS
    'Array of distinct content categories specifically from viewing events (PLAY_START, PLAY_STOP, CLICK_CONTENT, BROWSE_PAGE, etc.).';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.CONTENT_PATHS IS
    'Array of distinct page paths accessed during content viewing events, tracking navigation patterns and content discovery paths.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.AVG_SITE_VISITS_PER_MONTH IS
    'Average number of active visit days per month calculated from clickstream events.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.LOGIN_FREQUENCY_PER_WEEK IS
    'Average login frequency per week derived from daily activity counts.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.TOTAL_EVENTS IS
    'Total number of clickstream events captured across the observation window.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.LAST_EVENT_TS IS
    'Timestamp of the most recent clickstream event for the subscriber.';
COMMENT ON COLUMN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS.GENERATED_TS IS
    'Timestamp indicating when the behavioural aggregation was generated.';



-- ---------------------------------------------------------------------------
-- 18-feature-engineering-procedures.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA ANALYSE;

/*
    Comprehensive feature engineering pipeline aligned with docs/feature_engineering_plan.md.
    - Builds shared history and wide feature tables sourced from INGEST + HARMONIZED layers
    - Materializes specialised feature sets for content attribution, clustering, churn, and LTV
    - Documents every artifact with table/column comments for lineage clarity
*/
CREATE OR REPLACE PROCEDURE FEATURE_ENGINEER_SUBSCRIBER_METRICS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    run_started TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
BEGIN
    --------------------------------------------------------------------------
    -- Base history (identity + behaviour + ad engagement + clickstream)
    --------------------------------------------------------------------------
    CREATE OR REPLACE TABLE FE_SUBSCRIBER_HISTORY AS
    WITH behaviour AS (
        SELECT
            abl.unique_id,
            abl.persona,
            abl.avg_site_visits_per_month,
            abl.login_frequency_per_week,
            abl.total_events,
            abl.last_event_ts,
            abl.content_categories
        FROM AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS abl
    ),
    subscriber_enriched AS (
        SELECT
            spe.unique_id,
            spe.profile_id,
            spe.tier,
            spe.full_name,
            spe.email,
            spe.primary_mobile,
            spe.ip_address,
            spe.lad_code,
            spe.area_name,
            spe.latitude,
            spe.longitude,
            spe.age,
            spe.age_band,
            spe.income_level,
            spe.education_level,
            spe.marital_status,
            spe.family_status,
            spe.behavioral_digital_media_consumption_index,
            spe.behavioral_fast_fashion_retail_propensity,
            spe.behavioral_grocery_online_delivery_use,
            spe.behavioral_financial_investment_interest
        FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
    ),
    ad_metrics AS (
        SELECT
            ae.unique_id,
            COUNT_IF(ae.event_type = 'IMPRESSION') AS impression_events,
            COUNT_IF(ae.event_type = 'CLICK') AS click_events,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' THEN ae.event_weight ELSE 0 END) AS impression_units,
            SUM(CASE WHEN ae.event_type = 'CLICK' THEN ae.event_weight ELSE 0 END) AS click_units,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' AND ae.event_date >= DATEADD(day,-30,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS impression_units_30,
            SUM(CASE WHEN ae.event_type = 'CLICK' AND ae.event_date >= DATEADD(day,-30,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS click_units_30,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' AND ae.event_date >= DATEADD(day,-90,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS impression_units_90,
            SUM(CASE WHEN ae.event_type = 'CLICK' AND ae.event_date >= DATEADD(day,-90,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS click_units_90,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' AND ae.event_date >= DATEADD(day,-180,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS impression_units_180,
            SUM(CASE WHEN ae.event_type = 'CLICK' AND ae.event_date >= DATEADD(day,-180,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS click_units_180,
            SUM(COALESCE(ap.effective_cpm, ap.booked_cpm, 0) * ae.event_weight / 1000) AS monetization_total,
            SUM(CASE WHEN ae.event_date >= DATEADD(day,-90,CURRENT_DATE()) THEN COALESCE(ap.effective_cpm, ap.booked_cpm, 0) * ae.event_weight / 1000 ELSE 0 END) AS monetization_90,
            SUM(CASE WHEN ae.event_date >= DATEADD(day,-180,CURRENT_DATE()) THEN COALESCE(ap.effective_cpm, ap.booked_cpm, 0) * ae.event_weight / 1000 ELSE 0 END) AS monetization_180,
            MIN(ae.event_ts) AS first_ad_event_ts,
            MAX(ae.event_ts) AS last_ad_event_ts
        FROM AME_AD_SALES_DEMO.INGEST.ADS_EVENTS ae
        LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG ap
          ON ap.campaign_id = ae.campaign_id
         AND ap.report_date = ae.event_date
        GROUP BY ae.unique_id
    ),
    clickstream AS (
        SELECT
            ce.unique_id,
            MIN(ce.event_ts) AS first_event_ts,
            MAX(ce.event_ts) AS last_event_ts,
            COUNT(*) AS clickstream_events,
            COUNT(DISTINCT DATE_TRUNC('DAY', ce.event_ts)) AS active_days,
            COUNT(DISTINCT ce.event_type) AS distinct_event_types
        FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
        GROUP BY ce.unique_id
    ),
    watch_time_metrics AS (
        WITH play_events AS (
            SELECT
                ce.unique_id,
                ce.event_ts,
                ce.event_type,
                ce.session_id,
                ce.content_type,
                ce.content_category,
                ce.device,
                ce.attributes,
                -- Find the most recent PLAY_START before each PLAY_STOP
                MAX(CASE WHEN ce.event_type = 'PLAY_START' THEN ce.event_ts END) 
                    OVER (PARTITION BY ce.unique_id, COALESCE(ce.session_id, '') 
                          ORDER BY ce.event_ts 
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_play_start_ts
            FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
            WHERE ce.event_type IN ('PLAY_START', 'PLAY_STOP')
        ),
        play_sessions AS (
            SELECT
                pe.unique_id,
                pe.event_ts,
                pe.event_type,
                pe.session_id,
                pe.content_type,
                pe.content_category,
                pe.device,
                pe.attributes,
                -- Extract duration_seconds from attributes, or calculate from PLAY_START to PLAY_STOP
                CASE 
                    WHEN pe.event_type = 'PLAY_STOP' THEN
                        COALESCE(
                            pe.attributes:duration_seconds::FLOAT,
                            pe.attributes:duration::FLOAT,
                            CASE 
                                WHEN pe.last_play_start_ts IS NOT NULL 
                                THEN TIMESTAMPDIFF('second', pe.last_play_start_ts, pe.event_ts)
                                ELSE NULL
                            END
                        )
                    ELSE NULL
                END AS duration_seconds,
                -- Extract completion reason
                CASE 
                    WHEN pe.event_type = 'PLAY_STOP' THEN pe.attributes:reason::STRING
                    ELSE NULL
                END AS stop_reason
            FROM play_events pe
        ),
        watch_time_by_intervals AS (
            SELECT
                ps.unique_id,
                -- Lifetime totals
                SUM(COALESCE(ps.duration_seconds, 0)) AS watch_time_total,
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL) AS watch_session_count,
                MAX(ps.duration_seconds) AS watch_longest_session_seconds,
                -- Daily totals
                SUM(CASE WHEN DATE_TRUNC('DAY', ps.event_ts) = DATE_TRUNC('DAY', CURRENT_DATE()) THEN COALESCE(ps.duration_seconds, 0) ELSE 0 END) AS watch_time_daily,
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND DATE_TRUNC('DAY', ps.event_ts) = DATE_TRUNC('DAY', CURRENT_DATE()) AND ps.duration_seconds IS NOT NULL) AS watch_session_count_daily,
                -- Weekly totals
                SUM(CASE WHEN DATE_TRUNC('WEEK', ps.event_ts) = DATE_TRUNC('WEEK', CURRENT_DATE()) THEN COALESCE(ps.duration_seconds, 0) ELSE 0 END) AS watch_time_weekly,
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND DATE_TRUNC('WEEK', ps.event_ts) = DATE_TRUNC('WEEK', CURRENT_DATE()) AND ps.duration_seconds IS NOT NULL) AS watch_session_count_weekly,
                -- Monthly totals
                SUM(CASE WHEN DATE_TRUNC('MONTH', ps.event_ts) = DATE_TRUNC('MONTH', CURRENT_DATE()) THEN COALESCE(ps.duration_seconds, 0) ELSE 0 END) AS watch_time_monthly,
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND DATE_TRUNC('MONTH', ps.event_ts) = DATE_TRUNC('MONTH', CURRENT_DATE()) AND ps.duration_seconds IS NOT NULL) AS watch_session_count_monthly,
                -- 30-day rolling
                SUM(CASE WHEN ps.event_ts >= DATEADD(day, -30, CURRENT_DATE()) THEN COALESCE(ps.duration_seconds, 0) ELSE 0 END) AS watch_time_30,
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND ps.event_ts >= DATEADD(day, -30, CURRENT_DATE()) AND ps.duration_seconds IS NOT NULL) AS watch_session_count_30,
                -- 90-day rolling
                SUM(CASE WHEN ps.event_ts >= DATEADD(day, -90, CURRENT_DATE()) THEN COALESCE(ps.duration_seconds, 0) ELSE 0 END) AS watch_time_90,
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND ps.event_ts >= DATEADD(day, -90, CURRENT_DATE()) AND ps.duration_seconds IS NOT NULL) AS watch_session_count_90,
                -- 180-day rolling
                SUM(CASE WHEN ps.event_ts >= DATEADD(day, -180, CURRENT_DATE()) THEN COALESCE(ps.duration_seconds, 0) ELSE 0 END) AS watch_time_180,
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND ps.event_ts >= DATEADD(day, -180, CURRENT_DATE()) AND ps.duration_seconds IS NOT NULL) AS watch_session_count_180,
                -- Completion rate (sessions with completed_episode or finish_episode reason)
                COUNT_IF(ps.event_type = 'PLAY_STOP' AND ps.stop_reason IN ('completed_episode', 'finish_episode')) AS watch_completed_sessions,
                -- Active weeks count (for average calculation)
                COUNT(DISTINCT DATE_TRUNC('WEEK', ps.event_ts)) AS active_weeks_count,
                -- Active days count (for average per day)
                COUNT(DISTINCT DATE_TRUNC('DAY', ps.event_ts)) AS watch_active_days
            FROM play_sessions ps
            WHERE ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL
            GROUP BY ps.unique_id
        ),
        watch_time_by_content AS (
            SELECT
                ps.unique_id,
                ARRAY_AGG(OBJECT_CONSTRUCT(
                    'content_type', ps.content_type,
                    'watch_time_seconds', content_watch_time
                )) WITHIN GROUP (ORDER BY content_watch_time DESC) AS watch_time_by_content_type
            FROM (
                SELECT
                    ps.unique_id,
                    ps.content_type,
                    SUM(COALESCE(ps.duration_seconds, 0)) AS content_watch_time
                FROM play_sessions ps
                WHERE ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL AND ps.content_type IS NOT NULL
                GROUP BY ps.unique_id, ps.content_type
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.unique_id ORDER BY SUM(COALESCE(ps.duration_seconds, 0)) DESC) <= 10
            ) ps
            GROUP BY ps.unique_id
        ),
        watch_time_by_device AS (
            SELECT
                ps.unique_id,
                ARRAY_AGG(OBJECT_CONSTRUCT(
                    'device', ps.device,
                    'watch_time_seconds', device_watch_time
                )) WITHIN GROUP (ORDER BY device_watch_time DESC) AS watch_time_by_device_type
            FROM (
                SELECT
                    ps.unique_id,
                    ps.device,
                    SUM(COALESCE(ps.duration_seconds, 0)) AS device_watch_time
                FROM play_sessions ps
                WHERE ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL AND ps.device IS NOT NULL
                GROUP BY ps.unique_id, ps.device
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.unique_id ORDER BY SUM(COALESCE(ps.duration_seconds, 0)) DESC) <= 10
            ) ps
            GROUP BY ps.unique_id
        ),
        peak_viewing_times AS (
            SELECT
                hour_data.unique_id,
                hour_data.watch_peak_hour,
                dow_data.watch_peak_day_of_week
            FROM (
                SELECT
                    ps.unique_id,
                    HOUR(ps.event_ts) AS watch_peak_hour
                FROM play_sessions ps
                WHERE ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL
                GROUP BY ps.unique_id, HOUR(ps.event_ts)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.unique_id ORDER BY SUM(COALESCE(ps.duration_seconds, 0)) DESC) = 1
            ) hour_data
            LEFT JOIN (
                SELECT
                    ps.unique_id,
                    DAYOFWEEK(ps.event_ts) - 1 AS watch_peak_day_of_week
                FROM play_sessions ps
                WHERE ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL
                GROUP BY ps.unique_id, DAYOFWEEK(ps.event_ts)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.unique_id ORDER BY SUM(COALESCE(ps.duration_seconds, 0)) DESC) = 1
            ) dow_data ON dow_data.unique_id = hour_data.unique_id
        ),
        binge_indicators AS (
            SELECT
                ps.unique_id,
                MAX(CASE 
                    WHEN daily_sessions >= 3 OR daily_watch_time >= 7200 THEN 1 
                    ELSE 0 
                END) AS watch_binge_indicator
            FROM (
                SELECT
                    ps.unique_id,
                    DATE_TRUNC('DAY', ps.event_ts) AS watch_date,
                    COUNT_IF(ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL) AS daily_sessions,
                    SUM(COALESCE(ps.duration_seconds, 0)) AS daily_watch_time
                FROM play_sessions ps
                WHERE ps.event_type = 'PLAY_STOP' AND ps.duration_seconds IS NOT NULL
                GROUP BY ps.unique_id, DATE_TRUNC('DAY', ps.event_ts)
            ) ps
            GROUP BY ps.unique_id
        )
        SELECT
            wti.unique_id,
            -- Basic metrics - totals
            COALESCE(wti.watch_time_total, 0) AS watch_time_total,
            COALESCE(wti.watch_time_daily, 0) AS watch_time_daily,
            COALESCE(wti.watch_time_weekly, 0) AS watch_time_weekly,
            COALESCE(wti.watch_time_monthly, 0) AS watch_time_monthly,
            COALESCE(wti.watch_time_30, 0) AS watch_time_30,
            COALESCE(wti.watch_time_90, 0) AS watch_time_90,
            COALESCE(wti.watch_time_180, 0) AS watch_time_180,
            -- Basic metrics - session counts
            COALESCE(wti.watch_session_count, 0) AS watch_session_count,
            COALESCE(wti.watch_session_count_daily, 0) AS watch_session_count_daily,
            COALESCE(wti.watch_session_count_weekly, 0) AS watch_session_count_weekly,
            COALESCE(wti.watch_session_count_monthly, 0) AS watch_session_count_monthly,
            COALESCE(wti.watch_session_count_30, 0) AS watch_session_count_30,
            COALESCE(wti.watch_session_count_90, 0) AS watch_session_count_90,
            COALESCE(wti.watch_session_count_180, 0) AS watch_session_count_180,
            -- Basic metrics - averages
            CASE 
                WHEN COALESCE(wti.watch_session_count, 0) > 0 
                THEN COALESCE(wti.watch_time_total, 0) / wti.watch_session_count 
                ELSE 0 
            END AS watch_session_avg_duration,
            CASE 
                WHEN COALESCE(wti.watch_session_count_daily, 0) > 0 
                THEN COALESCE(wti.watch_time_daily, 0) / wti.watch_session_count_daily 
                ELSE 0 
            END AS watch_session_avg_duration_daily,
            CASE 
                WHEN COALESCE(wti.watch_session_count_weekly, 0) > 0 
                THEN COALESCE(wti.watch_time_weekly, 0) / wti.watch_session_count_weekly 
                ELSE 0 
            END AS watch_session_avg_duration_weekly,
            CASE 
                WHEN COALESCE(wti.watch_session_count_monthly, 0) > 0 
                THEN COALESCE(wti.watch_time_monthly, 0) / wti.watch_session_count_monthly 
                ELSE 0 
            END AS watch_session_avg_duration_monthly,
            CASE 
                WHEN COALESCE(wti.watch_session_count_30, 0) > 0 
                THEN COALESCE(wti.watch_time_30, 0) / wti.watch_session_count_30 
                ELSE 0 
            END AS watch_session_avg_duration_30,
            CASE 
                WHEN COALESCE(wti.watch_session_count_90, 0) > 0 
                THEN COALESCE(wti.watch_time_90, 0) / wti.watch_session_count_90 
                ELSE 0 
            END AS watch_session_avg_duration_90,
            CASE 
                WHEN COALESCE(wti.watch_session_count_180, 0) > 0 
                THEN COALESCE(wti.watch_time_180, 0) / wti.watch_session_count_180 
                ELSE 0 
            END AS watch_session_avg_duration_180,
            -- Average watch time per week
            CASE 
                WHEN COALESCE(wti.active_weeks_count, 0) > 0 
                THEN COALESCE(wti.watch_time_total, 0) / wti.active_weeks_count 
                ELSE 0 
            END AS watch_time_weekly_avg,
            -- Average watch time per active day
            CASE 
                WHEN COALESCE(wti.watch_active_days, 0) > 0 
                THEN COALESCE(wti.watch_time_total, 0) / wti.watch_active_days 
                ELSE 0 
            END AS watch_time_avg_per_active_day,
            -- Comprehensive metrics
            COALESCE(wti.watch_longest_session_seconds, 0) AS watch_longest_session_seconds,
            CASE 
                WHEN COALESCE(wti.watch_session_count, 0) > 0 
                THEN COALESCE(wti.watch_completed_sessions, 0) / wti.watch_session_count 
                ELSE 0 
            END AS watch_completion_rate,
            COALESCE(wtc.watch_time_by_content_type, ARRAY_CONSTRUCT()) AS watch_time_by_content_type,
            COALESCE(wtd.watch_time_by_device_type, ARRAY_CONSTRUCT()) AS watch_time_by_device_type,
            COALESCE(pvt.watch_peak_hour, NULL) AS watch_peak_hour,
            COALESCE(pvt.watch_peak_day_of_week, NULL) AS watch_peak_day_of_week,
            COALESCE(bi.watch_binge_indicator, 0) AS watch_binge_indicator
        FROM watch_time_by_intervals wti
        LEFT JOIN watch_time_by_content wtc ON wtc.unique_id = wti.unique_id
        LEFT JOIN watch_time_by_device wtd ON wtd.unique_id = wti.unique_id
        LEFT JOIN peak_viewing_times pvt ON pvt.unique_id = wti.unique_id
        LEFT JOIN binge_indicators bi ON bi.unique_id = wti.unique_id
    ),
    negative_events AS (
        SELECT
            ce.unique_id,
            COUNT(*) AS negative_event_count,
            COUNT_IF(ce.event_ts >= DATEADD(day,-30,CURRENT_TIMESTAMP())) AS negative_event_count_30
        FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
        WHERE ce.event_type IN ('REBUFFER_EVENT','ERROR_EVENT','CANCELLATION_SURVEY_SUBMIT')
        GROUP BY ce.unique_id
    ),
    mau_mav_metrics AS (
        SELECT
            ce.unique_id,
            -- MAU: Count of distinct months with any clickstream activity
            COUNT(DISTINCT DATE_TRUNC('MONTH', ce.event_ts)) AS mau_count,
            -- MAV: Count of distinct months with viewing activity (PLAY_START or PLAY_STOP)
            COUNT(DISTINCT CASE 
                WHEN ce.event_type IN ('PLAY_START', 'PLAY_STOP') 
                THEN DATE_TRUNC('MONTH', ce.event_ts) 
            END) AS mav_count,
            -- Current month MAU indicator (1 if active this month, 0 otherwise)
            MAX(CASE 
                WHEN DATE_TRUNC('MONTH', ce.event_ts) = DATE_TRUNC('MONTH', CURRENT_DATE()) 
                THEN 1 
                ELSE 0 
            END) AS mau_current_month,
            -- Current month MAV indicator (1 if viewed this month, 0 otherwise)
            MAX(CASE 
                WHEN ce.event_type IN ('PLAY_START', 'PLAY_STOP') 
                    AND DATE_TRUNC('MONTH', ce.event_ts) = DATE_TRUNC('MONTH', CURRENT_DATE()) 
                THEN 1 
                ELSE 0 
            END) AS mav_current_month,
            -- Last active month (most recent month with activity)
            MAX(DATE_TRUNC('MONTH', ce.event_ts)) AS last_active_month,
            -- Last viewing month (most recent month with viewing activity)
            MAX(CASE 
                WHEN ce.event_type IN ('PLAY_START', 'PLAY_STOP') 
                THEN DATE_TRUNC('MONTH', ce.event_ts) 
            END) AS last_viewing_month
        FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
        GROUP BY ce.unique_id
    )
    SELECT
        se.unique_id,
        se.profile_id,
        se.tier,
        beh.persona,
        beh.avg_site_visits_per_month,
        beh.login_frequency_per_week,
        beh.total_events AS behavioural_events,
        beh.content_categories,
        beh.last_event_ts AS behavioural_last_event_ts,
        se.lad_code,
        se.area_name,
        se.latitude,
        se.longitude,
        se.age,
        se.age_band,
        se.income_level,
        se.education_level,
        se.marital_status,
        se.family_status,
        se.behavioral_digital_media_consumption_index,
        se.behavioral_fast_fashion_retail_propensity,
        se.behavioral_grocery_online_delivery_use,
        se.behavioral_financial_investment_interest,
        COALESCE(adm.impression_events, 0) AS impression_events,
        COALESCE(adm.click_events, 0) AS click_events,
        COALESCE(adm.impression_units, 0) AS impression_units,
        COALESCE(adm.click_units, 0) AS click_units,
        COALESCE(adm.first_ad_event_ts, NULL) AS first_ad_event_ts,
        COALESCE(adm.last_ad_event_ts, NULL) AS last_ad_event_ts,
        cs.first_event_ts AS clickstream_first_event_ts,
        cs.last_event_ts AS clickstream_last_event_ts,
        CASE
            WHEN adm.first_ad_event_ts IS NULL THEN cs.first_event_ts
            WHEN cs.first_event_ts IS NULL THEN adm.first_ad_event_ts
            ELSE LEAST(adm.first_ad_event_ts, cs.first_event_ts)
        END AS first_seen_ts,
        cs.clickstream_events,
        cs.active_days AS clickstream_active_days,
        cs.distinct_event_types,
        COALESCE(ne.negative_event_count, 0) AS negative_event_count,
        COALESCE(ne.negative_event_count_30, 0) AS negative_event_count_30,
        -- Watch time metrics
        COALESCE(wtm.watch_time_total, 0) AS watch_time_total,
        COALESCE(wtm.watch_time_daily, 0) AS watch_time_daily,
        COALESCE(wtm.watch_time_weekly, 0) AS watch_time_weekly,
        COALESCE(wtm.watch_time_monthly, 0) AS watch_time_monthly,
        COALESCE(wtm.watch_time_30, 0) AS watch_time_30,
        COALESCE(wtm.watch_time_90, 0) AS watch_time_90,
        COALESCE(wtm.watch_time_180, 0) AS watch_time_180,
        COALESCE(wtm.watch_session_count, 0) AS watch_session_count,
        COALESCE(wtm.watch_session_count_daily, 0) AS watch_session_count_daily,
        COALESCE(wtm.watch_session_count_weekly, 0) AS watch_session_count_weekly,
        COALESCE(wtm.watch_session_count_monthly, 0) AS watch_session_count_monthly,
        COALESCE(wtm.watch_session_count_30, 0) AS watch_session_count_30,
        COALESCE(wtm.watch_session_count_90, 0) AS watch_session_count_90,
        COALESCE(wtm.watch_session_count_180, 0) AS watch_session_count_180,
        COALESCE(wtm.watch_session_avg_duration, 0) AS watch_session_avg_duration,
        COALESCE(wtm.watch_session_avg_duration_daily, 0) AS watch_session_avg_duration_daily,
        COALESCE(wtm.watch_session_avg_duration_weekly, 0) AS watch_session_avg_duration_weekly,
        COALESCE(wtm.watch_session_avg_duration_monthly, 0) AS watch_session_avg_duration_monthly,
        COALESCE(wtm.watch_session_avg_duration_30, 0) AS watch_session_avg_duration_30,
        COALESCE(wtm.watch_session_avg_duration_90, 0) AS watch_session_avg_duration_90,
        COALESCE(wtm.watch_session_avg_duration_180, 0) AS watch_session_avg_duration_180,
        COALESCE(wtm.watch_time_weekly_avg, 0) AS watch_time_weekly_avg,
        COALESCE(wtm.watch_time_avg_per_active_day, 0) AS watch_time_avg_per_active_day,
        COALESCE(wtm.watch_longest_session_seconds, 0) AS watch_longest_session_seconds,
        COALESCE(wtm.watch_completion_rate, 0) AS watch_completion_rate,
        COALESCE(wtm.watch_time_by_content_type, ARRAY_CONSTRUCT()) AS watch_time_by_content_type,
        COALESCE(wtm.watch_time_by_device_type, ARRAY_CONSTRUCT()) AS watch_time_by_device_type,
        wtm.watch_peak_hour,
        wtm.watch_peak_day_of_week,
        COALESCE(wtm.watch_binge_indicator, 0) AS watch_binge_indicator,
        -- MAU/MAV metrics
        COALESCE(mam.mau_count, 0) AS mau_count,
        COALESCE(mam.mav_count, 0) AS mav_count,
        COALESCE(mam.mau_current_month, 0) AS mau_current_month,
        COALESCE(mam.mav_current_month, 0) AS mav_current_month,
        mam.last_active_month,
        mam.last_viewing_month,
        CURRENT_TIMESTAMP() AS generated_ts
    FROM subscriber_enriched se
    LEFT JOIN behaviour beh ON beh.unique_id = se.unique_id
    LEFT JOIN ad_metrics adm ON adm.unique_id = se.unique_id
    LEFT JOIN clickstream cs ON cs.unique_id = se.unique_id
    LEFT JOIN negative_events ne ON ne.unique_id = se.unique_id
    LEFT JOIN watch_time_metrics wtm ON wtm.unique_id = se.unique_id
    LEFT JOIN mau_mav_metrics mam ON mam.unique_id = se.unique_id;

    COMMENT ON TABLE FE_SUBSCRIBER_HISTORY IS
        'Base subscriber history combining identity, behavioural, ad engagement, and clickstream aggregates sourced from harmonized + ingest layers.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.UNIQUE_ID IS 'Synthetic subscriber identifier (primary key for analytic joins).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.PERSONA IS 'Persona inferred from behavioural logs; used for clustering and modelling.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.AVG_SITE_VISITS_PER_MONTH IS 'Average count of active visit days per month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.LOGIN_FREQUENCY_PER_WEEK IS 'Average weekly login frequency.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.BEHAVIOURAL_EVENTS IS 'Total number of clickstream events recorded.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.BEHAVIOURAL_LAST_EVENT_TS IS 'Timestamp of the most recent behavioural event.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.IMPRESSION_UNITS IS 'Weighted impression volume accumulated from ad events.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.CLICK_UNITS IS 'Weighted click volume accumulated from ad events.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.NEGATIVE_EVENT_COUNT IS 'Lifetime count of negative QoE events (rebuffering, playback errors, cancellations).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_TOTAL IS 'Total watch time in seconds across all viewing sessions (lifetime).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_DAILY IS 'Watch time in seconds for the current day.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_WEEKLY IS 'Watch time in seconds for the current week.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_MONTHLY IS 'Watch time in seconds for the current month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_30 IS 'Watch time in seconds over the last 30 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_90 IS 'Watch time in seconds over the last 90 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_180 IS 'Watch time in seconds over the last 180 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_COUNT IS 'Total number of viewing sessions (lifetime).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_COUNT_DAILY IS 'Number of viewing sessions for the current day.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_COUNT_WEEKLY IS 'Number of viewing sessions for the current week.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_COUNT_MONTHLY IS 'Number of viewing sessions for the current month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_COUNT_30 IS 'Number of viewing sessions over the last 30 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_COUNT_90 IS 'Number of viewing sessions over the last 90 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_COUNT_180 IS 'Number of viewing sessions over the last 180 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_AVG_DURATION IS 'Average duration per viewing session in seconds (lifetime).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_AVG_DURATION_DAILY IS 'Average duration per viewing session in seconds for the current day.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_AVG_DURATION_WEEKLY IS 'Average duration per viewing session in seconds for the current week.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_AVG_DURATION_MONTHLY IS 'Average duration per viewing session in seconds for the current month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_AVG_DURATION_30 IS 'Average duration per viewing session in seconds over the last 30 days.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_AVG_DURATION_90 IS 'Average duration per viewing session in seconds over the last 90 days.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_SESSION_AVG_DURATION_180 IS 'Average duration per viewing session in seconds over the last 180 days.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_WEEKLY_AVG IS 'Average watch time per week in seconds (calculated as total watch time divided by number of active weeks).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_AVG_PER_ACTIVE_DAY IS 'Average watch time per active viewing day in seconds.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_LONGEST_SESSION_SECONDS IS 'Duration of the longest viewing session in seconds.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_COMPLETION_RATE IS 'Fraction of viewing sessions that were completed (completed_episode or finish_episode reason).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_BY_CONTENT_TYPE IS 'Array of objects showing watch time breakdown by content type (top 10).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_TIME_BY_DEVICE_TYPE IS 'Array of objects showing watch time breakdown by device type (top 10).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_PEAK_HOUR IS 'Hour of day (0-23) when the subscriber watches most content.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_PEAK_DAY_OF_WEEK IS 'Day of week (0=Sunday, 6=Saturday) when the subscriber watches most content.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.WATCH_BINGE_INDICATOR IS 'Binary indicator (1/0) for binge-watching behavior (3+ sessions per day or 2+ hours per day).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.MAU_COUNT IS 'Monthly Active Users: Count of distinct months where subscriber had any clickstream activity.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.MAV_COUNT IS 'Monthly Active Viewers: Count of distinct months where subscriber had viewing activity (PLAY_START or PLAY_STOP events).';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.MAU_CURRENT_MONTH IS 'Binary indicator (1/0) for whether subscriber was active in the current month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.MAV_CURRENT_MONTH IS 'Binary indicator (1/0) for whether subscriber had viewing activity in the current month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.LAST_ACTIVE_MONTH IS 'Most recent month (truncated date) where subscriber had any clickstream activity.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.LAST_VIEWING_MONTH IS 'Most recent month (truncated date) where subscriber had viewing activity.';
    COMMENT ON COLUMN FE_SUBSCRIBER_HISTORY.FIRST_SEEN_TS IS 'Earliest interaction timestamp across ad events and clickstream events.';

    --------------------------------------------------------------------------
    -- Wide feature table (general purpose signal repository)
    --------------------------------------------------------------------------
    CREATE OR REPLACE TABLE FE_SUBSCRIBER_FEATURES AS
    WITH history AS (
        SELECT * FROM FE_SUBSCRIBER_HISTORY
    ),
    ad_metrics AS (
        SELECT
            ae.unique_id,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' THEN ae.event_weight ELSE 0 END) AS impression_units,
            SUM(CASE WHEN ae.event_type = 'CLICK' THEN ae.event_weight ELSE 0 END) AS click_units,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' AND ae.event_date >= DATEADD(day,-30,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS impression_units_30,
            SUM(CASE WHEN ae.event_type = 'CLICK' AND ae.event_date >= DATEADD(day,-30,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS click_units_30,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' AND ae.event_date >= DATEADD(day,-90,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS impression_units_90,
            SUM(CASE WHEN ae.event_type = 'CLICK' AND ae.event_date >= DATEADD(day,-90,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS click_units_90,
            SUM(CASE WHEN ae.event_type = 'IMPRESSION' AND ae.event_date >= DATEADD(day,-180,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS impression_units_180,
            SUM(CASE WHEN ae.event_type = 'CLICK' AND ae.event_date >= DATEADD(day,-180,CURRENT_DATE()) THEN ae.event_weight ELSE 0 END) AS click_units_180,
            SUM(COALESCE(ap.effective_cpm, ap.booked_cpm, 0) * ae.event_weight / 1000) AS monetization_total,
            SUM(CASE WHEN ae.event_date >= DATEADD(day,-90,CURRENT_DATE()) THEN COALESCE(ap.effective_cpm, ap.booked_cpm, 0) * ae.event_weight / 1000 ELSE 0 END) AS monetization_90,
            SUM(CASE WHEN ae.event_date >= DATEADD(day,-180,CURRENT_DATE()) THEN COALESCE(ap.effective_cpm, ap.booked_cpm, 0) * ae.event_weight / 1000 ELSE 0 END) AS monetization_180
        FROM AME_AD_SALES_DEMO.INGEST.ADS_EVENTS ae
        LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG ap
          ON ap.campaign_id = ae.campaign_id
         AND ap.report_date = ae.event_date
        GROUP BY ae.unique_id
    )
    SELECT
        h.unique_id,
        h.profile_id,
        h.tier,
        h.persona,
        h.lad_code,
        h.area_name,
        h.age,
        h.age_band,
        h.income_level,
        h.education_level,
        h.marital_status,
        h.family_status,
        h.behavioral_digital_media_consumption_index,
        h.behavioral_fast_fashion_retail_propensity,
        h.behavioral_grocery_online_delivery_use,
        h.behavioral_financial_investment_interest,
        h.avg_site_visits_per_month,
        h.login_frequency_per_week,
        h.behavioural_events,
        h.clickstream_events,
        h.clickstream_active_days,
        h.distinct_event_types,
        h.impression_events,
        h.click_events,
        COALESCE(am.impression_units, 0) AS impression_units,
        COALESCE(am.click_units, 0) AS click_units,
        COALESCE(am.impression_units_30, 0) AS impression_units_30,
        COALESCE(am.click_units_30, 0) AS click_units_30,
        COALESCE(am.impression_units_90, 0) AS impression_units_90,
        COALESCE(am.click_units_90, 0) AS click_units_90,
        COALESCE(am.impression_units_180, 0) AS impression_units_180,
        COALESCE(am.click_units_180, 0) AS click_units_180,
        COALESCE(am.monetization_total, 0) AS monetization_total,
        COALESCE(am.monetization_90, 0) AS monetization_90,
        COALESCE(am.monetization_180, 0) AS monetization_180,
        DATEDIFF('day', h.behavioural_last_event_ts, CURRENT_DATE()) AS days_since_behaviour,
        DATEDIFF('day', h.last_ad_event_ts, CURRENT_DATE()) AS days_since_ad_engagement,
        DATEDIFF('day', h.clickstream_last_event_ts, CURRENT_DATE()) AS days_since_clickstream,
        CASE WHEN COALESCE(am.impression_units,0) > 0 THEN COALESCE(am.click_units,0) / am.impression_units ELSE 0 END AS ad_click_rate,
        CASE WHEN COALESCE(h.clickstream_active_days,0) > 0 THEN COALESCE(h.clickstream_events,0) / h.clickstream_active_days ELSE 0 END AS events_per_active_day,
        CASE WHEN COALESCE(h.login_frequency_per_week,0) > 0 THEN COALESCE(h.avg_site_visits_per_month,0) / (h.login_frequency_per_week * 4.0) ELSE NULL END AS visit_to_login_ratio,
        COALESCE(am.monetization_total, 0) AS attributed_spend,
        CASE WHEN COALESCE(am.monetization_total, 0) > 0 THEN LEAST(1.0, am.monetization_total / 100.0) ELSE 0 END AS monetization_index,
        ARRAY_TO_STRING(h.content_categories, ',') AS content_category_vector,
        COALESCE(h.negative_event_count, 0) AS negative_event_count,
        COALESCE(h.negative_event_count_30, 0) AS negative_event_count_30,
        CASE WHEN COALESCE(h.clickstream_events,0) > 0 THEN COALESCE(h.negative_event_count,0) / h.clickstream_events ELSE 0 END AS negative_event_ratio,
        -- Watch time metrics (key metrics from history)
        COALESCE(h.watch_time_total, 0) AS watch_time_total,
        COALESCE(h.watch_time_weekly_avg, 0) AS watch_time_weekly_avg,
        COALESCE(h.watch_time_30, 0) AS watch_time_30,
        COALESCE(h.watch_time_90, 0) AS watch_time_90,
        COALESCE(h.watch_time_180, 0) AS watch_time_180,
        COALESCE(h.watch_session_count, 0) AS watch_session_count,
        COALESCE(h.watch_session_count_30, 0) AS watch_session_count_30,
        COALESCE(h.watch_session_count_90, 0) AS watch_session_count_90,
        COALESCE(h.watch_session_count_180, 0) AS watch_session_count_180,
        COALESCE(h.watch_session_avg_duration, 0) AS watch_session_avg_duration,
        COALESCE(h.watch_session_avg_duration_30, 0) AS watch_session_avg_duration_30,
        COALESCE(h.watch_session_avg_duration_90, 0) AS watch_session_avg_duration_90,
        COALESCE(h.watch_session_avg_duration_180, 0) AS watch_session_avg_duration_180,
        COALESCE(h.watch_time_avg_per_active_day, 0) AS watch_time_avg_per_active_day,
        COALESCE(h.watch_longest_session_seconds, 0) AS watch_longest_session_seconds,
        COALESCE(h.watch_completion_rate, 0) AS watch_completion_rate,
        COALESCE(h.watch_binge_indicator, 0) AS watch_binge_indicator,
        -- MAU/MAV metrics
        COALESCE(h.mau_count, 0) AS mau_count,
        COALESCE(h.mav_count, 0) AS mav_count,
        COALESCE(h.mau_current_month, 0) AS mau_current_month,
        COALESCE(h.mav_current_month, 0) AS mav_current_month,
        h.behavioural_last_event_ts,
        h.last_ad_event_ts,
        h.clickstream_last_event_ts,
        h.first_seen_ts,
        CURRENT_TIMESTAMP() AS generated_ts
    FROM history h
    LEFT JOIN ad_metrics am ON am.unique_id = h.unique_id;

    COMMENT ON TABLE FE_SUBSCRIBER_FEATURES IS
        'Wide subscriber feature table capturing behaviour, engagement, monetisation, and demographic signals for downstream modelling.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.PERSONA IS 'Behaviourally inferred persona label.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.MONETIZATION_TOTAL IS 'Total attributed spend derived from ad event weights and CPM allocations.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.NEGATIVE_EVENT_RATIO IS 'Fraction of events that were negative QoE events over lifetime.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_TIME_TOTAL IS 'Total watch time in seconds across all viewing sessions (lifetime).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_TIME_WEEKLY_AVG IS 'Average watch time per week in seconds (calculated as total watch time divided by number of active weeks).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_TIME_30 IS 'Watch time in seconds over the last 30 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_TIME_90 IS 'Watch time in seconds over the last 90 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_TIME_180 IS 'Watch time in seconds over the last 180 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_COUNT IS 'Total number of viewing sessions (lifetime).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_COUNT_30 IS 'Number of viewing sessions over the last 30 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_COUNT_90 IS 'Number of viewing sessions over the last 90 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_COUNT_180 IS 'Number of viewing sessions over the last 180 days (rolling window).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_AVG_DURATION IS 'Average duration per viewing session in seconds (lifetime).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_AVG_DURATION_30 IS 'Average duration per viewing session in seconds over the last 30 days.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_AVG_DURATION_90 IS 'Average duration per viewing session in seconds over the last 90 days.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_SESSION_AVG_DURATION_180 IS 'Average duration per viewing session in seconds over the last 180 days.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_TIME_AVG_PER_ACTIVE_DAY IS 'Average watch time per active viewing day in seconds.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_LONGEST_SESSION_SECONDS IS 'Duration of the longest viewing session in seconds.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_COMPLETION_RATE IS 'Fraction of viewing sessions that were completed (completed_episode or finish_episode reason).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.WATCH_BINGE_INDICATOR IS 'Binary indicator (1/0) for binge-watching behavior (3+ sessions per day or 2+ hours per day).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.MAU_COUNT IS 'Monthly Active Users: Count of distinct months where subscriber had any clickstream activity.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.MAV_COUNT IS 'Monthly Active Viewers: Count of distinct months where subscriber had viewing activity (PLAY_START or PLAY_STOP events).';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.MAU_CURRENT_MONTH IS 'Binary indicator (1/0) for whether subscriber was active in the current month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.MAV_CURRENT_MONTH IS 'Binary indicator (1/0) for whether subscriber had viewing activity in the current month.';
    COMMENT ON COLUMN FE_SUBSCRIBER_FEATURES.FIRST_SEEN_TS IS 'Earliest interaction timestamp across ad events and clickstream events.';

    --------------------------------------------------------------------------
    -- Content attribution features
    --------------------------------------------------------------------------
    CREATE OR REPLACE TABLE FE_SUBSCRIBER_CONTENT_FEATURES AS
    WITH raw_affinity AS (
        SELECT
            se.unique_id,
            se.profile_id,
            LOWER(COALESCE(NULLIF(ce.content_category::STRING, ''),
                           ce.attributes:content_category::STRING,
                           ce.content_type::STRING,
                           ce.event_type)) AS content_key,
            COUNT(*) AS event_count,
            SUM(CASE WHEN ce.event_ts >= DATEADD(day,-30,CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS event_count_30,
            MAX(ce.event_ts) AS last_event_ts
        FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED se
        LEFT JOIN AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
          ON ce.unique_id = se.unique_id
        GROUP BY se.unique_id, se.profile_id, content_key
    ),
    ranked AS (
        SELECT
            ra.*,
            ROW_NUMBER() OVER (PARTITION BY ra.unique_id ORDER BY ra.event_count DESC NULLS LAST, ra.content_key) AS rn,
            SUM(ra.event_count) OVER (PARTITION BY ra.unique_id) AS total_event_count
        FROM raw_affinity ra
    ),
    primary_tag AS (
        SELECT unique_id, content_key, event_count, total_event_count
        FROM ranked
        WHERE rn = 1
    ),
    secondary_tag AS (
        SELECT unique_id, content_key, event_count
        FROM ranked
        WHERE rn = 2
    ),
    totals AS (
        SELECT
            unique_id,
            SUM(event_count) AS content_event_total,
            SUM(event_count_30) AS recent_event_count_30
        FROM raw_affinity
        GROUP BY unique_id
    )
    SELECT
        fa.unique_id,
        fa.profile_id,
        COALESCE(pt.content_key, 'unknown') AS primary_content_type,
        CASE WHEN COALESCE(pt.total_event_count, 0) > 0 THEN pt.event_count / pt.total_event_count ELSE NULL END AS primary_content_share,
        COALESCE(st.content_key, 'n/a') AS secondary_content_type,
        CASE WHEN COALESCE(pt.total_event_count, 0) > 0 THEN COALESCE(st.event_count,0) / pt.total_event_count ELSE NULL END AS secondary_content_share,
        COALESCE(tt.content_event_total, 0) AS content_event_total,
        COALESCE(tt.recent_event_count_30, 0) AS recent_event_count_30,
        COALESCE(ff.negative_event_ratio, 0) AS negative_event_rate,
        CURRENT_TIMESTAMP() AS generated_ts
    FROM raw_affinity fa
    LEFT JOIN primary_tag pt ON pt.unique_id = fa.unique_id
    LEFT JOIN secondary_tag st ON st.unique_id = fa.unique_id
    LEFT JOIN totals tt ON tt.unique_id = fa.unique_id
    LEFT JOIN FE_SUBSCRIBER_FEATURES ff ON ff.unique_id = fa.unique_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fa.unique_id ORDER BY fa.unique_id) = 1;

    COMMENT ON TABLE FE_SUBSCRIBER_CONTENT_FEATURES IS
        'Content affinity features capturing dominant content types and engagement share per subscriber.';
    COMMENT ON COLUMN FE_SUBSCRIBER_CONTENT_FEATURES.PRIMARY_CONTENT_TYPE IS 'Most engaged content category based on clickstream events.';
    COMMENT ON COLUMN FE_SUBSCRIBER_CONTENT_FEATURES.PRIMARY_CONTENT_SHARE IS 'Share of events attributed to the dominant content type.';
    COMMENT ON COLUMN FE_SUBSCRIBER_CONTENT_FEATURES.SECONDARY_CONTENT_TYPE IS 'Second most engaged content category (if available).';

    --------------------------------------------------------------------------
    -- Cluster feature set
    --------------------------------------------------------------------------
    CREATE OR REPLACE TABLE FE_SUBSCRIBER_CLUSTER_FEATURES AS
    WITH base AS (
        SELECT
            f.unique_id,
            f.profile_id,
            f.persona,
            f.tier,
            f.income_level,
            f.education_level,
            f.marital_status,
            f.family_status,
            COALESCE(cf.primary_content_type, 'unknown') AS primary_content_type,
            DENSE_RANK() OVER (ORDER BY f.tier) - 1 AS tier_index,
            DENSE_RANK() OVER (ORDER BY f.income_level) - 1 AS income_index,
            DENSE_RANK() OVER (ORDER BY f.education_level) - 1 AS education_index,
            DENSE_RANK() OVER (ORDER BY f.persona) - 1 AS persona_index,
            DENSE_RANK() OVER (ORDER BY f.family_status) - 1 AS family_index,
            COALESCE(f.behavioral_digital_media_consumption_index, 0) AS digital_media_index,
            COALESCE(f.behavioral_fast_fashion_retail_propensity, 0) AS fast_fashion_index,
            COALESCE(f.behavioral_grocery_online_delivery_use, 0) AS grocery_index,
            COALESCE(f.behavioral_financial_investment_interest, 0) AS financial_index,
            COALESCE(f.avg_site_visits_per_month, 0) AS avg_site_visits_per_month,
            COALESCE(f.login_frequency_per_week, 0) AS login_frequency_per_week,
            COALESCE(f.events_per_active_day, 0) AS events_per_active_day,
            COALESCE(f.ad_click_rate, 0) AS ad_click_rate,
            COALESCE(f.monetization_180, 0) AS monetization_180,
            COALESCE(cf.primary_content_share, 0) AS primary_content_share,
            COALESCE(cf.secondary_content_share, 0) AS secondary_content_share
        FROM FE_SUBSCRIBER_FEATURES f
        LEFT JOIN FE_SUBSCRIBER_CONTENT_FEATURES cf ON cf.unique_id = f.unique_id
    )
    SELECT
        *,
        CURRENT_TIMESTAMP() AS generated_ts
    FROM base;

    COMMENT ON TABLE FE_SUBSCRIBER_CLUSTER_FEATURES IS
        'Feature matrix prepared for unsupervised audience clustering (numeric encodings + propensity indices).';
    COMMENT ON COLUMN FE_SUBSCRIBER_CLUSTER_FEATURES.TIER_INDEX IS 'Dense-rank encoded tier for clustering algorithms.';
    COMMENT ON COLUMN FE_SUBSCRIBER_CLUSTER_FEATURES.PERSONA_INDEX IS 'Dense-rank encoded persona label to retain behavioural signal.';

    --------------------------------------------------------------------------
    -- Churn modelling feature set (with label)
    --------------------------------------------------------------------------
    CREATE OR REPLACE TABLE FE_SUBSCRIBER_CHURN_FEATURES AS
    WITH base AS (
        SELECT
            f.unique_id,
            f.profile_id,
            f.tier,
            DENSE_RANK() OVER (ORDER BY f.tier) - 1 AS tier_index,
            DENSE_RANK() OVER (ORDER BY f.income_level) - 1 AS income_index,
            COALESCE(f.behavioral_digital_media_consumption_index, 0) AS digital_media_index,
            COALESCE(f.avg_site_visits_per_month, 0) AS visit_frequency,
            COALESCE(f.login_frequency_per_week, 0) AS login_frequency,
            COALESCE(f.impression_units_30, 0) AS ad_impressions_30,
            COALESCE(f.click_units_30, 0) AS ad_clicks_30,
            COALESCE(f.negative_event_ratio, 0) AS negative_event_rate,
            COALESCE(f.monetization_90, 0) AS monetization_90,
            DATEDIFF('day', f.behavioural_last_event_ts, CURRENT_DATE()) AS recency_days,
            CASE WHEN DATEDIFF('day', f.behavioural_last_event_ts, CURRENT_DATE()) > 30 THEN 1 ELSE 0 END AS churn_label
        FROM FE_SUBSCRIBER_FEATURES f
    )
    SELECT
        *,
        CURRENT_TIMESTAMP() AS generated_ts
    FROM base;

    COMMENT ON TABLE FE_SUBSCRIBER_CHURN_FEATURES IS
        'Supervised learning dataset for churn, containing engineered predictors and binary churn label (recency > 30 days).';
    COMMENT ON COLUMN FE_SUBSCRIBER_CHURN_FEATURES.CHURN_LABEL IS
        'Binary target: 1 when subscriber inactive for >30 days, otherwise 0.';

    --------------------------------------------------------------------------
    -- LTV modelling feature set (with target)
    --------------------------------------------------------------------------
    CREATE OR REPLACE TABLE FE_SUBSCRIBER_LTV_FEATURES AS
    WITH base AS (
        SELECT
            f.unique_id,
            f.profile_id,
            f.tier,
            DENSE_RANK() OVER (ORDER BY f.tier) - 1 AS tier_index,
            DENSE_RANK() OVER (ORDER BY f.income_level) - 1 AS income_index,
            COALESCE(f.behavioral_digital_media_consumption_index, 0) AS digital_media_index,
            COALESCE(f.avg_site_visits_per_month, 0) AS avg_site_visits_per_month,
            COALESCE(f.login_frequency_per_week, 0) AS login_frequency_per_week,
            COALESCE(f.impression_units_180, 0) AS ad_impressions_180,
            COALESCE(f.click_units_180, 0) AS ad_clicks_180,
            COALESCE(f.monetization_180, 0) AS monetization_180,
            CASE WHEN COALESCE(f.impression_units_180,0) > 0 THEN COALESCE(f.click_units_180,0) / f.impression_units_180 ELSE 0 END AS engagement_ratio,
            -- Enhanced LTV target calculation:
            -- 1. Subscription value: tier-based monthly value * estimated months active (based on MAU)
            --    Premium: $15/month, Standard: $10/month, Ad-supported: $5/month
            -- 2. Ad monetization: existing monetization_180
            -- 3. Engagement value: watch time and session activity signals
            -- 4. Retention premium: MAV count indicates viewing engagement beyond base subscription
            COALESCE(
                -- Subscription value component (tier-based monthly value * months active)
                CASE 
                    WHEN f.tier = 'Premium' THEN 15.0 * GREATEST(COALESCE(f.mau_count, 0), 1)
                    WHEN f.tier = 'Standard' THEN 10.0 * GREATEST(COALESCE(f.mau_count, 0), 1)
                    WHEN f.tier = 'Ad-supported' THEN 5.0 * GREATEST(COALESCE(f.mau_count, 0), 1)
                    ELSE 5.0 * GREATEST(COALESCE(f.mau_count, 0), 1) -- Default to Ad-supported
                END
                +
                -- Ad monetization component (existing)
                COALESCE(f.monetization_180, 0)
                +
                -- Engagement value component (watch time signals)
                -- Normalize watch_time_180 (in seconds) to engagement value: 1 hour = $0.50 value
                LEAST(COALESCE(f.watch_time_180, 0) / 7200.0, 50.0) -- Cap at 100 hours = $50
                +
                -- Session activity component (more sessions = higher engagement = higher value)
                LEAST(COALESCE(f.watch_session_count, 0) * 0.25, 25.0) -- $0.25 per session, cap at $25
                +
                -- Retention premium (MAV count indicates viewing engagement beyond base subscription)
                COALESCE(f.mav_count, 0) * 1.0 -- $1 per viewing month (separate from subscription months)
                +
                -- Monetization index adjustment (existing logic, reduced weight)
                COALESCE(f.monetization_index, 0) * 25.0 -- Reduced from 100 to 25 since we have subscription value
            , 0) AS ltv_target
        FROM FE_SUBSCRIBER_FEATURES f
    )
    SELECT
        *,
        CURRENT_TIMESTAMP() AS generated_ts
    FROM base;

    COMMENT ON TABLE FE_SUBSCRIBER_LTV_FEATURES IS
        'Supervised learning dataset for subscriber LTV estimation with engineered predictors and synthetic continuous target.';
    COMMENT ON COLUMN FE_SUBSCRIBER_LTV_FEATURES.LTV_TARGET IS
        'Enhanced synthetic LTV proxy combining subscription value (tier * months active), ad monetization (180-day), engagement signals (watch time, sessions), and retention premium (MAV count). Ensures all subscribers have non-zero LTV based on tier and activity.';

    --------------------------------------------------------------------------
    -- Daily content views aggregation table
    --------------------------------------------------------------------------
    CREATE OR REPLACE TABLE FE_CONTENT_VIEWS_DAILY AS
    WITH play_events AS (
        SELECT
            ce.unique_id,
            DATE_TRUNC('DAY', ce.event_ts) AS view_date,
            ce.content_type,
            ce.content_category,
            ce.device,
            ce.event_type,
            ce.event_ts,
            ce.attributes,
            -- Find the most recent PLAY_START before each PLAY_STOP
            MAX(CASE WHEN ce.event_type = 'PLAY_START' THEN ce.event_ts END) 
                OVER (PARTITION BY ce.unique_id, COALESCE(ce.session_id, '') 
                      ORDER BY ce.event_ts 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_play_start_ts
        FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS ce
        WHERE ce.event_type IN ('PLAY_START', 'PLAY_STOP', 'CLICK_CONTENT', 'BROWSE_PAGE')
    ),
    view_sessions AS (
        SELECT
            pe.unique_id,
            pe.view_date,
            pe.content_type,
            pe.content_category,
            pe.device,
            pe.event_type,
            -- Calculate duration for PLAY_STOP events
            CASE 
                WHEN pe.event_type = 'PLAY_STOP' THEN
                    COALESCE(
                        pe.attributes:duration_seconds::FLOAT,
                        pe.attributes:duration::FLOAT,
                        CASE 
                            WHEN pe.last_play_start_ts IS NOT NULL 
                            THEN TIMESTAMPDIFF('second', pe.last_play_start_ts, pe.event_ts)
                            ELSE NULL
                        END
                    )
                ELSE NULL
            END AS duration_seconds
        FROM play_events pe
    ),
    daily_content_views AS (
        SELECT
            vs.view_date,
            vs.content_type,
            vs.content_category,
            vs.device,
            -- View counts
            COUNT_IF(vs.event_type = 'PLAY_START') AS play_start_count,
            COUNT_IF(vs.event_type = 'PLAY_STOP') AS play_stop_count,
            COUNT_IF(vs.event_type = 'CLICK_CONTENT') AS content_click_count,
            COUNT_IF(vs.event_type = 'BROWSE_PAGE') AS browse_page_count,
            COUNT(DISTINCT vs.unique_id) AS unique_viewers,
            COUNT(DISTINCT CASE WHEN vs.event_type IN ('PLAY_START', 'PLAY_STOP') THEN vs.unique_id END) AS unique_active_viewers,
            -- Watch time metrics
            SUM(COALESCE(vs.duration_seconds, 0)) AS total_watch_time_seconds,
            COUNT_IF(vs.event_type = 'PLAY_STOP' AND vs.duration_seconds IS NOT NULL) AS completed_sessions,
            AVG(CASE WHEN vs.event_type = 'PLAY_STOP' AND vs.duration_seconds IS NOT NULL THEN vs.duration_seconds END) AS avg_session_duration_seconds,
            MAX(CASE WHEN vs.event_type = 'PLAY_STOP' THEN vs.duration_seconds END) AS max_session_duration_seconds,
            -- Session metrics (count PLAY_START as sessions started, PLAY_STOP as sessions completed)
            COUNT_IF(vs.event_type = 'PLAY_START') AS total_sessions_started,
            COUNT_IF(vs.event_type = 'PLAY_STOP' AND vs.duration_seconds IS NOT NULL) AS total_sessions_completed
        FROM view_sessions vs
        GROUP BY vs.view_date, vs.content_type, vs.content_category, vs.device
    )
    SELECT
        dcv.view_date,
        COALESCE(dcv.content_type, 'unknown') AS content_type,
        COALESCE(dcv.content_category, 'unknown') AS content_category,
        COALESCE(dcv.device, 'unknown') AS device,
        dcv.play_start_count,
        dcv.play_stop_count,
        dcv.content_click_count,
        dcv.browse_page_count,
        dcv.unique_viewers,
        dcv.unique_active_viewers,
        dcv.total_watch_time_seconds,
        dcv.completed_sessions,
        COALESCE(dcv.avg_session_duration_seconds, 0) AS avg_session_duration_seconds,
        COALESCE(dcv.max_session_duration_seconds, 0) AS max_session_duration_seconds,
        dcv.total_sessions_started,
        dcv.total_sessions_completed,
        CASE 
            WHEN COALESCE(dcv.total_sessions_started, 0) > 0 
            THEN dcv.completed_sessions / dcv.total_sessions_started
            ELSE 0 
        END AS session_completion_rate,
        CASE 
            WHEN COALESCE(dcv.unique_viewers, 0) > 0 
            THEN dcv.total_watch_time_seconds / dcv.unique_viewers
            ELSE 0 
        END AS avg_watch_time_per_viewer_seconds,
        CURRENT_TIMESTAMP() AS generated_ts
    FROM daily_content_views dcv;

    COMMENT ON TABLE FE_CONTENT_VIEWS_DAILY IS
        'Daily aggregation of content viewing metrics by content type, category, and device. Supports content performance analysis and trend monitoring.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.VIEW_DATE IS 'Date (truncated to day) for the aggregated metrics.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.CONTENT_TYPE IS 'Type of content viewed (series, movie, live, short_form, etc.).';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.CONTENT_CATEGORY IS 'Content category/genre (Sports, Drama, Lifestyle, etc.).';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.DEVICE IS 'Device type used for viewing (Mobile, Smart TV, Web, Tablet).';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.PLAY_START_COUNT IS 'Total number of PLAY_START events for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.PLAY_STOP_COUNT IS 'Total number of PLAY_STOP events for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.CONTENT_CLICK_COUNT IS 'Total number of content click events for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.BROWSE_PAGE_COUNT IS 'Total number of browse page views for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.UNIQUE_VIEWERS IS 'Count of distinct subscribers who interacted with content on this day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.UNIQUE_ACTIVE_VIEWERS IS 'Count of distinct subscribers who had viewing activity (PLAY_START or PLAY_STOP) on this day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.TOTAL_WATCH_TIME_SECONDS IS 'Total watch time in seconds aggregated for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.COMPLETED_SESSIONS IS 'Number of completed viewing sessions (PLAY_STOP events with duration) for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.AVG_SESSION_DURATION_SECONDS IS 'Average duration per viewing session in seconds for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.MAX_SESSION_DURATION_SECONDS IS 'Maximum duration of any viewing session in seconds for the day/content/device combination.';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.TOTAL_SESSIONS_STARTED IS 'Total number of viewing sessions started (count of PLAY_START events).';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.TOTAL_SESSIONS_COMPLETED IS 'Total number of viewing sessions completed (count of PLAY_STOP events with duration).';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.SESSION_COMPLETION_RATE IS 'Fraction of started sessions that were completed (completed_sessions / total_sessions_started).';
    COMMENT ON COLUMN FE_CONTENT_VIEWS_DAILY.AVG_WATCH_TIME_PER_VIEWER_SECONDS IS 'Average watch time per unique viewer in seconds for the day/content/device combination.';

    RETURN OBJECT_CONSTRUCT(
        'procedure', 'FEATURE_ENGINEER_SUBSCRIBER_METRICS',
        'run_started', run_started,
        'run_completed', CURRENT_TIMESTAMP(),
        'history_rows', (SELECT COUNT(*) FROM FE_SUBSCRIBER_HISTORY),
        'feature_rows', (SELECT COUNT(*) FROM FE_SUBSCRIBER_FEATURES),
        'content_rows', (SELECT COUNT(*) FROM FE_SUBSCRIBER_CONTENT_FEATURES),
        'cluster_rows', (SELECT COUNT(*) FROM FE_SUBSCRIBER_CLUSTER_FEATURES),
        'churn_rows', (SELECT COUNT(*) FROM FE_SUBSCRIBER_CHURN_FEATURES),
        'ltv_rows', (SELECT COUNT(*) FROM FE_SUBSCRIBER_LTV_FEATURES),
        'content_views_daily_rows', (SELECT COUNT(*) FROM FE_CONTENT_VIEWS_DAILY)
    );
END;
$$;

/* Helper wrapper for orchestration */
CREATE OR REPLACE PROCEDURE REFRESH_SUBSCRIBER_FEATURES()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  result VARIANT;
BEGIN
  result := (CALL FEATURE_ENGINEER_SUBSCRIBER_METRICS());
  RETURN result;
END;
$$;

CALL REFRESH_SUBSCRIBER_FEATURES();

-- ---------------------------------------------------------------------------
-- 19-subscriber-ltv-scoring.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA ANALYSE;

/*
    Prototype linear regression model for subscriber LTV using Snowpark + NumPy.
    The procedure trains a simple linear model, stores coefficients for lineage,
    and materialises predicted LTV scores back into ANALYSE.FE_SUBSCRIBER_LTV_SCORES.
*/
CREATE OR REPLACE PROCEDURE ANALYSE.TRAIN_LINEAR_LTV_MODEL()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'numpy', 'pandas')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
import numpy as np
import pandas as pd

FEATURE_COLS = [
    "TIER_INDEX",
    "INCOME_INDEX",
    "DIGITAL_MEDIA_INDEX",
    "AVG_SITE_VISITS_PER_MONTH",
    "LOGIN_FREQUENCY_PER_WEEK",
    "AD_IMPRESSIONS_180",
    "AD_CLICKS_180",
    "MONETIZATION_180",
    "ENGAGEMENT_RATIO"
]
TARGET_COL = "LTV_TARGET"

def _prepare_dataframe(session: Session) -> pd.DataFrame:
    df = session.table("ANALYSE.FE_SUBSCRIBER_LTV_FEATURES")
    cols = ["UNIQUE_ID", "PROFILE_ID"] + FEATURE_COLS + [TARGET_COL]
    pdf = df.select(cols).to_pandas()
    if not pdf.empty:
        pdf = pdf.fillna(0.0)
    return pdf

def _resolve_table_identifiers(session: Session, table_name: str) -> tuple[str, str, str]:
    parts = [p.strip('"') for p in table_name.split('.') if p]
    if len(parts) == 1:
        db = session.get_current_database()
        schema = session.get_current_schema()
        if not db or not schema:
            raise ValueError("Current database/schema must be set when using unqualified table names")
        db = db.strip('"')
        schema = schema.strip('"')
        table = parts[0]
    elif len(parts) == 2:
        db = session.get_current_database()
        if not db:
            raise ValueError("Current database must be set when table name is schema.table")
        db = db.strip('"')
        schema, table = parts
    else:
        db, schema, table = parts[-3:]
    return db, schema, table

def _quote(identifier: str) -> str:
    return f'"{identifier}"'

def _write_table(session: Session, table_name: str, pdf: pd.DataFrame, schema: list):
    db, schema_name, table = _resolve_table_identifiers(session, table_name)
    full_name = '.'.join([_quote(db), _quote(schema_name), _quote(table)])
    session.sql(f"CREATE OR REPLACE TABLE {full_name} ({', '.join(schema)})").collect()
    session.sql(f"TRUNCATE TABLE {full_name}").collect()
    if not pdf.empty:
        session.write_pandas(
            pdf,
            table,
            database=db,
            schema=schema_name,
            auto_create_table=False,
            overwrite=False
        )

def run(session: Session):
    pdf = _prepare_dataframe(session)
    if pdf.empty:
        _write_table(
            session,
            "ANALYSE.FE_SUBSCRIBER_LTV_MODEL_COEFFS",
            pd.DataFrame(columns=["FEATURE_NAME", "COEFFICIENT"]),
            ["FEATURE_NAME STRING", "COEFFICIENT FLOAT"]
        )
        _write_table(
            session,
            "ANALYSE.FE_SUBSCRIBER_LTV_SCORES",
            pd.DataFrame(columns=["UNIQUE_ID", "PROFILE_ID", "LTV_TARGET", "PREDICTED_LTV", "RESIDUAL", "LTV_SEGMENT", "GENERATED_TS"]),
            ["UNIQUE_ID STRING", "PROFILE_ID STRING", "LTV_TARGET FLOAT", "PREDICTED_LTV FLOAT", "RESIDUAL FLOAT", "LTV_SEGMENT STRING", "GENERATED_TS TIMESTAMP_NTZ"]
        )
        return {"status": "empty_dataset", "row_count": 0}

    X = pdf[FEATURE_COLS].astype(float).to_numpy()
    y = pdf[TARGET_COL].astype(float).to_numpy()
    X = np.hstack([np.ones((X.shape[0], 1)), X])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)

    coeff_df = pd.DataFrame({
        "FEATURE_NAME": ["intercept"] + FEATURE_COLS,
        "COEFFICIENT": beta.tolist()
    })
    _write_table(
        session,
        "ANALYSE.FE_SUBSCRIBER_LTV_MODEL_COEFFS",
        coeff_df,
        ["FEATURE_NAME STRING", "COEFFICIENT FLOAT"]
    )

    preds = X @ beta
    pdf["PREDICTED_LTV"] = preds
    pdf["RESIDUAL"] = pdf[TARGET_COL] - pdf["PREDICTED_LTV"]
    pdf["LTV_SEGMENT"] = pd.cut(
        preds,
        bins=[-np.inf, 50, 150, np.inf],
        labels=["Low", "Medium", "High"]
    )
    pdf["GENERATED_TS"] = pd.Timestamp.utcnow()
    scores_df = pdf[["UNIQUE_ID", "PROFILE_ID", TARGET_COL, "PREDICTED_LTV", "RESIDUAL", "LTV_SEGMENT", "GENERATED_TS"]]
    _write_table(
        session,
        "ANALYSE.FE_SUBSCRIBER_LTV_SCORES",
        scores_df,
        ["UNIQUE_ID STRING", "PROFILE_ID STRING", "LTV_TARGET FLOAT", "PREDICTED_LTV FLOAT", "RESIDUAL FLOAT", "LTV_SEGMENT STRING", "GENERATED_TS TIMESTAMP_NTZ"]
    )

    return {
        "status": "trained_linear_model",
        "row_count": int(len(pdf)),
        "features": FEATURE_COLS,
        "coefficients": coeff_df.to_dict(orient="records")
    }
$$;

CREATE OR REPLACE TABLE ANALYSE.FE_SUBSCRIBER_LTV_MODEL_COEFFS (
    FEATURE_NAME STRING,
    COEFFICIENT FLOAT
);

CREATE OR REPLACE TABLE ANALYSE.FE_SUBSCRIBER_LTV_SCORES (
    UNIQUE_ID STRING,
    PROFILE_ID STRING,
    LTV_TARGET FLOAT,
    PREDICTED_LTV FLOAT,
    RESIDUAL FLOAT,
    LTV_SEGMENT STRING,
    GENERATED_TS TIMESTAMP_NTZ
);

/* Convenience wrapper for orchestration */
CREATE OR REPLACE PROCEDURE ANALYSE.REFRESH_SUBSCRIBER_LTV()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  result VARIANT;
BEGIN
  result := (CALL ANALYSE.TRAIN_LINEAR_LTV_MODEL());
  RETURN result;
END;
$$;

CALL ANALYSE.REFRESH_SUBSCRIBER_LTV();


COMMENT ON TABLE ANALYSE.FE_SUBSCRIBER_LTV_MODEL_COEFFS IS
    'Linear regression coefficients (intercept + feature weights) for the current LTV prototype model.';
COMMENT ON COLUMN ANALYSE.FE_SUBSCRIBER_LTV_MODEL_COEFFS.FEATURE_NAME IS 'Feature or intercept name.';
COMMENT ON COLUMN ANALYSE.FE_SUBSCRIBER_LTV_MODEL_COEFFS.COEFFICIENT IS 'Estimated coefficient from linear regression fit.';

COMMENT ON TABLE ANALYSE.FE_SUBSCRIBER_LTV_SCORES IS
    'Predicted LTV scores generated by TRAIN_LINEAR_LTV_MODEL(); stores target, prediction, residual, segment, and timestamp per subscriber.';
COMMENT ON COLUMN ANALYSE.FE_SUBSCRIBER_LTV_SCORES.PREDICTED_LTV IS 'Predicted LTV value from the current linear regression model.';
COMMENT ON COLUMN ANALYSE.FE_SUBSCRIBER_LTV_SCORES.RESIDUAL IS 'Difference between target LTV and predicted LTV (useful for diagnostics).';
COMMENT ON COLUMN ANALYSE.FE_SUBSCRIBER_LTV_SCORES.LTV_SEGMENT IS 'LTV segment classification: Low (<$50), Medium ($50-$150), High (>$150).';


-- ---------------------------------------------------------------------------
-- 20-subscriber-churn-scoring.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA ANALYSE;

/*
    Prototype linear regression churn model using Snowpark + NumPy.
    Produces coefficients table + churn probability per subscriber.
*/
CREATE OR REPLACE PROCEDURE ANALYSE.TRAIN_LINEAR_CHURN_MODEL()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'numpy', 'pandas')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
import numpy as np
import pandas as pd

FEATURE_COLS = [
    "TIER_INDEX",
    "INCOME_INDEX",
    "DIGITAL_MEDIA_INDEX",
    "VISIT_FREQUENCY",
    "LOGIN_FREQUENCY",
    "AD_IMPRESSIONS_30",
    "AD_CLICKS_30",
    "NEGATIVE_EVENT_RATE",
    "RECENCY_DAYS",
    "MONETIZATION_90"
]
TARGET_COL = "CHURN_LABEL"

def _prepare_dataframe(session: Session) -> pd.DataFrame:
    df = session.table("ANALYSE.FE_SUBSCRIBER_CHURN_FEATURES")
    cols = ["UNIQUE_ID", "PROFILE_ID"] + FEATURE_COLS + [TARGET_COL]
    pdf = df.select(cols).to_pandas()
    if not pdf.empty:
        pdf = pdf.fillna(0.0)
    return pdf

def _resolve_table_identifiers(session: Session, table_name: str) -> tuple[str, str, str]:
    parts = [p.strip('"') for p in table_name.split('.') if p]
    if len(parts) == 1:
        db = session.get_current_database()
        schema = session.get_current_schema()
        if not db or not schema:
            raise ValueError("Current database/schema must be set when using unqualified table names")
        db = db.strip('"')
        schema = schema.strip('"')
        table = parts[0]
    elif len(parts) == 2:
        db = session.get_current_database()
        if not db:
            raise ValueError("Current database must be set when table name is schema.table")
        db = db.strip('"')
        schema, table = parts
    else:
        db, schema, table = parts[-3:]
    return db, schema, table

def _quote(identifier: str) -> str:
    return f'"{identifier}"'

def _write_table(session: Session, table_name: str, pdf: pd.DataFrame, schema: list):
    db, schema_name, table = _resolve_table_identifiers(session, table_name)
    full_name = '.'.join([_quote(db), _quote(schema_name), _quote(table)])
    session.sql(f"CREATE OR REPLACE TABLE {full_name} ({', '.join(schema)})").collect()
    session.sql(f"TRUNCATE TABLE {full_name}").collect()
    if not pdf.empty:
        session.write_pandas(
            pdf,
            table,
            database=db,
            schema=schema_name,
            auto_create_table=False,
            overwrite=False
        )

def run(session: Session):
    pdf = _prepare_dataframe(session)
    if pdf.empty:
        _write_table(
            session,
            "ANALYSE.FE_SUBSCRIBER_CHURN_MODEL_COEFFS",
            pd.DataFrame(columns=["FEATURE_NAME", "COEFFICIENT"]),
            ["FEATURE_NAME STRING", "COEFFICIENT FLOAT"]
        )
        _write_table(
            session,
            "ANALYSE.FE_SUBSCRIBER_CHURN_RISK",
            pd.DataFrame(columns=["UNIQUE_ID", "PROFILE_ID", "CHURN_LABEL", "MODEL_SCORE", "PREDICTED_CHURN_PROB", "CHURN_RISK_SEGMENT", "GENERATED_TS"]),
            ["UNIQUE_ID STRING", "PROFILE_ID STRING", "CHURN_LABEL FLOAT", "MODEL_SCORE FLOAT", "PREDICTED_CHURN_PROB FLOAT", "CHURN_RISK_SEGMENT STRING", "GENERATED_TS TIMESTAMP_NTZ"]
        )
        return {"status": "empty_dataset", "row_count": 0}

    X = pdf[FEATURE_COLS].astype(float).to_numpy()
    y = pdf[TARGET_COL].astype(float).to_numpy()
    X = np.hstack([np.ones((X.shape[0], 1)), X])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)

    coeff_df = pd.DataFrame({
        "FEATURE_NAME": ["intercept"] + FEATURE_COLS,
        "COEFFICIENT": beta.tolist()
    })
    _write_table(
        session,
        "ANALYSE.FE_SUBSCRIBER_CHURN_MODEL_COEFFS",
        coeff_df,
        ["FEATURE_NAME STRING", "COEFFICIENT FLOAT"]
    )

    linear_scores = X @ beta
    probs = 1 / (1 + np.exp(-linear_scores))
    pdf["MODEL_SCORE"] = linear_scores
    pdf["PREDICTED_CHURN_PROB"] = probs
    pdf["CHURN_RISK_SEGMENT"] = pd.cut(
        probs,
        bins=[-np.inf, 0.4, 0.7, np.inf],
        labels=["Low", "Medium", "High"]
    )
    pdf["GENERATED_TS"] = pd.Timestamp.utcnow()
    out_df = pdf[["UNIQUE_ID", "PROFILE_ID", TARGET_COL, "MODEL_SCORE", "PREDICTED_CHURN_PROB", "CHURN_RISK_SEGMENT", "GENERATED_TS"]]
    _write_table(
        session,
        "ANALYSE.FE_SUBSCRIBER_CHURN_RISK",
        out_df,
        ["UNIQUE_ID STRING", "PROFILE_ID STRING", "CHURN_LABEL FLOAT", "MODEL_SCORE FLOAT", "PREDICTED_CHURN_PROB FLOAT", "CHURN_RISK_SEGMENT STRING", "GENERATED_TS TIMESTAMP_NTZ"]
    )

    return {
        "status": "trained_linear_model",
        "row_count": int(len(pdf)),
        "features": FEATURE_COLS,
        "coefficients": coeff_df.to_dict(orient="records")
    }
$$;

CREATE OR REPLACE TABLE ANALYSE.FE_SUBSCRIBER_CHURN_MODEL_COEFFS (
    FEATURE_NAME STRING,
    COEFFICIENT FLOAT
);

CREATE OR REPLACE TABLE ANALYSE.FE_SUBSCRIBER_CHURN_RISK (
    UNIQUE_ID STRING,
    PROFILE_ID STRING,
    CHURN_LABEL FLOAT,
    MODEL_SCORE FLOAT,
    PREDICTED_CHURN_PROB FLOAT,
    CHURN_RISK_SEGMENT STRING,
    GENERATED_TS TIMESTAMP_NTZ
);

COMMENT ON TABLE ANALYSE.FE_SUBSCRIBER_CHURN_MODEL_COEFFS IS
    'Linear regression coefficients (intercept + feature weights) for the churn-risk prototype model.';
COMMENT ON COLUMN ANALYSE.FE_SUBSCRIBER_CHURN_MODEL_COEFFS.COEFFICIENT IS 'Estimated coefficient capturing direction and magnitude of churn impact.';

COMMENT ON TABLE ANALYSE.FE_SUBSCRIBER_CHURN_RISK IS
    'Predicted churn probabilities generated by TRAIN_LINEAR_CHURN_MODEL(). Includes model score, probability, and segment banding.';
COMMENT ON COLUMN ANALYSE.FE_SUBSCRIBER_CHURN_RISK.PREDICTED_CHURN_PROB IS 'Sigmoid-transformed probability estimate from the linear regression model.';

CREATE OR REPLACE PROCEDURE ANALYSE.REFRESH_SUBSCRIBER_CHURN_RISK()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  result VARIANT;
BEGIN
  result := (CALL ANALYSE.TRAIN_LINEAR_CHURN_MODEL());
  RETURN result;
END;
$$;

CALL ANALYSE.REFRESH_SUBSCRIBER_CHURN_RISK();


-- ---------------------------------------------------------------------------
-- 21-create-clean-room-tool.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA DCR_ANALYSIS;

-- --------------------------------------------------------------------------
-- Mock Data Clean Room Overlap Stored Procedure for Cortex Analyst Tooling
-- --------------------------------------------------------------------------

CREATE OR REPLACE TABLE DCR_ANALYSIS.PARTNER_X_CUSTOMERS AS
SELECT
    spe.unique_id,
    spe.email,
    SHA2(LOWER(TRIM(spe.email)), 256) AS hashed_email
FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
SAMPLE (45) SEED (42);

CREATE OR REPLACE PROCEDURE DCR_ANALYSIS.RUN_OVERLAP_QUERY(target_segment_sql_filter STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session


def run(session: Session, target_segment_sql_filter: str):
    trimmed_filter = (target_segment_sql_filter or '').strip()
    where_clause = f" WHERE {trimmed_filter}" if trimmed_filter else ''

    segment_query = (
        f"""
        WITH SUBSCRIBER_BEHAVIOR AS (
            SELECT *, SHA2(LOWER(TRIM(spe.email)), 256) AS hashed_email
                FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
                INNER JOIN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS bl
                ON spe.UNIQUE_ID = bl.UNIQUE_ID
            {where_clause}
            )
        SELECT COUNT(DISTINCT s.hashed_email) AS cnt
        FROM SUBSCRIBER_BEHAVIOR s
        """
    )
    total_segment = session.sql(segment_query).collect()[0].CNT

    overlap_query = f"""
        WITH SUBSCRIBER_BEHAVIOR AS (
            SELECT *, SHA2(LOWER(TRIM(spe.email)), 256) AS hashed_email
                FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
                INNER JOIN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS bl
                ON spe.UNIQUE_ID = bl.UNIQUE_ID
            {where_clause}
            )
        SELECT COUNT(DISTINCT s.hashed_email) AS cnt
        FROM SUBSCRIBER_BEHAVIOR s
        JOIN DCR_ANALYSIS.PARTNER_X_CUSTOMERS p
          ON s.hashed_email = p.hashed_email
    """
    overlap_count = session.sql(overlap_query).collect()[0].CNT

    total_addressable = int(-(-(overlap_count + max(total_segment - overlap_count, 0) * 0.35) // 1))

    message = (
        "The overlap between our refined segment and the client's list is "
        f"{overlap_count:,.0f} unique IDs, giving us a total addressable campaign size of "
        f"{total_addressable:,.0f}."
    )

    return {
        "segment_filter": target_segment_sql_filter,
        "segment_size": total_segment,
        "overlap_unique_ids": overlap_count,
        "total_addressable_size": total_addressable,
        "message": message,
    }
$$;

COMMENT ON PROCEDURE DCR_ANALYSIS.RUN_OVERLAP_QUERY(STRING) IS 'Tool Name: RUN_CLEAN_ROOM_OVERLAP | Description: Executes a secure audience overlap analysis against a designated partner Data Clean Room (DCR) to determine the size of the combined, addressable audience.';

USE SCHEMA DCR_ANALYSIS;

CREATE OR REPLACE PROCEDURE AME_AD_SALES_DEMO.ACTIVATION.ACTIVATE_AUDIENCE(
    channel_name STRING,
    audience_payload STRING,
    activation_notes STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from datetime import datetime
from snowflake.snowpark import Session


SUPPORTED_CHANNELS = {
    'GOOGLE ADS': 'OpenFlow_GoogleAds_Connector',
    'AMAZON ADS': 'OpenFlow_AmazonAds_Connector',
    'LINKEDIN ADS': 'OpenFlow_LinkedInAds_Connector',
    'META ADS': 'OpenFlow_MetaAds_Connector',
}


def _resolve_channel(channel: str) -> str:
    if not channel:
        raise ValueError('Channel name must be provided.')
    normalized = channel.strip().upper()
    if normalized not in SUPPORTED_CHANNELS:
        raise ValueError(
            f"Unsupported activation channel '{channel}'. Supported channels: "
            + ', '.join(SUPPORTED_CHANNELS.keys())
        )
    return normalized


def run(session: Session, channel_name: str, audience_payload, activation_notes: str):
    normalized_channel = _resolve_channel(channel_name)
    connector_name = SUPPORTED_CHANNELS[normalized_channel]

    run_id = f"ACT_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"

    mock_request = {
        'connector': connector_name,
        'channel': normalized_channel.title(),
        'audience_payload': audience_payload,
        'activation_notes': activation_notes,
    }

    mock_response = {
        'status': 'QUEUED',
        'external_job_id': f"JOB_{datetime.utcnow().strftime('%H%M%S%f')}",
        'estimated_completion_minutes': 5,
    }

    return {
        'run_id': run_id,
        'channel': normalized_channel.title(),
        'connector_invoked': connector_name,
        'request_payload': mock_request,
        'connector_response': mock_response,
        'message': (
            f"Activation request for {normalized_channel.title()} has been queued via "
            f"{connector_name}. Reference ID: {mock_response['external_job_id']}"
        ),
    }
$$;

-- ---------------------------------------------------------------------------
-- 23-segment-stored-procedures.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA ANALYSE;

-- Helper: default AS_OF_DATE from latest clickstream event
CREATE OR REPLACE VIEW ANALYSE.V_DEFAULT_AS_OF_DATE AS
SELECT DATE_TRUNC('DAY', MAX(event_ts)) AS AS_OF_DATE
FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS;

-- Unified subscriber view CTE (features + churn + ltv + behavioral + profile)
-- NOTE: This shape is reproduced inside SPs to allow dynamic predicates
-- Column prefixes to disambiguate: f_, cr_, ltv_, bl_, spe_

-- Preview segment against unified view
CREATE OR REPLACE PROCEDURE ANALYSE.SEGMENT_PREVIEW(predicate STRING, limit_rows INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session
import pandas as pd

UNIFIED_SELECT = """
WITH base AS (
  SELECT
    f.UNIQUE_ID,
    -- Aliases for common, agent-friendly names
    f.TIER AS tier,
    f.PERSONA AS persona,
    f.PREDICTED_LTV AS predicted_ltv,
    cr.PREDICTED_CHURN_PROB AS predicted_churn_prob,
    -- Prefixed columns for full filtering power
    f.* AS f_*,
    cr.* AS cr_*,
    ltv.* AS ltv_*,
    bl.* AS bl_*,
    spe.* AS spe_*
  FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_FEATURES f
  LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CHURN_RISK cr
    ON cr.UNIQUE_ID = f.UNIQUE_ID
  LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_LTV_SCORES ltv
    ON ltv.UNIQUE_ID = f.UNIQUE_ID
  LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS bl
    ON bl.UNIQUE_ID = f.UNIQUE_ID
  LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
    ON spe.UNIQUE_ID = f.UNIQUE_ID
)
SELECT *
FROM base
WHERE {predicate}
LIMIT {limit_rows}
"""

def _predicate_or_true(pred: str|None) -> str:
    if pred is None or not str(pred).strip():
        return "TRUE"
    return pred

def run(session: Session, predicate: str, limit_rows: int):
    pred = _predicate_or_true(predicate)
    limit_n = int(limit_rows or 1000)
    sql = UNIFIED_SELECT.format(predicate=pred, limit_rows=limit_n)
    df = session.sql(sql).to_pandas()
    size = int(len(df))
    tier_dist = df['TIER'].value_counts(dropna=False).reset_index().rename(columns={'index':'TIER','TIER':'COUNT'}).to_dict(orient='records') if 'TIER' in df.columns else []
    persona_dist = df['PERSONA'].value_counts(dropna=False).reset_index().rename(columns={'index':'PERSONA','PERSONA':'COUNT'}).to_dict(orient='records') if 'PERSONA' in df.columns else []
    sample_rows = df.head(min(50, size)).to_dict(orient='records')
    return {
        "count": size,
        "tier_distribution": tier_dist,
        "persona_distribution": persona_dist,
        "sample": sample_rows
    }
$$;

-- Create/Upsert segment definition
CREATE OR REPLACE PROCEDURE ANALYSE.SEGMENT_CREATE(name STRING, predicate STRING, tags ARRAY, is_temporary BOOLEAN, comment_text STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session
import json

def run(session: Session, name: str, predicate: str, tags, is_temporary, comment_text):
    seg_id = session.sql("SELECT UUID_STRING() AS ID").collect()[0]['ID']
    pred = predicate or ""
    # Build criteria_json with metadata for temporary/comment using json.dumps to avoid escaping issues
    metadata = {
        "temporary": bool(is_temporary) if is_temporary is not None else False,
        "comment": comment_text or ""
    }
    metadata_json = json.dumps(metadata)
    # Upsert by name: delete existing with same name
    session.sql(f"DELETE FROM AME_AD_SALES_DEMO.APPS.SEGMENT_DEFINITIONS WHERE NAME = %s", params=[name]).collect()
    session.sql("""
        INSERT INTO AME_AD_SALES_DEMO.APPS.SEGMENT_DEFINITIONS (SEGMENT_ID, NAME, OWNER, TAGS, CRITERIA_JSON, SQL_PREDICATE)
        SELECT %s, %s, CURRENT_USER(), ARRAY_CONSTRUCT(), PARSE_JSON(%s), %s
    """, params=[seg_id, name, metadata_json, pred]).collect()
    return {"segment_id": seg_id, "name": name, "temporary": (is_temporary is True), "comment": comment_text or ""}
$$;

-- Delete a segment and related artifacts
CREATE OR REPLACE PROCEDURE ANALYSE.SEGMENT_DELETE(segment_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session

def run(session: Session, segment_id: str):
    session.sql("DELETE FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_MEMBERS WHERE SEGMENT_ID = %s", params=[segment_id]).collect()
    session.sql("DELETE FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_METRICS WHERE SEGMENT_ID = %s", params=[segment_id]).collect()
    session.sql("DELETE FROM AME_AD_SALES_DEMO.APPS.SEGMENT_DEFINITIONS WHERE SEGMENT_ID = %s", params=[segment_id]).collect()
    return {"segment_id": segment_id, "deleted": True}
$$;

-- List segments with counts
CREATE OR REPLACE PROCEDURE ANALYSE.SEGMENT_LIST()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session

def run(session: Session):
    q = """
      SELECT d.SEGMENT_ID, d.NAME, d.OWNER, d.TAGS,
             COALESCE(m.cnt_members,0) AS members_snapshots,
             COALESCE(mt.cnt_metrics,0) AS metrics_snapshots,
             TRY_TO_BOOLEAN(d.CRITERIA_JSON:temporary) AS temporary,
             TRY_TO_VARCHAR(d.CRITERIA_JSON:comment) AS comment
      FROM AME_AD_SALES_DEMO.APPS.SEGMENT_DEFINITIONS d
      LEFT JOIN (
         SELECT SEGMENT_ID, COUNT(*) AS cnt_members
         FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_MEMBERS
         GROUP BY 1
      ) m ON m.SEGMENT_ID = d.SEGMENT_ID
      LEFT JOIN (
         SELECT SEGMENT_ID, COUNT(*) AS cnt_metrics
         FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_METRICS
         GROUP BY 1
      ) mt ON mt.SEGMENT_ID = d.SEGMENT_ID
      ORDER BY d.NAME
    """
    rows = session.sql(q).collect()
    return [r.asDict() for r in rows]
$$;

-- Materialize a segment members snapshot
CREATE OR REPLACE PROCEDURE ANALYSE.SEGMENT_MATERIALIZE(segment_id STRING, as_of_date DATE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session

UNIFIED_BASE = """
WITH base AS (
  SELECT
    f.UNIQUE_ID,
    f.TIER,
    f.PERSONA,
    cr.PREDICTED_CHURN_PROB,
    ltv.PREDICTED_LTV,
    f.WATCH_TIME_30,
    f.WATCH_TIME_90,
    f.WATCH_TIME_180,
    f.MAU_COUNT,
    f.MAV_COUNT
  FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_FEATURES f
  LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CHURN_RISK cr
    ON cr.UNIQUE_ID = f.UNIQUE_ID
  LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_LTV_SCORES ltv
    ON ltv.UNIQUE_ID = f.UNIQUE_ID
  LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS bl
    ON bl.UNIQUE_ID = f.UNIQUE_ID
  LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
    ON spe.UNIQUE_ID = f.UNIQUE_ID
)
SELECT UNIQUE_ID
FROM base
WHERE {predicate}
"""

def _resolve_as_of(session: Session, as_of_date):
    if as_of_date is not None:
        return as_of_date
    df = session.table("ANALYSE.V_DEFAULT_AS_OF_DATE").collect()
    return df[0]['AS_OF_DATE'] if df else None

def _get_predicate(session: Session, segment_id: str) -> str:
    rows = session.sql("SELECT SQL_PREDICATE FROM AME_AD_SALES_DEMO.APPS.SEGMENT_DEFINITIONS WHERE SEGMENT_ID = %s", params=[segment_id]).collect()
    return rows[0]['SQL_PREDICATE'] if rows else ""

def run(session: Session, segment_id: str, as_of_date):
    snap_date = _resolve_as_of(session, as_of_date)
    predicate = _get_predicate(session, segment_id) or "TRUE"
    sql = UNIFIED_BASE.format(predicate=predicate)
    ids = session.sql(sql).select("UNIQUE_ID")
    session.sql("DELETE FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_MEMBERS WHERE SEGMENT_ID = %s AND AS_OF_DATE = %s", params=[segment_id, str(snap_date)]).collect()
    session.sql(f"""
        INSERT INTO AME_AD_SALES_DEMO.ANALYSE.SEGMENT_MEMBERS (SEGMENT_ID, UNIQUE_ID, AS_OF_DATE)
        SELECT %s, UNIQUE_ID, %s::DATE FROM ({sql})
    """, params=[segment_id, str(snap_date)]).collect()
    inserted = session.sql("SELECT COUNT(*) AS C FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_MEMBERS WHERE SEGMENT_ID = %s AND AS_OF_DATE = %s", params=[segment_id, str(snap_date)]).collect()[0]['C']
    return {"segment_id": segment_id, "as_of_date": str(snap_date), "members": int(inserted)}
$$;

-- Compute/overwrite metrics for a segment snapshot
CREATE OR REPLACE PROCEDURE ANALYSE.SEGMENT_METRICS(segment_id STRING, as_of_date DATE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session
import pandas as pd

def run(session: Session, segment_id: str, as_of_date):
    rows = session.sql("""
      SELECT sm.UNIQUE_ID
      FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_MEMBERS sm
      WHERE sm.SEGMENT_ID = %s AND sm.AS_OF_DATE = %s
    """, params=[segment_id, str(as_of_date)]).count()
    if rows == 0:
        return {"segment_id": segment_id, "as_of_date": str(as_of_date), "metrics_written": 0, "note": "no members"}

    df = session.sql(f"""
      SELECT
        f.UNIQUE_ID,
        f.PREDICTED_LTV,
        f.WATCH_TIME_30,
        f.WATCH_TIME_90,
        f.WATCH_TIME_180,
        f.MAU_COUNT,
        f.MAV_COUNT
      FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_MEMBERS sm
      JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_FEATURES f
        ON f.UNIQUE_ID = sm.UNIQUE_ID
      WHERE sm.SEGMENT_ID = '{segment_id}' AND sm.AS_OF_DATE = '{as_of_date}'
    """)
    size = df.count()
    agg = df.agg({
        "PREDICTED_LTV":"avg",
        "WATCH_TIME_30":"avg",
        "WATCH_TIME_90":"avg",
        "WATCH_TIME_180":"avg",
        "MAU_COUNT":"avg",
        "MAV_COUNT":"avg"
    }).to_pandas().iloc[0].to_dict()
    session.sql("""
        DELETE FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_METRICS
        WHERE SEGMENT_ID = %s AND AS_OF_DATE = %s
    """, params=[segment_id, str(as_of_date)]).collect()
    session.sql(f"""
        INSERT INTO AME_AD_SALES_DEMO.ANALYSE.SEGMENT_METRICS (
          SEGMENT_ID, AS_OF_DATE, SEGMENT_SIZE, AVG_PREDICTED_LTV,
          AVG_CHURN_PROB, AVG_WATCH_TIME_30, AVG_WATCH_TIME_90, AVG_WATCH_TIME_180,
          MAU_RATE, MAV_RATE
        )
        SELECT
          '{segment_id}', '{as_of_date}'::DATE, {size},
          {agg.get('AVG(PREDICTED_LTV)', 'NULL')},
          NULL,
          {agg.get('AVG(WATCH_TIME_30)', 'NULL')},
          {agg.get('AVG(WATCH_TIME_90)', 'NULL')},
          {agg.get('AVG(WATCH_TIME_180)', 'NULL')},
          {agg.get('AVG(MAU_COUNT)', 'NULL')},
          {agg.get('AVG(MAV_COUNT)', 'NULL')}
    """).collect()
    return {"segment_id": segment_id, "as_of_date": str(as_of_date), "metrics_written": 1}
$$;

-- Compare two segments at the same snapshot date
CREATE OR REPLACE PROCEDURE ANALYSE.SEGMENT_COMPARE(segment_id_a STRING, segment_id_b STRING, as_of_date DATE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session

def run(session: Session, segment_id_a: str, segment_id_b: str, as_of_date):
    sql = f"""
      SELECT
        a.SEGMENT_ID AS SEGMENT_ID_A,
        b.SEGMENT_ID AS SEGMENT_ID_B,
        a.AS_OF_DATE,
        a.SEGMENT_SIZE - b.SEGMENT_SIZE AS SIZE_DELTA,
        a.AVG_PREDICTED_LTV - b.AVG_PREDICTED_LTV AS LTV_DELTA,
        a.AVG_WATCH_TIME_30 - b.AVG_WATCH_TIME_30 AS WATCH_TIME_30_DELTA
      FROM AME_AD_SALES_DEMO.ANALYSE.SEGMENT_METRICS a
      JOIN AME_AD_SALES_DEMO.ANALYSE.SEGMENT_METRICS b
        ON a.AS_OF_DATE = b.AS_OF_DATE
       AND a.SEGMENT_ID = '{segment_id_a}'
       AND b.SEGMENT_ID = '{segment_id_b}'
      WHERE a.AS_OF_DATE = '{as_of_date}'
    """
    rows = session.sql(sql).collect()
    return {} if not rows else {k: rows[0][k] for k in rows[0].asDict()}
$$;

-- Grants for agent/MCP roles (adjust role names as needed)
GRANT USAGE ON SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE AME_AD_SALES_DEMO_ADMIN;
GRANT USAGE ON SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE ACCOUNTADMIN;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE AME_AD_SALES_DEMO_ADMIN;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE ACCOUNTADMIN;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE AME_AD_SALES_DEMO_ADMIN;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE ACCOUNTADMIN;




-- ---------------------------------------------------------------------------
-- 24-subscriber-attributes-dynamic-table.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA ANALYSE;

-- Dynamic table combining subscriber features with predicted churn risk and LTV
CREATE OR REPLACE DYNAMIC TABLE AME_AD_SALES_DEMO.ANALYSE.SUBSCRIBER_ATTRIBUTES
  WAREHOUSE = APP_WH
  TARGET_LAG = 'DOWNSTREAM'
AS
SELECT
  f.*,
  /* Predicted churn probability from churn risk table */
  cr.PREDICTED_CHURN_PROB AS PREDICTED_CHURN_PROB,
  /* Predicted LTV from scores table (aliased to avoid name collision with any existing f.PREDICTED_LTV) */
  ltv.PREDICTED_LTV AS PREDICTED_LTV_MODEL,
  /* Optional: expose churn label and LTV target for convenience */
  cr.CHURN_LABEL AS CHURN_LABEL,
  ltv.LTV_TARGET AS LTV_TARGET_GROUND_TRUTH
FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_FEATURES f
LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CHURN_RISK cr
  ON cr.UNIQUE_ID = f.UNIQUE_ID
LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_LTV_SCORES ltv
  ON ltv.UNIQUE_ID = f.UNIQUE_ID;




-- =============================================================================
-- CREATE TABLE LINEAGE VIEWS (required for Dataset Explorer)
-- =============================================================================
-- NOTE: These views must be created by ACCOUNTADMIN because they reference
-- SNOWFLAKE.ACCOUNT_USAGE views. Grant privileges to create in APPS schema.
USE ROLE ACCOUNTADMIN;
USE DATABASE AME_AD_SALES_DEMO;

-- Grant ACCOUNTADMIN ability to create views in APPS schema (owned by AME_AD_SALES_DEMO_ADMIN)
GRANT CREATE VIEW ON SCHEMA AME_AD_SALES_DEMO.APPS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA AME_AD_SALES_DEMO.APPS TO ROLE ACCOUNTADMIN;

USE SCHEMA APPS;

CREATE OR REPLACE SECURE VIEW ACCOUNT_USAGE_QUERY_HISTORY_VW
AS
SELECT
    T.VALUE:objectName::STRING AS target_table_name,
    T.VALUE:objectId::INT AS target_table_id,
    S.VALUE:objectName::STRING AS source_table_name,
    S.VALUE:objectId::INT AS source_table_id,
    A.QUERY_ID,
    A.QUERY_START_TIME,
    A.USER_NAME
FROM
    SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY AS A,
    LATERAL FLATTEN(A.OBJECTS_MODIFIED) AS T,
    LATERAL FLATTEN(A.BASE_OBJECTS_ACCESSED) AS S
WHERE 
    STARTSWITH(T.VALUE:objectName::STRING, current_database())
    AND S.VALUE:objectName::STRING IS NOT NULL;

GRANT SELECT ON VIEW ACCOUNT_USAGE_QUERY_HISTORY_VW TO ROLE AME_AD_SALES_DEMO_ADMIN;

CREATE OR REPLACE SECURE VIEW ACCOUNT_USAGE_CREATE_TABLE_AS_SELECT_VW AS
WITH latest_relationships AS (
SELECT 
    A.TARGET_TABLE_NAME,
    A.SOURCE_TABLE_NAME,
    Q.QUERY_TYPE
FROM ACCOUNT_USAGE_QUERY_HISTORY_VW A
INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
    ON Q.QUERY_ID = A.QUERY_ID
WHERE Q.QUERY_TYPE = 'CREATE_TABLE_AS_SELECT'
QUALIFY ROW_NUMBER() OVER (PARTITION BY target_table_name, source_table_name ORDER BY query_start_time DESC) = 1
)
SELECT
    R.TARGET_TABLE_NAME,
    ARRAY_AGG(R.SOURCE_TABLE_NAME) AS SOURCE_TABLES
FROM latest_relationships AS R
GROUP BY ALL
ORDER BY target_table_name;

GRANT SELECT ON VIEW ACCOUNT_USAGE_CREATE_TABLE_AS_SELECT_VW TO ROLE AME_AD_SALES_DEMO_ADMIN;

-- =============================================================================
-- DEPLOY STREAMLIT APPS FROM GIT REPOSITORY
-- =============================================================================

-- API Integration requires ACCOUNTADMIN privileges
USE ROLE ACCOUNTADMIN;

-- Create API integration for GitHub (public repo - no secrets needed)
CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs/')
    ENABLED = TRUE;

-- Grant usage on the API integration to the demo admin role
GRANT USAGE ON INTEGRATION GITHUB_API_INTEGRATION TO ROLE AME_AD_SALES_DEMO_ADMIN;

-- Switch back to demo admin role for remaining objects
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;

-- APPS schema already created and owned by AME_AD_SALES_DEMO_ADMIN (see line ~70)

-- Create Git repository for Streamlit app deployment
CREATE OR REPLACE GIT REPOSITORY AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO
    API_INTEGRATION = GITHUB_API_INTEGRATION
    ORIGIN = 'https://github.com/Snowflake-Labs/sfguide-mea-subscriber-analytics.git';

-- Fetch latest from Git repo
ALTER GIT REPOSITORY AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO FETCH;

-- -----------------------------------------------------------------------------
-- INGEST EXPLORER
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS AME_AD_SALES_DEMO.APPS.STAGE_INGEST_EXPLORER;

COPY FILES INTO @AME_AD_SALES_DEMO.APPS.STAGE_INGEST_EXPLORER
    FROM @AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO/branches/main/streamlit/
    FILES = ('streamlit_ingest_explorer.py', 'environment.yml');

CREATE OR REPLACE STREAMLIT AME_AD_SALES_DEMO.APPS.INGEST_EXPLORER
    FROM @AME_AD_SALES_DEMO.APPS.STAGE_INGEST_EXPLORER
    MAIN_FILE = 'streamlit_ingest_explorer.py'
    QUERY_WAREHOUSE = 'APP_WH'
    COMMENT = '{"origin":"sf_sit-is","name":"agentic-audience-analytics","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"streamlit"}}';

-- -----------------------------------------------------------------------------
-- DATASET EXPLORER
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS AME_AD_SALES_DEMO.APPS.STAGE_DATASET_EXPLORER;

COPY FILES INTO @AME_AD_SALES_DEMO.APPS.STAGE_DATASET_EXPLORER
    FROM @AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO/branches/main/streamlit/
    FILES = ('streamlit_dataset_explorer.py', 'environment.yml');

CREATE OR REPLACE STREAMLIT AME_AD_SALES_DEMO.APPS.DATASET_EXPLORER
    FROM @AME_AD_SALES_DEMO.APPS.STAGE_DATASET_EXPLORER
    MAIN_FILE = 'streamlit_dataset_explorer.py'
    QUERY_WAREHOUSE = 'APP_WH'
    COMMENT = '{"origin":"sf_sit-is","name":"agentic-audience-analytics","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"streamlit"}}';

-- -----------------------------------------------------------------------------
-- DASHBOARD
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS AME_AD_SALES_DEMO.APPS.STAGE_DASHBOARD;

COPY FILES INTO @AME_AD_SALES_DEMO.APPS.STAGE_DASHBOARD
    FROM @AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO/branches/main/streamlit/
    FILES = ('streamlit_dashboard.py', 'environment.yml');

CREATE OR REPLACE STREAMLIT AME_AD_SALES_DEMO.APPS.DASHBOARD
    FROM @AME_AD_SALES_DEMO.APPS.STAGE_DASHBOARD
    MAIN_FILE = 'streamlit_dashboard.py'
    QUERY_WAREHOUSE = 'APP_WH'
    COMMENT = '{"origin":"sf_sit-is","name":"agentic-audience-analytics","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"streamlit"}}';

-- -----------------------------------------------------------------------------
-- SEGMENT BUILDER
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS AME_AD_SALES_DEMO.APPS.STAGE_SEGMENT_BUILDER;

COPY FILES INTO @AME_AD_SALES_DEMO.APPS.STAGE_SEGMENT_BUILDER
    FROM @AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO/branches/main/streamlit/
    FILES = ('streamlit_segment_builder.py', 'environment.yml');

CREATE OR REPLACE STREAMLIT AME_AD_SALES_DEMO.APPS.SEGMENT_BUILDER
    FROM @AME_AD_SALES_DEMO.APPS.STAGE_SEGMENT_BUILDER
    MAIN_FILE = 'streamlit_segment_builder.py'
    QUERY_WAREHOUSE = 'APP_WH'
    COMMENT = '{"origin":"sf_sit-is","name":"agentic-audience-analytics","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"streamlit"}}';

-- Grant access to all Streamlit apps
GRANT USAGE ON SCHEMA AME_AD_SALES_DEMO.APPS TO ROLE AME_AD_SALES_DEMO_ADMIN;
GRANT USAGE ON ALL STREAMLITS IN SCHEMA AME_AD_SALES_DEMO.APPS TO ROLE AME_AD_SALES_DEMO_ADMIN;
GRANT ALL ON ALL STAGES IN SCHEMA AME_AD_SALES_DEMO.APPS TO ROLE AME_AD_SALES_DEMO_ADMIN;

-- =============================================================================
-- SNOWFLAKE INTELLIGENCE: CREATE SEMANTIC VIEW
-- =============================================================================
-- Creates a semantic view from the YAML file to enable Snowflake Intelligence
-- natural language querying. Users can access this via Snowsight Intelligence.
-- The semantic view name is defined in the YAML file as AME_AD_SALES_SEMANTIC_VIEW.

USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA ANALYSE;

-- Copy semantic view YAML from Git repository to stage (if not already uploaded via PUT)
-- For HOL deployments, the file is uploaded via PUT before this runs
-- For manual runs, this copies from the Git repository
COPY FILES
    INTO @AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS/
    FROM @AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO/branches/main/scripts/semantic_view.yaml;

-- Refresh the stage directory to ensure the file is visible
ALTER STAGE AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS REFRESH;

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
    'AME_AD_SALES_DEMO.ANALYSE',
    (SELECT LISTAGG($1, '\n') WITHIN GROUP (ORDER BY METADATA$FILE_ROW_NUMBER) 
     FROM @AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS/semantic_view.yaml)
);

GRANT SELECT ON SEMANTIC VIEW AME_AD_SALES_DEMO.ANALYSE.AME_AD_SALES_SEMANTIC_VIEW 
TO ROLE AME_AD_SALES_DEMO_ADMIN;

-- =============================================================================
-- CORTEX AI: CREATE SUBSCRIBER RECOMMENDATIONS UDF
-- =============================================================================
-- Creates a UDF that uses CORTEX.COMPLETE to generate personalized recommendations
-- for subscribers based on their profile, engagement, and risk data.

CREATE OR REPLACE FUNCTION AME_AD_SALES_DEMO.ANALYSE.GET_SUBSCRIBER_RECOMMENDATIONS(
    subscriber_profile_id VARCHAR,
    recommendation_type VARCHAR DEFAULT 'all'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        CONCAT(
            'You are a personalized recommendation assistant for a media streaming platform. ',
            'Based on the subscriber data below, provide actionable recommendations. ',
            'Format your response as a clear, bulleted list with specific actions. ',
            'Be concise but insightful.\n\n',
            'SUBSCRIBER DATA:\n',
            (SELECT CONCAT(
                'Profile ID: ', f.PROFILE_ID, '\n',
                'Tier: ', f.TIER, '\n',
                'Content Preference: ', COALESCE(cf.PRIMARY_CONTENT_TYPE, 'Unknown'), '\n',
                'Age: ', COALESCE(f.AGE::VARCHAR, 'Unknown'), '\n',
                'Income Level: ', COALESCE(f.INCOME_LEVEL, 'Unknown'), '\n',
                'Monthly Site Visits: ', ROUND(f.AVG_SITE_VISITS_PER_MONTH, 1), '\n',
                'Login Frequency/Week: ', ROUND(f.LOGIN_FREQUENCY_PER_WEEK, 1), '\n',
                'Watch Time (30d): ', ROUND(f.WATCH_TIME_30/3600, 1), ' hours\n',
                'Secondary Content: ', COALESCE(cf.SECONDARY_CONTENT_TYPE, 'None'), '\n',
                'Content Categories: ', COALESCE(f.CONTENT_CATEGORY_VECTOR, 'None'), '\n',
                'Watch Completion Rate: ', ROUND(COALESCE(f.WATCH_COMPLETION_RATE, 0) * 100, 1), '%\n',
                'Binge Watcher: ', CASE WHEN f.WATCH_BINGE_INDICATOR = 1 THEN 'Yes' ELSE 'No' END, '\n',
                'Negative Events (30d): ', COALESCE(f.NEGATIVE_EVENT_COUNT_30::VARCHAR, '0'), '\n',
                'Churn Risk: ', COALESCE(cr.CHURN_RISK_SEGMENT, 'Unknown'), '\n',
                'Churn Probability: ', ROUND(COALESCE(cr.PREDICTED_CHURN_PROB, 0) * 100, 1), '%\n',
                'Predicted LTV: $', ROUND(COALESCE(ltv.PREDICTED_LTV, 0), 2), '\n',
                'LTV Segment: ', COALESCE(ltv.LTV_SEGMENT, 'Unknown')
            )
            FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_FEATURES f
            LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CHURN_RISK cr ON f.UNIQUE_ID = cr.UNIQUE_ID
            LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_LTV_SCORES ltv ON f.UNIQUE_ID = ltv.UNIQUE_ID
            LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CONTENT_FEATURES cf ON f.UNIQUE_ID = cf.UNIQUE_ID
            WHERE f.PROFILE_ID = subscriber_profile_id
            LIMIT 1),
            '\n\nRECOMMENDATION TYPE: ', recommendation_type, '\n',
            CASE 
                WHEN recommendation_type = 'retention' THEN 
                    'Focus on: Strategies to reduce churn risk and improve engagement.'
                WHEN recommendation_type = 'upsell' THEN 
                    'Focus on: Opportunities to upgrade tier or increase spend.'
                WHEN recommendation_type = 'content' THEN 
                    'Focus on: Content recommendations based on viewing patterns.'
                WHEN recommendation_type = 'engagement' THEN 
                    'Focus on: Ways to increase platform engagement and watch time.'
                ELSE 
                    'Provide comprehensive recommendations covering retention, engagement, and growth opportunities.'
            END
        )
    )
$$;

COMMENT ON FUNCTION AME_AD_SALES_DEMO.ANALYSE.GET_SUBSCRIBER_RECOMMENDATIONS(VARCHAR, VARCHAR)
IS 'Generates AI-powered personalized recommendations for subscribers using Cortex LLM. 
    Parameters: subscriber_profile_id (e.g., SUB-000000001), recommendation_type (all, retention, upsell, content, engagement)';

-- =============================================================================
-- SNOWFLAKE INTELLIGENCE: CREATE AGENT
-- =============================================================================
-- Creates the Subscriber Analytics Agent in SNOWFLAKE_INTELLIGENCE database
-- following the standard Snowflake Intelligence pattern. This agent enables 
-- natural language querying of subscriber data via Snowsight Intelligence.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL SEMANTIC VIEWS IN SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE ACCOUNTADMIN;

CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.SUBSCRIBER_ANALYTICS_AGENT
WITH PROFILE='{"display_name":"Subscriber Analytics AI Agent"}'
COMMENT='AI agent for media subscriber analytics - ask questions about subscriber demographics, churn risk, lifetime value, ad performance, and content engagement patterns'
FROM SPECIFICATION $$
{
  "models": {"orchestration": ""},
  "instructions": {
    "response": "You are a media analytics assistant. Provide insights with charts. Use bar charts for comparisons, line charts for trends. Key metrics: Churn risk threshold <10%, LTV segments (High/Medium/Low), CTR benchmark >0.4%.",
    "orchestration": "Use PROFILE_ID for subscriber lookups (format: SUB-XXXXXXXXX). Join FE_SUBSCRIBER_FEATURES with FE_SUBSCRIBER_CHURN_RISK and FE_SUBSCRIBER_LTV_SCORES using UNIQUE_ID. When users ask about recent, last month, last quarter, or other relative time references, interpret these relative to the most recent data available rather than the current calendar date. Use the analytics tool for data queries. Use the recommendations tool when users ask for personalized recommendations for a specific subscriber.",
    "sample_questions": [
      {"question":"How many subscribers are at high churn risk?"},
      {"question":"What is the average LTV by subscriber tier?"},
      {"question":"Show top 10 campaigns by CTR"},
      {"question":"Which content categories have highest engagement?"},
      {"question":"Compare impressions across verticals"},
      {"question":"What is the churn risk distribution by subscriber tier?"}
    ]
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "subscriber_analytics",
        "description": "Query subscriber demographics, engagement metrics, churn risk predictions, lifetime value scores, ad performance data, and content viewing patterns"
      }
    },
    {
      "tool_spec": {
        "type": "generic",
        "name": "get_recommendations",
        "description": "Generate personalized retention, upsell, content, or engagement recommendations for a specific subscriber based on their profile, churn risk, and lifetime value",
        "input_schema": {
          "type": "object",
          "properties": {
            "subscriber_profile_id": {"type": "string", "description": "The subscriber profile ID (format: SUB-XXXXXXXXX)"},
            "recommendation_type": {"type": "string", "description": "Type of recommendation: retention, upsell, content, engagement, or all", "default": "all"}
          },
          "required": ["subscriber_profile_id"]
        }
      }
    }
  ],
  "tool_resources": {
    "subscriber_analytics": {
      "semantic_view": "AME_AD_SALES_DEMO.ANALYSE.AME_AD_SALES_SEMANTIC_VIEW",
      "execution_environment": {
        "type": "warehouse",
        "warehouse": "APP_WH"
      }
    },
    "get_recommendations": {
      "type": "function",
      "identifier": "AME_AD_SALES_DEMO.ANALYSE.GET_SUBSCRIBER_RECOMMENDATIONS",
      "execution_environment": {
        "type": "warehouse",
        "warehouse": "APP_WH"
      }
    }
  }
}
$$;

-- Grant access to the custom role and Cortex capabilities
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.SUBSCRIBER_ANALYTICS_AGENT TO ROLE AME_AD_SALES_DEMO_ADMIN;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AME_AD_SALES_DEMO_ADMIN;

SHOW AGENTS IN SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS;

-- =============================================================================
-- DAILY DATA GENERATOR
-- =============================================================================
-- Keeps demo data fresh by generating new rows for time-series tables.
-- Fills any gap between the last data date and today so the demo never goes stale.
-- Three layers of freshness:
--   1. Initial CALL at deploy time (fills gap from S3 data through today)
--   2. Serverless task (daily, covers Snowflake Intelligence and idle periods)
--   3. Streamlit startup hooks (instant freshness on app open)
-- Idempotent — skips dates that already have data.
-- =============================================================================

USE SCHEMA AME_AD_SALES_DEMO.ANALYSE;

CREATE OR REPLACE PROCEDURE AME_AD_SALES_DEMO.ANALYSE.GENERATE_DAILY_DATA()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    content_max_date DATE;
    ad_max_date DATE;
    days_to_fill_content INTEGER;
    days_to_fill_ad INTEGER;
    content_rows_inserted INTEGER DEFAULT 0;
    ad_rows_inserted INTEGER DEFAULT 0;
BEGIN
    content_max_date := (SELECT MAX(VIEW_DATE)::DATE FROM AME_AD_SALES_DEMO.ANALYSE.FE_CONTENT_VIEWS_DAILY);
    ad_max_date := (SELECT MAX(REPORT_DATE) FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG);

    days_to_fill_content := DATEDIFF('day', :content_max_date, CURRENT_DATE());
    days_to_fill_ad := DATEDIFF('day', :ad_max_date, CURRENT_DATE());

    IF (days_to_fill_content <= 0 AND days_to_fill_ad <= 0) THEN
        RETURN 'No gap to fill. Content data through ' || :content_max_date || ', Ad data through ' || :ad_max_date;
    END IF;

    -- =========================================================================
    -- 1. FE_CONTENT_VIEWS_DAILY — 21 rows per day
    -- =========================================================================
    IF (days_to_fill_content > 0) THEN
        INSERT INTO AME_AD_SALES_DEMO.ANALYSE.FE_CONTENT_VIEWS_DAILY
        WITH template AS (
            SELECT * FROM AME_AD_SALES_DEMO.ANALYSE.FE_CONTENT_VIEWS_DAILY
            WHERE VIEW_DATE::DATE = (SELECT MAX(VIEW_DATE)::DATE FROM AME_AD_SALES_DEMO.ANALYSE.FE_CONTENT_VIEWS_DAILY)
        ),
        date_series AS (
            SELECT DATEADD('day', ROW_NUMBER() OVER (ORDER BY SEQ4()), :content_max_date) AS gen_date
            FROM TABLE(GENERATOR(ROWCOUNT => :days_to_fill_content))
        ),
        noised AS (
            SELECT
                d.gen_date::TIMESTAMP_NTZ AS VIEW_DATE,
                t.CONTENT_TYPE,
                t.CONTENT_CATEGORY,
                t.DEVICE,
                GREATEST(0, ROUND(t.PLAY_START_COUNT * UNIFORM(0.85, 1.15, RANDOM())))::INT AS PLAY_START_COUNT,
                GREATEST(0, ROUND(t.PLAY_STOP_COUNT * UNIFORM(0.85, 1.15, RANDOM())))::INT AS PLAY_STOP_COUNT,
                GREATEST(0, ROUND(t.CONTENT_CLICK_COUNT * UNIFORM(0.85, 1.15, RANDOM())))::INT AS CONTENT_CLICK_COUNT,
                GREATEST(0, ROUND(t.BROWSE_PAGE_COUNT * UNIFORM(0.85, 1.15, RANDOM())))::INT AS BROWSE_PAGE_COUNT,
                GREATEST(0, ROUND(t.UNIQUE_VIEWERS * UNIFORM(0.92, 1.08, RANDOM())))::INT AS UNIQUE_VIEWERS,
                GREATEST(0, ROUND(t.UNIQUE_ACTIVE_VIEWERS * UNIFORM(0.92, 1.08, RANDOM())))::INT AS UNIQUE_ACTIVE_VIEWERS,
                GREATEST(0, t.TOTAL_WATCH_TIME_SECONDS * UNIFORM(0.85, 1.15, RANDOM())) AS TOTAL_WATCH_TIME_SECONDS,
                GREATEST(0, ROUND(t.COMPLETED_SESSIONS * UNIFORM(0.85, 1.15, RANDOM())))::INT AS COMPLETED_SESSIONS,
                GREATEST(0, t.AVG_SESSION_DURATION_SECONDS * UNIFORM(0.90, 1.10, RANDOM())) AS AVG_SESSION_DURATION_SECONDS,
                GREATEST(0, t.MAX_SESSION_DURATION_SECONDS * UNIFORM(0.90, 1.10, RANDOM())) AS MAX_SESSION_DURATION_SECONDS,
                GREATEST(0, ROUND(t.TOTAL_SESSIONS_STARTED * UNIFORM(0.85, 1.15, RANDOM())))::INT AS TOTAL_SESSIONS_STARTED,
                GREATEST(0, ROUND(t.TOTAL_SESSIONS_COMPLETED * UNIFORM(0.85, 1.15, RANDOM())))::INT AS TOTAL_SESSIONS_COMPLETED
            FROM date_series d
            CROSS JOIN template t
        )
        SELECT
            VIEW_DATE,
            CONTENT_TYPE,
            CONTENT_CATEGORY,
            DEVICE,
            PLAY_START_COUNT,
            PLAY_STOP_COUNT,
            CONTENT_CLICK_COUNT,
            BROWSE_PAGE_COUNT,
            UNIQUE_VIEWERS,
            UNIQUE_ACTIVE_VIEWERS,
            TOTAL_WATCH_TIME_SECONDS,
            COMPLETED_SESSIONS,
            AVG_SESSION_DURATION_SECONDS,
            MAX_SESSION_DURATION_SECONDS,
            TOTAL_SESSIONS_STARTED,
            TOTAL_SESSIONS_COMPLETED,
            CASE WHEN TOTAL_SESSIONS_STARTED > 0
                 THEN ROUND(TOTAL_SESSIONS_COMPLETED::FLOAT / TOTAL_SESSIONS_STARTED, 6)
                 ELSE 0 END AS SESSION_COMPLETION_RATE,
            CASE WHEN UNIQUE_VIEWERS > 0
                 THEN ROUND(TOTAL_WATCH_TIME_SECONDS / UNIQUE_VIEWERS, 3)
                 ELSE 0 END AS AVG_WATCH_TIME_PER_VIEWER_SECONDS,
            CURRENT_TIMESTAMP() AS GENERATED_TS
        FROM noised;

        content_rows_inserted := (days_to_fill_content * 21);
    END IF;

    -- =========================================================================
    -- 2. AD_PERFORMANCE_DAILY_AGG — 2-3 campaigns per day
    -- =========================================================================
    IF (days_to_fill_ad > 0) THEN
        INSERT INTO AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG
        WITH campaigns AS (
            SELECT 'AD_0106' AS CAMPAIGN_ID, 'Northwind Insurance' AS ADVERTISER_NAME, 'Finance' AS VERTICAL,
                   'Family Entertainment' AS CONTENT_CATEGORY, 'CPM' AS RATE_TYPE,
                   580 AS BASE_IMPRESSIONS, 6 AS BASE_CLICKS, 17.41 AS BOOKED_CPM, 0.0106 AS BOOKED_CTR,
                   621 AS DAILY_CAP, 4 AS CREATIVE_COUNT,
                   ARRAY_CONSTRUCT('PREMIUM_DRAMA','FAMILY_HOUSEHOLD','SPORTS_ENGAGED','LIFESTYLE_MINDED','REALITY_FAN','KNOWLEDGE_SEEKER') AS TARGET_PERSONAS,
                   ARRAY_CONSTRUCT('Tablet','Smart TV') AS TARGET_DEVICES
            UNION ALL
            SELECT 'AD_0200', 'Glow Cosmetics', 'Beauty',
                   'Sports Enthusiasts', 'CPM',
                   1350, 17, 12.00, 0.0125,
                   1353, 3,
                   ARRAY_CONSTRUCT('LIFESTYLE_MINDED','SPORTS_ENGAGED','REALITY_FAN'),
                   ARRAY_CONSTRUCT('Mobile','Smart TV','Web')
            UNION ALL
            SELECT 'AD_0201', 'Falcon Motors', 'Auto',
                   'Premium Drama', 'Hybrid',
                   1100, 12, 17.41, 0.0106,
                   1112, 2,
                   ARRAY_CONSTRUCT('PREMIUM_DRAMA','SPORTS_ENGAGED','KNOWLEDGE_SEEKER'),
                   ARRAY_CONSTRUCT('Smart TV','Web')
        ),
        date_series AS (
            SELECT DATEADD('day', ROW_NUMBER() OVER (ORDER BY SEQ4()), :ad_max_date) AS gen_date
            FROM TABLE(GENERATOR(ROWCOUNT => :days_to_fill_ad))
        )
        SELECT
            d.gen_date AS REPORT_DATE,
            c.CAMPAIGN_ID,
            c.ADVERTISER_NAME,
            c.VERTICAL,
            c.CONTENT_CATEGORY,
            c.RATE_TYPE,
            GREATEST(1, ROUND(c.BASE_IMPRESSIONS * UNIFORM(0.80, 1.20, RANDOM()))) AS IMPRESSIONS,
            GREATEST(1, ROUND(c.BASE_CLICKS * UNIFORM(0.70, 1.30, RANDOM()))) AS CLICKS,
            ROUND(GREATEST(1, ROUND(c.BASE_CLICKS * UNIFORM(0.70, 1.30, RANDOM()))) /
                  NULLIF(GREATEST(1, ROUND(c.BASE_IMPRESSIONS * UNIFORM(0.80, 1.20, RANDOM()))), 0), 6) AS CTR,
            ROUND(GREATEST(1, ROUND(c.BASE_IMPRESSIONS * UNIFORM(0.80, 1.20, RANDOM()))) * c.BOOKED_CPM / 1000 * UNIFORM(0.90, 1.10, RANDOM()), 4) AS SPEND,
            ROUND(c.BOOKED_CPM * UNIFORM(0.90, 1.10, RANDOM()), 4) AS EFFECTIVE_CPM,
            c.BOOKED_CPM,
            c.BOOKED_CTR,
            c.DAILY_CAP,
            c.CREATIVE_COUNT,
            c.TARGET_PERSONAS,
            c.TARGET_DEVICES,
            GREATEST(100, ROUND(c.BASE_IMPRESSIONS * 0.55 * UNIFORM(0.85, 1.15, RANDOM())))::INT AS OBSERVED_UNIQUE_SUBSCRIBERS,
            CURRENT_TIMESTAMP() AS GENERATED_TS
        FROM date_series d
        CROSS JOIN campaigns c;

        ad_rows_inserted := (days_to_fill_ad * 3);
    END IF;

    RETURN 'Generated ' || :content_rows_inserted || ' content view rows and ' || :ad_rows_inserted || ' ad performance rows through ' || CURRENT_DATE();
END;
$$;

-- Serverless task: runs daily at 6am UTC to keep data fresh for Snowflake Intelligence
-- Auto-suspends after 3 consecutive failures. Near-zero cost (no-op when no gap exists).
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE AME_AD_SALES_DEMO_ADMIN;

USE ROLE AME_AD_SALES_DEMO_ADMIN;

CREATE OR REPLACE TASK AME_AD_SALES_DEMO.ANALYSE.DAILY_DATA_REFRESH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  AS CALL AME_AD_SALES_DEMO.ANALYSE.GENERATE_DAILY_DATA();

ALTER TASK AME_AD_SALES_DEMO.ANALYSE.DAILY_DATA_REFRESH RESUME;

-- Initial call at deploy time to fill any gap from S3 data through today
CALL AME_AD_SALES_DEMO.ANALYSE.GENERATE_DAILY_DATA();

-- Resize warehouse back to X-SMALL after setup
ALTER WAREHOUSE APP_WH SET WAREHOUSE_SIZE = 'X-SMALL';

-- =============================================================================
-- =============================================================================
-- TEARDOWN SCRIPT (COMMENTED OUT)
-- =============================================================================
-- =============================================================================
-- Uncomment and run the following statements to remove ALL objects created by
-- this Snowflake Guide. This will permanently delete all data!
--
-- WARNING: This action is IRREVERSIBLE. All data will be lost.
-- =============================================================================

/*
-- =============================================
-- TEARDOWN: Remove all Snowflake Guide objects
-- =============================================

USE ROLE ACCOUNTADMIN;

-- 1. Suspend and drop the daily data refresh task
ALTER TASK IF EXISTS AME_AD_SALES_DEMO.ANALYSE.DAILY_DATA_REFRESH SUSPEND;
DROP TASK IF EXISTS AME_AD_SALES_DEMO.ANALYSE.DAILY_DATA_REFRESH;

-- 2. Drop the database (cascades to all schemas, tables, views, stages, 
--    Streamlit apps, functions, procedures, git repos, secrets inside)
DROP DATABASE IF EXISTS AME_AD_SALES_DEMO CASCADE;

-- 3. Drop the API integration (must be done separately, not in database)
DROP INTEGRATION IF EXISTS GITHUB_API_INTEGRATION;

-- 4. Drop the warehouse
DROP WAREHOUSE IF EXISTS APP_WH;

-- 5. Drop the custom role (do this last)
DROP ROLE IF EXISTS AME_AD_SALES_DEMO_ADMIN;

-- Verify cleanup
SHOW DATABASES LIKE 'AME_AD_SALES_DEMO';
SHOW WAREHOUSES LIKE 'APP_WH';
SHOW ROLES LIKE 'AME_AD_SALES_DEMO_ADMIN';
SHOW INTEGRATIONS LIKE 'GITHUB_API_INTEGRATION';

-- Expected: All queries should return 0 rows

*/
