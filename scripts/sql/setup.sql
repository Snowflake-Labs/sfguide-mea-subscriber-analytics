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

-- ============================================================================
-- DATA VOLUME CONFIGURATION (REDUCED FOR FASTER EXECUTION)
-- ============================================================================
-- Current values are reduced for HOL/testing (~15-20 min execution).
-- To restore FULL production data volume, change these values throughout:
--
--   Parameter                          Current    Original
--   --------------------------------   -------    --------
--   TOTAL_SUBSCRIBERS                  100000     781638
--   MONTHS_BACK                        6          24
--   ROLLING_MONTH_WINDOW               6          24
--   ADS_MAX_IMPRESSION_EVENTS          1000       8000
--   ADS_MAX_CLICK_EVENTS               200        1200
--   ADS_MIN_IMPRESSION_EVENTS          20         100
--   ADS_MIN_CLICK_EVENTS               5          25
--   ADS_EVENTS_IMPRESSION_SAMPLE_RATIO 0.1        0.6
--   ADS_EVENTS_CLICK_SAMPLE_RATIO      0.15       0.9
--
-- Full data volume execution time: ~60+ minutes
-- ============================================================================


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

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.GENERATE;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.INGEST;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.HARMONIZED;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.ANALYSE;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.DCR_ANALYSIS;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.ACTIVATION;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.DATA_SHARING;

CREATE SCHEMA IF NOT EXISTS AME_AD_SALES_DEMO.APPS;

-- Grant ACCOUNTADMIN privileges to create secrets in GENERATE schema
GRANT ALL PRIVILEGES ON SCHEMA AME_AD_SALES_DEMO.GENERATE TO ROLE ACCOUNTADMIN;

-- =============================================================================
-- TRANSFER SCHEMA OWNERSHIP TO ADMIN ROLE
-- =============================================================================

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.GENERATE TO ROLE AME_AD_SALES_DEMO_ADMIN COPY CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.INGEST TO ROLE AME_AD_SALES_DEMO_ADMIN;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.HARMONIZED TO ROLE AME_AD_SALES_DEMO_ADMIN;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.ANALYSE TO ROLE AME_AD_SALES_DEMO_ADMIN;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.DCR_ANALYSIS TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.ACTIVATION TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.DATA_SHARING TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA AME_AD_SALES_DEMO.APPS TO ROLE AME_AD_SALES_DEMO_ADMIN REVOKE CURRENT GRANTS;

USE ROLE AME_AD_SALES_DEMO_ADMIN;

CREATE OR REPLACE STAGE AME_AD_SALES_DEMO.GENERATE.DEMO_STAGE
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE STAGE AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = NONE RECORD_DELIMITER = '\n')
    DIRECTORY = (ENABLE = TRUE);

GRANT READ, WRITE ON STAGE AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS TO ROLE AME_AD_SALES_DEMO_ADMIN;



-- =============================================================================
-- DATA GENERATION AND PROCESSING SCRIPTS
-- Combined from individual scripts 02-24
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 02-generate-subscriber-archetypes.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

-- Total population to generate (aligned with behavioural log scope)
SET TOTAL_SUBSCRIBERS = 100000;

-- Tier mix assumption can be tuned to match business reality
SET AD_SUPPORTED_SHARE = 0.35;
SET STANDARD_SHARE = 0.45;
SET PREMIUM_SHARE = 0.20;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES AS
WITH tier_weights AS (
    SELECT *
    FROM (
        SELECT 1 AS order_key, 'Ad-supported' AS tier, $AD_SUPPORTED_SHARE::float AS weight UNION ALL
        SELECT 2, 'Standard', $STANDARD_SHARE::float UNION ALL
        SELECT 3, 'Premium', $PREMIUM_SHARE::float
    )
), tier_bounds AS (
    SELECT
        tier,
        weight,
        order_key,
        upper_bound,
        COALESCE(LAG(upper_bound) OVER (ORDER BY order_key), 0) AS lower_bound
    FROM (
        SELECT
            tier,
            weight,
            order_key,
            SUM(weight) OVER (ORDER BY order_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS upper_bound
        FROM tier_weights
    ) t
), archetype_weights AS (
    SELECT *
    FROM (
        SELECT 1 AS order_key, 'Ad-supported' AS tier, 'Prestige Binge Watcher' AS archetype, 0.05 AS weight UNION ALL
        SELECT 2, 'Ad-supported', 'Live Sports Fanatic', 0.10 UNION ALL
        SELECT 3, 'Ad-supported', 'Indie Film Explorer', 0.05 UNION ALL
        SELECT 4, 'Ad-supported', 'Kids Content Guardian', 0.10 UNION ALL
        SELECT 5, 'Ad-supported', 'Reality Show Enthusiast', 0.15 UNION ALL
        SELECT 6, 'Ad-supported', 'Documentary Devotee', 0.05 UNION ALL
        SELECT 7, 'Ad-supported', 'Sci-Fi Completionist', 0.05 UNION ALL
        SELECT 8, 'Ad-supported', 'New Release Hunter', 0.10 UNION ALL
        SELECT 9, 'Ad-supported', 'Comedy Night Casual', 0.15 UNION ALL
        SELECT 10, 'Ad-supported', 'Educational Learner', 0.05 UNION ALL
        SELECT 11, 'Ad-supported', 'Cult Classic Archivist', 0.05 UNION ALL
        SELECT 12, 'Ad-supported', 'Lifestyle Sampler', 0.10 UNION ALL
        SELECT 1, 'Standard', 'Prestige Binge Watcher', 0.10 UNION ALL
        SELECT 2, 'Standard', 'Live Sports Fanatic', 0.12 UNION ALL
        SELECT 3, 'Standard', 'Indie Film Explorer', 0.08 UNION ALL
        SELECT 4, 'Standard', 'Kids Content Guardian', 0.08 UNION ALL
        SELECT 5, 'Standard', 'Reality Show Enthusiast', 0.09 UNION ALL
        SELECT 6, 'Standard', 'Documentary Devotee', 0.09 UNION ALL
        SELECT 7, 'Standard', 'Sci-Fi Completionist', 0.10 UNION ALL
        SELECT 8, 'Standard', 'New Release Hunter', 0.08 UNION ALL
        SELECT 9, 'Standard', 'Comedy Night Casual', 0.08 UNION ALL
        SELECT 10, 'Standard', 'Educational Learner', 0.08 UNION ALL
        SELECT 11, 'Standard', 'Cult Classic Archivist', 0.05 UNION ALL
        SELECT 12, 'Standard', 'Lifestyle Sampler', 0.05 UNION ALL
        SELECT 1, 'Premium', 'Prestige Binge Watcher', 0.16 UNION ALL
        SELECT 2, 'Premium', 'Live Sports Fanatic', 0.12 UNION ALL
        SELECT 3, 'Premium', 'Indie Film Explorer', 0.09 UNION ALL
        SELECT 4, 'Premium', 'Kids Content Guardian', 0.05 UNION ALL
        SELECT 5, 'Premium', 'Reality Show Enthusiast', 0.04 UNION ALL
        SELECT 6, 'Premium', 'Documentary Devotee', 0.12 UNION ALL
        SELECT 7, 'Premium', 'Sci-Fi Completionist', 0.10 UNION ALL
        SELECT 8, 'Premium', 'New Release Hunter', 0.11 UNION ALL
        SELECT 9, 'Premium', 'Comedy Night Casual', 0.04 UNION ALL
        SELECT 10, 'Premium', 'Educational Learner', 0.11 UNION ALL
        SELECT 11, 'Premium', 'Cult Classic Archivist', 0.04 UNION ALL
        SELECT 12, 'Premium', 'Lifestyle Sampler', 0.02
    )
), archetype_bounds AS (
    SELECT
        tier,
        archetype,
        weight,
        order_key,
        upper_bound,
        COALESCE(LAG(upper_bound) OVER (PARTITION BY tier ORDER BY order_key), 0) AS lower_bound
    FROM (
        SELECT
            tier,
            archetype,
            weight,
            order_key,
            SUM(weight) OVER (PARTITION BY tier ORDER BY order_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS upper_bound
        FROM archetype_weights
    ) aw
), archetype_persona_weights AS (
    SELECT *
    FROM (
        SELECT 'Prestige Binge Watcher' AS archetype, 'High-Value User' AS persona, 0.55 AS weight UNION ALL
        SELECT 'Prestige Binge Watcher', 'Power User', 0.10 UNION ALL
        SELECT 'Prestige Binge Watcher', 'Disappointed User', 0.05 UNION ALL
        SELECT 'Prestige Binge Watcher', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Prestige Binge Watcher', 'Churn-Risk User', 0.05 UNION ALL
        SELECT 'Prestige Binge Watcher', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Prestige Binge Watcher', 'Subscriber Acquisition Candidate', 0.10 UNION ALL
        SELECT 'Live Sports Fanatic', 'High-Value User', 0.10 UNION ALL
        SELECT 'Live Sports Fanatic', 'Power User', 0.60 UNION ALL
        SELECT 'Live Sports Fanatic', 'Disappointed User', 0.05 UNION ALL
        SELECT 'Live Sports Fanatic', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Live Sports Fanatic', 'Churn-Risk User', 0.05 UNION ALL
        SELECT 'Live Sports Fanatic', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Live Sports Fanatic', 'Subscriber Acquisition Candidate', 0.05 UNION ALL
        SELECT 'Indie Film Explorer', 'High-Value User', 0.05 UNION ALL
        SELECT 'Indie Film Explorer', 'Power User', 0.10 UNION ALL
        SELECT 'Indie Film Explorer', 'Disappointed User', 0.55 UNION ALL
        SELECT 'Indie Film Explorer', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Indie Film Explorer', 'Churn-Risk User', 0.10 UNION ALL
        SELECT 'Indie Film Explorer', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Indie Film Explorer', 'Subscriber Acquisition Candidate', 0.05 UNION ALL
        SELECT 'Kids Content Guardian', 'High-Value User', 0.10 UNION ALL
        SELECT 'Kids Content Guardian', 'Power User', 0.05 UNION ALL
        SELECT 'Kids Content Guardian', 'Disappointed User', 0.05 UNION ALL
        SELECT 'Kids Content Guardian', 'Cross-Platform User', 0.55 UNION ALL
        SELECT 'Kids Content Guardian', 'Churn-Risk User', 0.10 UNION ALL
        SELECT 'Kids Content Guardian', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Kids Content Guardian', 'Subscriber Acquisition Candidate', 0.10 UNION ALL
        SELECT 'Reality Show Enthusiast', 'High-Value User', 0.05 UNION ALL
        SELECT 'Reality Show Enthusiast', 'Power User', 0.10 UNION ALL
        SELECT 'Reality Show Enthusiast', 'Disappointed User', 0.10 UNION ALL
        SELECT 'Reality Show Enthusiast', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Reality Show Enthusiast', 'Churn-Risk User', 0.50 UNION ALL
        SELECT 'Reality Show Enthusiast', 'Retargeting Candidate', 0.10 UNION ALL
        SELECT 'Reality Show Enthusiast', 'Subscriber Acquisition Candidate', 0.05 UNION ALL
        SELECT 'Documentary Devotee', 'High-Value User', 0.15 UNION ALL
        SELECT 'Documentary Devotee', 'Power User', 0.15 UNION ALL
        SELECT 'Documentary Devotee', 'Disappointed User', 0.05 UNION ALL
        SELECT 'Documentary Devotee', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Documentary Devotee', 'Churn-Risk User', 0.05 UNION ALL
        SELECT 'Documentary Devotee', 'Retargeting Candidate', 0.45 UNION ALL
        SELECT 'Documentary Devotee', 'Subscriber Acquisition Candidate', 0.05 UNION ALL
        SELECT 'Sci-Fi Completionist', 'High-Value User', 0.10 UNION ALL
        SELECT 'Sci-Fi Completionist', 'Power User', 0.55 UNION ALL
        SELECT 'Sci-Fi Completionist', 'Disappointed User', 0.05 UNION ALL
        SELECT 'Sci-Fi Completionist', 'Cross-Platform User', 0.15 UNION ALL
        SELECT 'Sci-Fi Completionist', 'Churn-Risk User', 0.05 UNION ALL
        SELECT 'Sci-Fi Completionist', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Sci-Fi Completionist', 'Subscriber Acquisition Candidate', 0.05 UNION ALL
        SELECT 'New Release Hunter', 'High-Value User', 0.10 UNION ALL
        SELECT 'New Release Hunter', 'Power User', 0.10 UNION ALL
        SELECT 'New Release Hunter', 'Disappointed User', 0.05 UNION ALL
        SELECT 'New Release Hunter', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'New Release Hunter', 'Churn-Risk User', 0.05 UNION ALL
        SELECT 'New Release Hunter', 'Retargeting Candidate', 0.10 UNION ALL
        SELECT 'New Release Hunter', 'Subscriber Acquisition Candidate', 0.50 UNION ALL
        SELECT 'Comedy Night Casual', 'High-Value User', 0.10 UNION ALL
        SELECT 'Comedy Night Casual', 'Power User', 0.10 UNION ALL
        SELECT 'Comedy Night Casual', 'Disappointed User', 0.05 UNION ALL
        SELECT 'Comedy Night Casual', 'Cross-Platform User', 0.50 UNION ALL
        SELECT 'Comedy Night Casual', 'Churn-Risk User', 0.10 UNION ALL
        SELECT 'Comedy Night Casual', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Comedy Night Casual', 'Subscriber Acquisition Candidate', 0.10 UNION ALL
        SELECT 'Educational Learner', 'High-Value User', 0.60 UNION ALL
        SELECT 'Educational Learner', 'Power User', 0.10 UNION ALL
        SELECT 'Educational Learner', 'Disappointed User', 0.05 UNION ALL
        SELECT 'Educational Learner', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Educational Learner', 'Churn-Risk User', 0.05 UNION ALL
        SELECT 'Educational Learner', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Educational Learner', 'Subscriber Acquisition Candidate', 0.05 UNION ALL
        SELECT 'Cult Classic Archivist', 'High-Value User', 0.05 UNION ALL
        SELECT 'Cult Classic Archivist', 'Power User', 0.10 UNION ALL
        SELECT 'Cult Classic Archivist', 'Disappointed User', 0.55 UNION ALL
        SELECT 'Cult Classic Archivist', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Cult Classic Archivist', 'Churn-Risk User', 0.10 UNION ALL
        SELECT 'Cult Classic Archivist', 'Retargeting Candidate', 0.05 UNION ALL
        SELECT 'Cult Classic Archivist', 'Subscriber Acquisition Candidate', 0.05 UNION ALL
        SELECT 'Lifestyle Sampler', 'High-Value User', 0.10 UNION ALL
        SELECT 'Lifestyle Sampler', 'Power User', 0.05 UNION ALL
        SELECT 'Lifestyle Sampler', 'Disappointed User', 0.10 UNION ALL
        SELECT 'Lifestyle Sampler', 'Cross-Platform User', 0.10 UNION ALL
        SELECT 'Lifestyle Sampler', 'Churn-Risk User', 0.10 UNION ALL
        SELECT 'Lifestyle Sampler', 'Retargeting Candidate', 0.45 UNION ALL
        SELECT 'Lifestyle Sampler', 'Subscriber Acquisition Candidate', 0.10
    )
), tier_persona_weights AS (
    SELECT *
    FROM (
        SELECT 1 AS order_key, 'Ad-supported' AS tier, 'High-Value User' AS persona, 0.10 AS weight UNION ALL
        SELECT 2, 'Ad-supported', 'Power User', 0.15 UNION ALL
        SELECT 3, 'Ad-supported', 'Disappointed User', 0.10 UNION ALL
        SELECT 4, 'Ad-supported', 'Cross-Platform User', 0.25 UNION ALL
        SELECT 5, 'Ad-supported', 'Churn-Risk User', 0.15 UNION ALL
        SELECT 6, 'Ad-supported', 'Retargeting Candidate', 0.15 UNION ALL
        SELECT 7, 'Ad-supported', 'Subscriber Acquisition Candidate', 0.10 UNION ALL
        SELECT 1, 'Standard', 'High-Value User', 0.20 UNION ALL
        SELECT 2, 'Standard', 'Power User', 0.20 UNION ALL
        SELECT 3, 'Standard', 'Disappointed User', 0.10 UNION ALL
        SELECT 4, 'Standard', 'Cross-Platform User', 0.20 UNION ALL
        SELECT 5, 'Standard', 'Churn-Risk User', 0.10 UNION ALL
        SELECT 6, 'Standard', 'Retargeting Candidate', 0.10 UNION ALL
        SELECT 7, 'Standard', 'Subscriber Acquisition Candidate', 0.10 UNION ALL
        SELECT 1, 'Premium', 'High-Value User', 0.35 UNION ALL
        SELECT 2, 'Premium', 'Power User', 0.20 UNION ALL
        SELECT 3, 'Premium', 'Disappointed User', 0.05 UNION ALL
        SELECT 4, 'Premium', 'Cross-Platform User', 0.15 UNION ALL
        SELECT 5, 'Premium', 'Churn-Risk User', 0.05 UNION ALL
        SELECT 6, 'Premium', 'Retargeting Candidate', 0.10 UNION ALL
        SELECT 7, 'Premium', 'Subscriber Acquisition Candidate', 0.10
    )
), persona_joint AS (
    SELECT
        ap.archetype,
        tp.tier,
        ap.persona,
        (ap.weight * tp.weight) AS raw_weight
    FROM archetype_persona_weights ap
    JOIN tier_persona_weights tp
      ON ap.persona = tp.persona
), persona_norm AS (
    SELECT
        tier,
        archetype,
        persona,
        raw_weight / NULLIF(SUM(raw_weight) OVER (PARTITION BY tier, archetype), 0) AS weight,
        ROW_NUMBER() OVER (
            PARTITION BY tier, archetype
            ORDER BY CASE persona
                WHEN 'High-Value User' THEN 1
                WHEN 'Power User' THEN 2
                WHEN 'Disappointed User' THEN 3
                WHEN 'Cross-Platform User' THEN 4
                WHEN 'Churn-Risk User' THEN 5
                WHEN 'Retargeting Candidate' THEN 6
                ELSE 7
            END
        ) AS order_key
    FROM persona_joint
), persona_bounds AS (
    SELECT
        tier,
        archetype,
        persona,
        weight,
        upper_bound,
        COALESCE(LAG(upper_bound) OVER (PARTITION BY tier, archetype ORDER BY order_key), 0) AS lower_bound
    FROM (
        SELECT
            tier,
            archetype,
            persona,
            weight,
            order_key,
            SUM(weight) OVER (
                PARTITION BY tier, archetype
                ORDER BY order_key
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS upper_bound
        FROM persona_norm
    ) pn
), base_population AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY seq4()) AS row_id,
        UNIFORM(0::float, 1::float, RANDOM()) AS tier_rand,
        UNIFORM(0::float, 1::float, RANDOM()) AS archetype_rand,
        UNIFORM(0::float, 1::float, RANDOM()) AS persona_rand
    FROM TABLE(GENERATOR(ROWCOUNT => $TOTAL_SUBSCRIBERS))
), assigned_tiers AS (
    SELECT
        bp.row_id,
        tb.tier,
        bp.archetype_rand,
        bp.persona_rand
    FROM base_population bp
    JOIN tier_bounds tb
      ON bp.tier_rand >= tb.lower_bound
     AND bp.tier_rand < tb.upper_bound
), assigned_archetypes AS (
    SELECT
        at.row_id,
        ar.tier,
        ar.archetype,
        at.persona_rand
    FROM assigned_tiers at
    JOIN archetype_bounds ar
      ON at.tier = ar.tier
     AND at.archetype_rand >= ar.lower_bound
     AND at.archetype_rand < ar.upper_bound
), assigned_personas AS (
    SELECT
        aa.row_id,
        aa.tier,
        aa.archetype,
        pb.persona
    FROM assigned_archetypes aa
    JOIN persona_bounds pb
      ON aa.tier = pb.tier
     AND aa.archetype = pb.archetype
     AND aa.persona_rand >= pb.lower_bound
     AND aa.persona_rand < pb.upper_bound
)
SELECT
    UUID_STRING() AS UNIQUE_ID,
    archetype AS ARCHETYPE,
    persona AS PERSONA,
    tier AS TIER
FROM assigned_personas;


SELECT count(*) as "Created rows in GENERATE.SUBSCRIBER_PROFILES" FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES;

-- ---------------------------------------------------------------------------
-- 03-generates-demographics-distributions.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.DEMOGRAPHICS_DISTRIBUTIONS (
    LAD_CODE STRING,
    TOTAL_POPULATION NUMBER,
    RURAL_URBAN STRING,
    INCOME_LEVEL STRING,
    EDUCATION_LEVEL STRING,
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

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.LAD_SAMPLE_AREAS_RAW (
    LAD_CODE STRING,
    DATA VARIANT
);

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.LAD_SAMPLE_AREAS (
    LAD_CODE STRING,
    AREA_NAME STRING,
    LONGITUDE FLOAT,
    LATITUDE FLOAT
);

COPY INTO AME_AD_SALES_DEMO.GENERATE.DEMOGRAPHICS_DISTRIBUTIONS
FROM @AME_AD_SALES_DEMO.GENERATE.DEMO_STAGE/lad_code_distributions.csv
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;

COPY INTO AME_AD_SALES_DEMO.GENERATE.LAD_SAMPLE_AREAS_RAW
FROM @AME_AD_SALES_DEMO.GENERATE.DEMO_STAGE/lad_sample_areas.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.LAD_SAMPLE_AREAS (
    LAD_CODE STRING,
    AREA_NAME STRING,
    LONGITUDE FLOAT,
    LATITUDE FLOAT
 ) AS 
 SELECT
    lad_code,
    value:name::string as area_name,
    value:longitude::float as longitude,
    value:latitude::float as latitude,
FROM
(
    SELECT
        t.lad_code,
        f.value
    FROM AME_AD_SALES_DEMO.GENERATE.LAD_SAMPLE_AREAS_RAW t,
    LATERAL FLATTEN(input => t.data) f
);





SELECT 'Rows in GENERATE.LAD_SAMPLE_AREAS' as "Table created", count(*) as "Rows generated" FROM AME_AD_SALES_DEMO.GENERATE.LAD_SAMPLE_AREAS
UNION ALL 
SELECT 'Rows in GENERATE.DEMOGRAPHICS_DISTRIBUTIONS' as "Table created", count(*) as "Rows generated" FROM AME_AD_SALES_DEMO.GENERATE.DEMOGRAPHICS_DISTRIBUTIONS
;



-- ---------------------------------------------------------------------------
-- 04-create-archetype-distributions.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_ARCHETYPE_DISTRIBUTIONS (
    ARCHETYPE STRING,
    RURAL_URBAN STRING,
    INCOME_LEVEL STRING,
    EDUCATION_LEVEL STRING,
    AGE_BANDS STRING,
    FAMILY_MARITAL_STATUS STRING,
    DIGITAL_MEDIA_PROPENSITY STRING,
    FAST_FASHION_PROPENSITY STRING,
    ONLINE_GROCERY_USE STRING,
    FINANCIAL_INVESTMENT_INTEREST STRING
);

INSERT OVERWRITE INTO AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_ARCHETYPE_DISTRIBUTIONS
SELECT * FROM VALUES
    ('Prestige Binge Watcher','Urban','High','Graduate','Established Career/Middle Age','Married / Cohabit w/ teens','Very High','Medium','High','High'),
    ('Live Sports Fanatic','Suburban / Urban','Upper-Mid','College','Young Adults/Early; Prime Settling/Family Formation','Married / Young family','High','Medium','Medium','Medium'),
    ('Indie Film Explorer','Urban Core','Mid','Graduate','Young Adults/Early','Single / Partnered','High','Low','Medium','Medium'),
    ('Kids Content Guardian','Suburban','Mid','Some College','Prime Settling/Family Formation','Married w/ young kids','Medium','Low','High','Low'),
    ('Reality Show Enthusiast','Urban / Suburban','Lower-Mid','High School','Adolescence/Teen Years; Young Adults/Early','Single / Cohabit','Very High','High','Medium','Low'),
    ('Documentary Devotee','Urban','High','Post-Grad','Established Career/Middle Age; Pre-Retirement/Empty Nesters','Married / Empty nester','High','Low','Medium','Very High'),
    ('Sci-Fi Completionist','Suburban / Urban','Upper-Mid','Graduate','Young Adults/Early','Single / Partnered','Very High','Medium','Medium','Medium'),
    ('New Release Hunter','Urban','High','Graduate','Young Adults/Early','Single','Very High','High','Medium','Medium'),
    ('Comedy Night Casual','Urban / Suburban','Mid','Some College','Young Adults/Early; Prime Settling/Family Formation','Married / Partnered','High','Medium','High','Low'),
    ('Educational Learner','Suburban / Rural Mix','Upper-Mid','Post-Grad','Established Career/Middle Age; Pre-Retirement/Empty Nesters','Married / Empty nester','High','Low','Medium','High'),
    ('Cult Classic Archivist','Urban Core','Mid','Graduate','Established Career/Middle Age','Single','Medium','Low','Low','Medium'),
    ('Lifestyle Sampler','Urban / Suburban','Mid','College','Young Adults/Early','Single / Partnered','High','Very High','High','Medium');

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_ARCHETYPE_REGION_CORRELATION AS
WITH archetypes AS (
    SELECT
        ARCHETYPE,
        UPPER(RURAL_URBAN) AS RURAL_TXT,
        UPPER(INCOME_LEVEL) AS INCOME_TXT,
        UPPER(EDUCATION_LEVEL) AS EDUCATION_TXT,
        UPPER(AGE_BANDS) AS AGE_TXT,
        UPPER(FAMILY_MARITAL_STATUS) AS FAMILY_TXT,
        UPPER(DIGITAL_MEDIA_PROPENSITY) AS DIGITAL_LEVEL_TXT,
        UPPER(FAST_FASHION_PROPENSITY) AS FASHION_LEVEL_TXT,
        UPPER(ONLINE_GROCERY_USE) AS GROCERY_LEVEL_TXT,
        UPPER(FINANCIAL_INVESTMENT_INTEREST) AS FINANCIAL_LEVEL_TXT,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%EARLY CHILDHOOD%' THEN TRUE ELSE FALSE END AS HAS_EARLY_CHILDHOOD,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%CHILDHOOD%' THEN TRUE ELSE FALSE END AS HAS_CHILDHOOD,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%ADOLESCENCE%' OR UPPER(AGE_BANDS) LIKE '%TEEN%' THEN TRUE ELSE FALSE END AS HAS_ADOLESCENCE,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%YOUNG ADULTS%' THEN TRUE ELSE FALSE END AS HAS_YOUNG_ADULTS,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%PRIME SETTLING%' THEN TRUE ELSE FALSE END AS HAS_PRIME_SETTLING,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%ESTABLISHED CAREER%' THEN TRUE ELSE FALSE END AS HAS_ESTABLISHED,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%PRE-RETIREMENT%' THEN TRUE ELSE FALSE END AS HAS_PRE_RETIREMENT,
        CASE WHEN UPPER(AGE_BANDS) LIKE '%RETIREMENT%' THEN TRUE ELSE FALSE END AS HAS_RETIREMENT,
        CASE WHEN UPPER(FAMILY_MARITAL_STATUS) LIKE '%MARRIED%' THEN TRUE ELSE FALSE END AS HAS_MARRIED,
        CASE WHEN UPPER(FAMILY_MARITAL_STATUS) LIKE '%SINGLE%' THEN TRUE ELSE FALSE END AS HAS_SINGLE,
        CASE WHEN UPPER(FAMILY_MARITAL_STATUS) LIKE '%COHABIT%' OR UPPER(FAMILY_MARITAL_STATUS) LIKE '%YOUNG%' OR UPPER(FAMILY_MARITAL_STATUS) LIKE '%TEEN%' OR UPPER(FAMILY_MARITAL_STATUS) LIKE '%DEPENDENT%' THEN TRUE ELSE FALSE END AS HAS_DEPENDENTS,
        CASE WHEN UPPER(FAMILY_MARITAL_STATUS) LIKE '%EMPTY NESTER%' THEN TRUE ELSE FALSE END AS HAS_EMPTY_NESTER,
        CASE WHEN UPPER(FAMILY_MARITAL_STATUS) LIKE '%PARTNER%' THEN TRUE ELSE FALSE END AS HAS_PARTNERED,
        CASE
            WHEN UPPER(DIGITAL_MEDIA_PROPENSITY) = 'VERY HIGH' THEN 4
            WHEN UPPER(DIGITAL_MEDIA_PROPENSITY) = 'HIGH' THEN 3
            WHEN UPPER(DIGITAL_MEDIA_PROPENSITY) = 'MEDIUM' THEN 2
            ELSE 1
        END AS DIGITAL_LEVEL_RANK,
        CASE
            WHEN UPPER(FAST_FASHION_PROPENSITY) = 'VERY HIGH' THEN 4
            WHEN UPPER(FAST_FASHION_PROPENSITY) = 'HIGH' THEN 3
            WHEN UPPER(FAST_FASHION_PROPENSITY) = 'MEDIUM' THEN 2
            ELSE 1
        END AS FASHION_LEVEL_RANK,
        CASE
            WHEN UPPER(ONLINE_GROCERY_USE) = 'VERY HIGH' THEN 4
            WHEN UPPER(ONLINE_GROCERY_USE) = 'HIGH' THEN 3
            WHEN UPPER(ONLINE_GROCERY_USE) = 'MEDIUM' THEN 2
            ELSE 1
        END AS GROCERY_LEVEL_RANK,
        CASE
            WHEN UPPER(FINANCIAL_INVESTMENT_INTEREST) = 'VERY HIGH' THEN 4
            WHEN UPPER(FINANCIAL_INVESTMENT_INTEREST) = 'HIGH' THEN 3
            WHEN UPPER(FINANCIAL_INVESTMENT_INTEREST) = 'MEDIUM' THEN 2
            ELSE 1
        END AS FINANCIAL_LEVEL_RANK
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_ARCHETYPE_DISTRIBUTIONS
),
lad AS (
    SELECT
        d.*,
        CASE
            WHEN BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX >= 7 THEN 'VERY HIGH'
            WHEN BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX >= 5 THEN 'HIGH'
            WHEN BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX >= 3 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS DIGITAL_LEVEL_TXT,
        CASE
            WHEN BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY >= 7 THEN 'VERY HIGH'
            WHEN BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY >= 5 THEN 'HIGH'
            WHEN BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY >= 3 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS FASHION_LEVEL_TXT,
        CASE
            WHEN BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE >= 7 THEN 'VERY HIGH'
            WHEN BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE >= 5 THEN 'HIGH'
            WHEN BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE >= 3 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS GROCERY_LEVEL_TXT,
        CASE
            WHEN BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST >= 7 THEN 'VERY HIGH'
            WHEN BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST >= 5 THEN 'HIGH'
            WHEN BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST >= 3 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS FINANCIAL_LEVEL_TXT,
        CASE
            WHEN BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX >= 7 THEN 4
            WHEN BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX >= 5 THEN 3
            WHEN BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX >= 3 THEN 2
            ELSE 1
        END AS DIGITAL_LEVEL_RANK,
        CASE
            WHEN BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY >= 7 THEN 4
            WHEN BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY >= 5 THEN 3
            WHEN BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY >= 3 THEN 2
            ELSE 1
        END AS FASHION_LEVEL_RANK,
        CASE
            WHEN BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE >= 7 THEN 4
            WHEN BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE >= 5 THEN 3
            WHEN BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE >= 3 THEN 2
            ELSE 1
        END AS GROCERY_LEVEL_RANK,
        CASE
            WHEN BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST >= 7 THEN 4
            WHEN BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST >= 5 THEN 3
            WHEN BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST >= 3 THEN 2
            ELSE 1
        END AS FINANCIAL_LEVEL_RANK
    FROM AME_AD_SALES_DEMO.GENERATE.DEMOGRAPHICS_DISTRIBUTIONS d
),
correlation AS (
    SELECT
        l.LAD_CODE,
        a.ARCHETYPE,
        IFF(a.RURAL_TXT LIKE '%' || UPPER(l.RURAL_URBAN) || '%', 1.0, 0.0) AS RURAL_MATCH,
        IFF(a.INCOME_TXT LIKE '%' || UPPER(l.INCOME_LEVEL) || '%', 1.0, 0.0) AS INCOME_MATCH,
        IFF(a.EDUCATION_TXT LIKE '%' || UPPER(l.EDUCATION_LEVEL) || '%', 1.0, 0.0) AS EDUCATION_MATCH,
        CASE
            WHEN (COALESCE(a.HAS_EARLY_CHILDHOOD, FALSE) OR COALESCE(a.HAS_CHILDHOOD, FALSE) OR COALESCE(a.HAS_ADOLESCENCE, FALSE) OR COALESCE(a.HAS_YOUNG_ADULTS, FALSE) OR COALESCE(a.HAS_PRIME_SETTLING, FALSE) OR COALESCE(a.HAS_ESTABLISHED, FALSE) OR COALESCE(a.HAS_PRE_RETIREMENT, FALSE) OR COALESCE(a.HAS_RETIREMENT, FALSE))
            THEN (
                (CASE WHEN a.HAS_EARLY_CHILDHOOD THEN l.EARLY_CHILDHOOD ELSE 0 END) +
                (CASE WHEN a.HAS_CHILDHOOD THEN l.CHILDHOOD_ELEMENTARY_SCHOOL ELSE 0 END) +
                (CASE WHEN a.HAS_ADOLESCENCE THEN l.ADOLESCENSE_TEEN_YEARS ELSE 0 END) +
                (CASE WHEN a.HAS_YOUNG_ADULTS THEN l.YOUNG_ADULTS_EARLY ELSE 0 END) +
                (CASE WHEN a.HAS_PRIME_SETTLING THEN l.PRIME_SETTLING_FAMILY_FORMATION ELSE 0 END) +
                (CASE WHEN a.HAS_ESTABLISHED THEN l.ESTABLISHED_CAREER_MIDDLE_AGE ELSE 0 END) +
                (CASE WHEN a.HAS_PRE_RETIREMENT THEN l.PRE_RETIREMENT_EMPTY_NESTERS ELSE 0 END) +
                (CASE WHEN a.HAS_RETIREMENT THEN l.RETIREMENT_SENIOR_YEARS ELSE 0 END)
            ) /
            NULLIF(
                (CASE WHEN a.HAS_EARLY_CHILDHOOD THEN 1 ELSE 0 END) +
                (CASE WHEN a.HAS_CHILDHOOD THEN 1 ELSE 0 END) +
                (CASE WHEN a.HAS_ADOLESCENCE THEN 1 ELSE 0 END) +
                (CASE WHEN a.HAS_YOUNG_ADULTS THEN 1 ELSE 0 END) +
                (CASE WHEN a.HAS_PRIME_SETTLING THEN 1 ELSE 0 END) +
                (CASE WHEN a.HAS_ESTABLISHED THEN 1 ELSE 0 END) +
                (CASE WHEN a.HAS_PRE_RETIREMENT THEN 1 ELSE 0 END) +
                (CASE WHEN a.HAS_RETIREMENT THEN 1 ELSE 0 END),
                0
            )
            ELSE 0
        END AS AGE_ALIGNMENT,
        CASE
            WHEN (
                (CASE WHEN COALESCE(a.HAS_MARRIED, FALSE) THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_SINGLE, FALSE) THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_DEPENDENTS, FALSE) THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_EMPTY_NESTER, FALSE) THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_PARTNERED, FALSE) THEN 1 ELSE 0 END)
            ) > 0 THEN (
                (CASE WHEN COALESCE(a.HAS_MARRIED, FALSE) THEN l.DEMO_MARITAL_MARRIED_OR_CIVIL_PARTNERSHIP ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_SINGLE, FALSE) THEN (l.DEMO_MARITAL_SINGLE_NEVER_MARRIED + l.DEMO_FAMILY_SINGLE_PERSON_HOUSEHOLD) / 2 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_DEPENDENTS, FALSE) THEN l.DEMO_FAMILY_COUPLES_WITH_DEPENDENT_CHILDREN ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_EMPTY_NESTER, FALSE) OR COALESCE(a.HAS_PARTNERED, FALSE) THEN l.DEMO_FAMILY_COUPLES_NO_CHILDREN ELSE 0 END)
            ) /
            NULLIF(
                (CASE WHEN COALESCE(a.HAS_MARRIED, FALSE) THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_SINGLE, FALSE) THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_DEPENDENTS, FALSE) THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(a.HAS_EMPTY_NESTER, FALSE) OR COALESCE(a.HAS_PARTNERED, FALSE) THEN 1 ELSE 0 END),
                0
            )
            ELSE 0
        END AS FAMILY_SCORE,
        IFF(l.DIGITAL_LEVEL_RANK >= a.DIGITAL_LEVEL_RANK, 1.0, 0.0) AS DIGITAL_MATCH,
        IFF(l.FASHION_LEVEL_RANK >= a.FASHION_LEVEL_RANK, 1.0, 0.0) AS FAST_FASHION_MATCH,
        IFF(l.GROCERY_LEVEL_RANK >= a.GROCERY_LEVEL_RANK, 1.0, 0.0) AS GROCERY_MATCH,
        IFF(l.FINANCIAL_LEVEL_RANK >= a.FINANCIAL_LEVEL_RANK, 1.0, 0.0) AS FINANCIAL_MATCH
    FROM lad l
    CROSS JOIN archetypes a
)
SELECT
    LAD_CODE,
    ARCHETYPE,
    RURAL_MATCH,
    INCOME_MATCH,
    EDUCATION_MATCH,
    AGE_ALIGNMENT,
    FAMILY_SCORE,
    DIGITAL_MATCH,
    FAST_FASHION_MATCH,
    GROCERY_MATCH,
    FINANCIAL_MATCH,
    (RURAL_MATCH + INCOME_MATCH + EDUCATION_MATCH + DIGITAL_MATCH + FAST_FASHION_MATCH + GROCERY_MATCH + FINANCIAL_MATCH + AGE_ALIGNMENT + FAMILY_SCORE) / 9 AS OVERALL_SCORE
FROM correlation;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES_DISTRIBUTIONS AS
WITH archetype_totals AS (
    SELECT ARCHETYPE, COUNT(*) AS archetype_count
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
    GROUP BY ARCHETYPE
),
scored AS (
    SELECT
        c.LAD_CODE,
        c.ARCHETYPE,
        c.OVERALL_SCORE,
        COALESCE(a.archetype_count, 0) AS archetype_count
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_ARCHETYPE_REGION_CORRELATION c
    LEFT JOIN archetype_totals a
      ON c.ARCHETYPE = a.ARCHETYPE
),
lad_weight AS (
    SELECT
        ARCHETYPE,
        SUM(OVERALL_SCORE) AS total_score
    FROM scored
    GROUP BY ARCHETYPE
)
SELECT
    s.LAD_CODE,
    s.ARCHETYPE,
    s.archetype_count,
    s.OVERALL_SCORE,
    lw.total_score,
    CASE
        WHEN lw.total_score > 0 THEN s.archetype_count * (s.OVERALL_SCORE / lw.total_score)
        ELSE 0
    END AS distributed_subscribers
FROM scored s
LEFT JOIN lad_weight lw
  ON s.ARCHETYPE = lw.ARCHETYPE;

SELECT 'Rows in GENERATE.SUBSCRIBER_ARCHETYPE_DISTRIBUTIONS' as "Table created", count(*) as "Rows generated" FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_ARCHETYPE_DISTRIBUTIONS
UNION ALL 
SELECT 'Rows in GENERATE.SUBSCRIBER_ARCHETYPE_REGION_CORRELATION' as "Table created", count(*) as "Rows generated" FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_ARCHETYPE_REGION_CORRELATION
UNION ALL 
SELECT 'Rows in GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES_DISTRIBUTIONS' as "Table created", count(*) as "Rows generated" FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES_DISTRIBUTIONS
;




-- ---------------------------------------------------------------------------
-- 05-generate-subscribers.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

-- Parameters for cohort-based signup generation
SET MONTHS_BACK = 6;
SET GROWTH_RATE_YOY = 0.25;
-- Fixed monthly multipliers (Jan..Dec)
CREATE OR REPLACE TEMP TABLE TMP_SEASONAL_BASE(month_num INT, base_mult FLOAT) AS
SELECT * FROM VALUES
  (1, 0.92),(2, 0.95),(3, 0.98),(4, 1.00),(5, 1.03),(6, 1.06),
  (7, 1.10),(8, 1.08),(9, 1.05),(10, 1.07),(11, 1.12),(12, 1.15);
SET SEASONAL_RANDOM_AMPLITUDE = 0.25;

CREATE OR REPLACE FUNCTION AME_AD_SALES_DEMO.GENERATE.GENERATE_SUBSCRIBER_IDENTITY(unique_id STRING, extra_seed STRING, use_extra_seed BOOLEAN)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('faker')
HANDLER = 'generate_identity'
AS
$$
from faker import Faker
import hashlib


def _sanitize(token: str) -> str:
    return "".join(ch for ch in token.lower() if ch.isalpha())


def generate_identity(unique_id: str, extra_seed: str | None, use_extra_seed: bool | None) -> dict:
    faker = Faker()

    seed_source = unique_id or ""

    if use_extra_seed and extra_seed:
        seed_source = f"{seed_source}|{extra_seed}"

    if seed_source:
        seed_val = int(hashlib.sha256(seed_source.encode("utf-8")).hexdigest(), 16) % 10**8
        faker.seed_instance(seed_val)

    first_name = faker.first_name()
    last_name = faker.last_name()

    first_token = _sanitize(first_name) or faker.first_name().lower()
    last_token = _sanitize(last_name) or faker.last_name().lower()

    unique_hash = hashlib.sha256((unique_id or '').encode('utf-8')).hexdigest()[:5]

    strategies = [
        lambda: f"{first_token}{unique_hash}",
        lambda: f"{first_token}.{last_token}{unique_hash}",
        lambda: f"{last_token}{unique_hash}",
        lambda: f"{first_token[0]}{last_token}{unique_hash}" if first_token else f"{last_token}{unique_hash}",
        lambda: f"{last_token}_{first_token}_{unique_hash}"
    ]

    local_part = faker.random_element(strategies)()
    domains = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "protonmail.com"]
    email = f"{local_part}@{faker.random_element(domains)}"

    username = f"{first_token[0]}{last_token}{faker.random_int(1, 9999):04d}" if first_token else faker.user_name()

    digits = "".join(str(faker.random_int(0, 9)) for _ in range(9))
    primary_mobile = f"+44 7{digits[:3]} {digits[3:6]} {digits[6:]}"

    ip_address = faker.ipv4_public()

    return {
        "full_name": f"{first_name} {last_name}",
        "email": email,
        "username": username,
        "primary_mobile": primary_mobile,
        "ip_address": ip_address
    }
$$;

-- Build signup cohorts over the last MONTHS_BACK months with 25% YoY growth and seasonality ±25%
CREATE OR REPLACE TEMP TABLE TMP_SIGNUP_DISTRIBUTION AS
WITH months AS (
  SELECT
    seq.value::INT AS month_offset,
    DATE_TRUNC('MONTH', DATEADD(month, -seq.value::INT, CURRENT_DATE())) AS month_start,
    MONTH(DATEADD(month, -seq.value::INT, CURRENT_DATE())) AS month_num,
    FLOOR((seq.value::INT)/12) AS years_since_start
  FROM LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, $MONTHS_BACK)) seq
),
seasonal AS (
  SELECT
    m.*,
    sb.base_mult,
    -- random variation per month within ± amplitude
    (1 + (UNIFORM(-$SEASONAL_RANDOM_AMPLITUDE::FLOAT, $SEASONAL_RANDOM_AMPLITUDE::FLOAT, RANDOM())::FLOAT)) AS noise_factor
  FROM months m
  JOIN TMP_SEASONAL_BASE sb ON sb.month_num = m.month_num
),
weighted AS (
  SELECT
    month_offset,
    month_start,
    base_mult,
    noise_factor,
    POW(1 + $GROWTH_RATE_YOY::FLOAT, years_since_start) AS growth_factor,
    base_mult * noise_factor * POW(1 + $GROWTH_RATE_YOY::FLOAT, years_since_start) AS raw_weight
  FROM seasonal
),
total_subs AS (
  SELECT COUNT(*) AS total_subscribers
  FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
),
norm AS (
  SELECT
    w.*,
    (w.raw_weight / NULLIF(SUM(w.raw_weight) OVER (), 0)) AS weight_norm
  FROM weighted w
),
alloc AS (
  SELECT
    n.month_offset,
    n.month_start,
    n.weight_norm,
    t.total_subscribers,
    GREATEST(0, ROUND(n.weight_norm * t.total_subscribers))::INT AS month_alloc
  FROM norm n
  CROSS JOIN total_subs t
),
balanced AS (
  -- Adjust allocation to exactly match total subscribers
  SELECT
    a.*,
    SUM(month_alloc) OVER () AS sum_alloc,
    (a.total_subscribers - SUM(month_alloc) OVER ()) AS diff -- usually small
  FROM alloc a
),
final_alloc AS (
  SELECT
    month_offset,
    month_start,
    CASE
      WHEN diff = 0 THEN month_alloc
      WHEN diff > 0 THEN month_alloc + CASE WHEN ROW_NUMBER() OVER (ORDER BY month_start) <= diff THEN 1 ELSE 0 END
      ELSE GREATEST(0, month_alloc - CASE WHEN ROW_NUMBER() OVER (ORDER BY month_start) <= ABS(diff) THEN 1 ELSE 0 END)
    END AS month_alloc
  FROM balanced
)
SELECT * FROM final_alloc;

-- Assign signup_date to each subscriber by mapping random order into monthly allocations
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_SIGNUPS AS
WITH subs AS (
  SELECT
    UNIQUE_ID,
    ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
  FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
),
expanded_base AS (
  SELECT
    fa.*,
    SUM(fa.month_alloc) OVER (ORDER BY fa.month_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_alloc
  FROM TMP_SIGNUP_DISTRIBUTION fa
),
expanded AS (
  SELECT
    eb.*,
    COALESCE(LAG(eb.cum_alloc) OVER (ORDER BY eb.month_start), 0) AS prev_cum_alloc
  FROM expanded_base eb
),
assignments AS (
  SELECT
    s.UNIQUE_ID,
    MIN(e.month_start) AS signup_month_start
  FROM subs s
  JOIN expanded e
    ON s.rn <= e.cum_alloc
   AND s.rn > e.prev_cum_alloc
  GROUP BY s.UNIQUE_ID
),
dated AS (
  SELECT
    a.UNIQUE_ID,
    DATEADD(
      day,
      MOD(ABS(HASH(a.UNIQUE_ID))::INT, GREATEST(DATEDIFF(day, a.signup_month_start, DATEADD(month, 1, a.signup_month_start)), 1)),
      a.signup_month_start
    ) AS SIGNUP_DATE
  FROM assignments a
)
SELECT UNIQUE_ID, SIGNUP_DATE FROM dated;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES AS
WITH enriched AS (
    SELECT
        sp.UNIQUE_ID,
        sp.ARCHETYPE,
        sp.PERSONA,
        sp.TIER,
        ROW_NUMBER() OVER (ORDER BY sp.UNIQUE_ID) AS PROFILE_SEQ_ID,
        AME_AD_SALES_DEMO.GENERATE.GENERATE_SUBSCRIBER_IDENTITY(sp.UNIQUE_ID, NULL, FALSE) AS identity_payload
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES sp
),
signup_resolved AS (
  SELECT
    sp.UNIQUE_ID,
    sgn.SIGNUP_DATE
  FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES sp
  LEFT JOIN AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_SIGNUPS sgn
    ON sgn.UNIQUE_ID = sp.UNIQUE_ID
)
SELECT
    enriched.UNIQUE_ID,
    PROFILE_SEQ_ID,
    CONCAT('SUB-', LPAD(TO_VARCHAR(PROFILE_SEQ_ID), 9, '0')) AS PROFILE_ID,
    ARCHETYPE,
    PERSONA,
    TIER,
    identity_payload:full_name::STRING AS FULL_NAME,
    identity_payload:email::STRING AS EMAIL,
    identity_payload:username::STRING AS USERNAME,
    identity_payload:primary_mobile::STRING AS PRIMARY_MOBILE,
    identity_payload:ip_address::STRING AS IP_ADDRESS,
    sr.SIGNUP_DATE
FROM enriched
LEFT JOIN signup_resolved sr
  ON sr.UNIQUE_ID = enriched.UNIQUE_ID;

-- Post-generation deduplication for identity attributes
UPDATE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES tgt
SET
    FULL_NAME = src.new_identity:full_name::STRING,
    EMAIL = src.new_identity:email::STRING,
    USERNAME = src.new_identity:username::STRING,
    PRIMARY_MOBILE = src.new_identity:primary_mobile::STRING,
    IP_ADDRESS = src.new_identity:ip_address::STRING
FROM (
    SELECT
        UNIQUE_ID,
        AME_AD_SALES_DEMO.GENERATE.GENERATE_SUBSCRIBER_IDENTITY(
            UNIQUE_ID,
            CONCAT('EMAIL_FIX_', ROW_NUMBER() OVER (PARTITION BY EMAIL ORDER BY UNIQUE_ID)),
            TRUE
        ) AS new_identity
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EMAIL ORDER BY UNIQUE_ID) > 1
) src
WHERE tgt.UNIQUE_ID = src.UNIQUE_ID;

UPDATE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES tgt
SET
    PRIMARY_MOBILE = src.new_mobile
FROM (
    SELECT
        UNIQUE_ID,
        AME_AD_SALES_DEMO.GENERATE.GENERATE_SUBSCRIBER_IDENTITY(
            UNIQUE_ID,
            CONCAT('MOBILE_FIX_', ROW_NUMBER() OVER (PARTITION BY PRIMARY_MOBILE ORDER BY UNIQUE_ID)),
            TRUE
        ):primary_mobile::STRING AS new_mobile
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRIMARY_MOBILE ORDER BY UNIQUE_ID) > 1
) src
WHERE tgt.UNIQUE_ID = src.UNIQUE_ID;

UPDATE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES tgt
SET
    IP_ADDRESS = src.new_ip
FROM (
    SELECT
        UNIQUE_ID,
        AME_AD_SALES_DEMO.GENERATE.GENERATE_SUBSCRIBER_IDENTITY(
            UNIQUE_ID,
            CONCAT('IP_FIX_', ROW_NUMBER() OVER (PARTITION BY IP_ADDRESS ORDER BY UNIQUE_ID)),
            TRUE
        ):ip_address::STRING AS new_ip
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY IP_ADDRESS ORDER BY UNIQUE_ID) > 1
) src
WHERE tgt.UNIQUE_ID = src.UNIQUE_ID;



SELECT 
    'Rows in GENERATE.SUBSCRIBER_FULL_PROFILES' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES;

-- ---------------------------------------------------------------------------
-- 06-generate-clickstream-events.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

SET ROLLING_MONTH_WINDOW = 6;
SET CLICKSTREAM_START_DATE = DATEADD(month, -$ROLLING_MONTH_WINDOW, CURRENT_DATE());
SET CLICKSTREAM_MONTH_COUNT = $ROLLING_MONTH_WINDOW;
SET CLICKSTREAM_TIMEZONE = 'UTC';
-- Rolling start/end dates stay aligned with ad generation (24 month lookback).

CREATE OR REPLACE FUNCTION AME_AD_SALES_DEMO.GENERATE.GENERATE_SUBSCRIBER_CLICKSTREAM(
    unique_id STRING,
    persona STRING,
    archetype STRING,
    base_start_date DATE,
    month_count INTEGER,
    timezone STRING
) RETURNS TABLE (
    event_id STRING,
    journey_code STRING,
    journey_name STRING,
    session_id STRING,
    event_type STRING,
    event_ts STRING,
    device STRING,
    page_path STRING,
    content_id STRING,
    content_type STRING,
    content_category STRING,
    attributes VARIANT,
    persona STRING,
    archetype STRING
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('pendulum')
HANDLER = 'ClickstreamGenerator'
AS
$$
import random
import hashlib
import pendulum
from typing import Dict, List, Optional

CONTENT_CATALOG = [
    {'content_id': 'SER-PRESTIGE-001', 'title': 'Prestige Heights', 'content_type': 'series', 'content_category': 'Drama', 'page_path': '/watch/series/prestige-heights'},
    {'content_id': 'SER-SCIFI-001', 'title': 'Nebula Frontier', 'content_type': 'series', 'content_category': 'Sci-Fi', 'page_path': '/watch/series/nebula-frontier'},
    {'content_id': 'MOV-CLIMATE-001', 'title': 'Climate Frontiers', 'content_type': 'movie', 'content_category': 'Documentary', 'page_path': '/watch/movie/climate-frontiers'},
    {'content_id': 'MOV-KIDS-001', 'title': 'Adventure Pals', 'content_type': 'kids', 'content_category': 'Kids', 'page_path': '/watch/kids/adventure-pals'},
    {'content_id': 'DOC-WELLNESS-001', 'title': 'Wellness Reset', 'content_type': 'docuseries', 'content_category': 'Lifestyle', 'page_path': '/watch/docuseries/wellness-reset'},
    {'content_id': 'DOC-CLIMATE-001', 'title': 'Planet Watch', 'content_type': 'docuseries', 'content_category': 'Documentary', 'page_path': '/watch/docuseries/planet-watch'},
    {'content_id': 'LIVE-SPORTS-001', 'title': 'Championship Final', 'content_type': 'live', 'content_category': 'Sports', 'page_path': '/watch/live/championship-final'},
    {'content_id': 'LIVE-SCIFI-001', 'title': 'Sci-Fi Marathon Live', 'content_type': 'live', 'content_category': 'Sci-Fi', 'page_path': '/watch/live/sci-fi-marathon'},
    {'content_id': 'CLIP-HIGHLIGHT-001', 'title': 'Match Highlights', 'content_type': 'short_form', 'content_category': 'Sports', 'page_path': '/watch/clips/match-highlights'},
    {'content_id': 'CLIP-COMEDY-001', 'title': 'Late Night Laughs', 'content_type': 'short_form', 'content_category': 'Comedy', 'page_path': '/watch/clips/late-night-laughs'},
    {'content_id': 'CLIP-WELLNESS-001', 'title': 'Wellness Minute', 'content_type': 'short_form', 'content_category': 'Lifestyle', 'page_path': '/watch/clips/wellness-minute'},
    {'content_id': 'PRV-ORIG-001', 'title': 'Original Premiere Trailer', 'content_type': 'preview', 'content_category': 'Originals', 'page_path': '/watch/preview/original-premiere'},
    {'content_id': 'ORIG-THRILLER-001', 'title': 'Night Signal', 'content_type': 'original', 'content_category': 'Originals', 'page_path': '/watch/original/night-signal'},
    {'content_id': 'REALITY-GAME-001', 'title': 'House of Fame', 'content_type': 'reality', 'content_category': 'Reality', 'page_path': '/watch/reality/house-of-fame'}
]

PERSONA_CONTENT_CATEGORIES = {
    'HIGH-VALUE USER': ['Drama', 'Originals'],
    'HIGH VALUE USER': ['Drama', 'Originals'],
    'POWER USER': ['Sports', 'Sci-Fi'],
    'DISAPPOINTED USER': ['Documentary', 'Drama'],
    'CROSS-PLATFORM USER': ['Kids', 'Comedy'],
    'CHURN-RISK USER': ['Reality', 'Lifestyle'],
    'RETARGETING CANDIDATE': ['Lifestyle', 'Documentary'],
    'SUBSCRIBER ACQUISITION CANDIDATE': ['Originals', 'Drama']
}

# Journey templates with ordered events and contextual metadata
JOURNEY_LIBRARY = {
    'HIGH_VALUE': {
        'name': 'High-Value Binge Upgrade Journey',
        'events': [
            {'event_type': 'PROFILE_VIEW', 'device': 'Smart TV', 'page_path': '/profiles', 'attributes': {'profile_id': 'primary'}},
            {'event_type': 'BROWSE_PAGE', 'device': 'Smart TV', 'page_path': '/browse/editors-picks', 'content_category': 'Drama', 'attributes': {'page_name': "Editor's Picks"}},
            {'event_type': 'PLAY_START', 'device': 'Smart TV', 'content_type': 'series', 'content_category': 'Drama', 'content_group': 'primary', 'content_action': 'set', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'PAUSE', 'device': 'Smart TV', 'content_type': 'series', 'content_category': 'Drama', 'content_group': 'primary', 'attributes': {'reason': 'pause_button'}},
            {'event_type': 'PLAY_RESUME', 'device': 'Smart TV', 'content_type': 'series', 'content_category': 'Drama', 'content_group': 'primary', 'attributes': {}},
            {'event_type': 'SKIP_INTRO', 'device': 'Smart TV', 'content_type': 'series', 'content_category': 'Drama', 'content_group': 'primary', 'attributes': {}},
            {'event_type': 'PLAY_STOP', 'device': 'Smart TV', 'content_type': 'series', 'content_category': 'Drama', 'content_group': 'primary', 'attributes': {'reason': 'completed_episode'}},
            {'event_type': 'SEARCH_QUERY', 'device': 'Smart TV', 'page_path': '/search', 'attributes': {'query': 'award winning drama', 'filters': ['series']}},
            {'event_type': 'ADD_TO_MY_LIST', 'device': 'Smart TV', 'page_path': '/watchlist', 'content_type': 'series', 'content_category': 'Drama', 'content_group': 'primary', 'attributes': {'action': 'add', 'list_name': 'Prestige Binge'}},
            {'event_type': 'VIEW_SUBSCRIPTION_PAGE', 'device': 'Mobile', 'page_path': '/account/subscription', 'attributes': {'current_tier': 'standard'}},
            {'event_type': 'UPGRADE_PAGE_VIEW', 'device': 'Mobile', 'page_path': '/account/upgrade', 'attributes': {'from_tier': 'standard', 'to_tier': 'premium'}},
            {'event_type': 'PLAY_START', 'device': 'Smart TV', 'content_type': 'original', 'content_category': 'Originals', 'content_group': 'feature', 'content_action': 'set', 'attributes': {'play_mode': 'on_demand'}}
        ]
    },
    'POWER_USER': {
        'name': 'Power User Live Event Journey',
        'events': [
            {'event_type': 'PROFILE_VIEW', 'device': 'Mobile', 'page_path': '/profiles', 'attributes': {'profile_id': 'mobile_main'}},
            {'event_type': 'SEARCH_QUERY', 'device': 'Mobile', 'page_path': '/search', 'attributes': {'query': 'live sports channel', 'filters': ['live']}},
            {'event_type': 'PLAY_START', 'device': 'Mobile', 'content_type': 'live', 'content_category': 'Sports', 'content_group': 'live_session', 'content_action': 'set', 'attributes': {'play_mode': 'live'}},
            {'event_type': 'PLAY_STOP', 'device': 'Mobile', 'content_type': 'live', 'content_category': 'Sports', 'content_group': 'live_session', 'attributes': {'reason': 'device_switch'}},
            {'event_type': 'PLAY_RESUME', 'device': 'Smart TV', 'content_type': 'live', 'content_category': 'Sports', 'content_group': 'live_session', 'attributes': {}},
            {'event_type': 'REBUFFER_EVENT', 'device': 'Smart TV', 'content_type': 'live', 'content_category': 'Sports', 'content_group': 'live_session', 'attributes': {'duration': 3}},
            {'event_type': 'PLAY_RESUME', 'device': 'Smart TV', 'content_type': 'live', 'content_category': 'Sports', 'content_group': 'live_session', 'attributes': {}},
            {'event_type': 'PLAY_STOP', 'device': 'Smart TV', 'content_type': 'live', 'content_category': 'Sports', 'content_group': 'live_session', 'attributes': {'reason': 'match_end'}},
            {'event_type': 'BROWSE_PAGE', 'device': 'Smart TV', 'page_path': '/browse/highlights', 'content_category': 'Sports', 'attributes': {'page_name': 'Highlights'}},
            {'event_type': 'CLICK_CONTENT', 'device': 'Smart TV', 'page_path': '/browse/highlights', 'content_type': 'short_form', 'content_category': 'Sports', 'content_group': 'highlight', 'content_action': 'set', 'attributes': {'from_page': 'highlights'}},
            {'event_type': 'PLAY_START', 'device': 'Mobile', 'content_type': 'short_form', 'content_category': 'Sports', 'content_group': 'highlight', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'ADD_TO_MY_LIST', 'device': 'Mobile', 'page_path': '/watchlist', 'content_type': 'docuseries', 'content_category': 'Documentary', 'content_group': 'docu_follow', 'content_action': 'set', 'attributes': {'action': 'add', 'list_name': 'Sports Stories'}}
        ]
    },
    'DISAPPOINTED': {
        'name': 'Discovery and Friction Journey',
        'events': [
            {'event_type': 'BROWSE_PAGE', 'device': 'Web', 'page_path': '/browse/international-cinema', 'content_category': 'Documentary', 'attributes': {'page_name': 'International Cinema'}},
            {'event_type': 'CLICK_CONTENT', 'device': 'Web', 'page_path': '/browse/international-cinema', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'primary', 'content_action': 'set', 'attributes': {'from_page': 'international_cinema'}},
            {'event_type': 'PLAY_START', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'primary', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'REBUFFER_EVENT', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'primary', 'attributes': {'duration': 8}},
            {'event_type': 'ERROR_EVENT', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'primary', 'attributes': {'error_code': 'PLAYBACK_TIMEOUT'}},
            {'event_type': 'PLAY_STOP', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'primary', 'attributes': {'reason': 'error'}},
            {'event_type': 'SETTINGS_UPDATE', 'device': 'Web', 'page_path': '/settings/playback', 'attributes': {'setting': 'playback_quality', 'old_value': 'Auto', 'new_value': 'Auto'}},
            {'event_type': 'PLAY_RESUME', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'primary', 'attributes': {}},
            {'event_type': 'PLAY_STOP', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'primary', 'attributes': {'reason': 'user_exit'}},
            {'event_type': 'SEARCH_QUERY', 'device': 'Web', 'page_path': '/search', 'attributes': {'query': 'indie festival', 'filters': ['movie']}},
            {'event_type': 'PLAY_START', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'alternate', 'content_action': 'set', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'PLAY_STOP', 'device': 'Web', 'content_type': 'movie', 'content_category': 'Documentary', 'content_group': 'alternate', 'attributes': {'reason': 'short_session'}},
            {'event_type': 'CANCELLATION_SURVEY_SUBMIT', 'device': 'Web', 'page_path': '/account/cancel', 'attributes': {'reason': 'technical issues', 'comment': 'Buffering too frequent'}}
        ]
    },
    'CROSS_PLATFORM': {
        'name': 'Cross-Platform Family Journey',
        'events': [
            {'event_type': 'PROFILE_VIEW', 'device': 'Smart TV', 'page_path': '/profiles', 'attributes': {'profile_id': 'kids'}},
            {'event_type': 'BROWSE_PAGE', 'device': 'Smart TV', 'page_path': '/browse/animation', 'content_category': 'Kids', 'attributes': {'page_name': 'Animation'}},
            {'event_type': 'PLAY_START', 'device': 'Tablet', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'content_action': 'set', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'PAUSE', 'device': 'Tablet', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {'reason': 'parent_pause'}},
            {'event_type': 'ADD_TO_MY_LIST', 'device': 'Tablet', 'page_path': '/watchlist/kids', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {'action': 'add', 'list_name': 'Kids Favorites'}},
            {'event_type': 'PLAY_STOP', 'device': 'Tablet', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {'reason': 'break'}},
            {'event_type': 'PLAY_RESUME', 'device': 'Mobile', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {}},
            {'event_type': 'SKIP_INTRO', 'device': 'Mobile', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {}},
            {'event_type': 'PLAY_STOP', 'device': 'Mobile', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {'reason': 'finish_episode'}},
            {'event_type': 'PLAY_RESUME', 'device': 'Smart TV', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {}},
            {'event_type': 'SETTINGS_UPDATE', 'device': 'Web', 'page_path': '/settings/parental-controls', 'attributes': {'setting': 'parental_controls', 'old_value': 'Teen', 'new_value': 'Teen'}},
            {'event_type': 'PLAY_STOP', 'device': 'Smart TV', 'content_type': 'kids', 'content_category': 'Kids', 'content_group': 'kids_session', 'attributes': {'reason': 'bedtime'}}
        ]
    },
    'CHURN_RISK': {
        'name': 'Churn-Risk Countdown Journey',
        'events': [
            {'event_type': 'PLAY_START', 'device': 'Smart TV', 'content_type': 'reality', 'content_category': 'Reality', 'content_group': 'reality_session', 'content_action': 'set', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'PLAY_RESUME', 'device': 'Smart TV', 'content_type': 'reality', 'content_category': 'Reality', 'content_group': 'reality_session', 'attributes': {}},
            {'event_type': 'SKIP_INTRO', 'device': 'Smart TV', 'content_type': 'reality', 'content_category': 'Reality', 'content_group': 'reality_session', 'attributes': {}},
            {'event_type': 'PLAY_STOP', 'device': 'Smart TV', 'content_type': 'reality', 'content_category': 'Reality', 'content_group': 'reality_session', 'attributes': {'reason': 'mid_episode_stop'}},
            {'event_type': 'REBUFFER_EVENT', 'device': 'Smart TV', 'content_type': 'reality', 'content_category': 'Reality', 'content_group': 'reality_session', 'attributes': {'duration': 4}},
            {'event_type': 'PLAY_RESUME', 'device': 'Smart TV', 'content_type': 'reality', 'content_category': 'Reality', 'content_group': 'reality_session', 'attributes': {}},
            {'event_type': 'PLAY_STOP', 'device': 'Smart TV', 'content_type': 'reality', 'content_category': 'Reality', 'content_group': 'reality_session', 'attributes': {'reason': 'episode_end'}},
            {'event_type': 'VIEW_SUBSCRIPTION_PAGE', 'device': 'Web', 'page_path': '/account/subscription', 'attributes': {'current_tier': 'standard'}},
            {'event_type': 'CANCELLATION_SURVEY_SUBMIT', 'device': 'Web', 'page_path': '/account/cancel', 'attributes': {'reason': 'content fatigue'}},
            {'event_type': 'BROWSE_PAGE', 'device': 'Web', 'page_path': '/campaign/email-landing', 'content_category': 'Reality', 'attributes': {'page_name': 'Email Campaign Landing'}}
        ]
    },
    'RETARGETING': {
        'name': 'Retargeting Awakening Journey',
        'events': [
            {'event_type': 'CAMPAIGN_CLICK', 'device': 'Web', 'page_path': '/campaigns/come-back', 'content_type': 'docuseries', 'content_category': 'Lifestyle', 'content_group': 'campaign', 'content_action': 'set', 'attributes': {'campaign': 'Come Back'}},
            {'event_type': 'PLAY_START', 'device': 'Web', 'content_type': 'docuseries', 'content_category': 'Lifestyle', 'content_group': 'campaign', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'ADD_TO_MY_LIST', 'device': 'Web', 'page_path': '/watchlist', 'content_type': 'docuseries', 'content_category': 'Lifestyle', 'content_group': 'campaign', 'attributes': {'action': 'add', 'list_name': 'Docuseries Weekend'}},
            {'event_type': 'PLAY_RESUME', 'device': 'Web', 'content_type': 'docuseries', 'content_category': 'Lifestyle', 'content_group': 'campaign', 'attributes': {}},
            {'event_type': 'PLAY_STOP', 'device': 'Web', 'content_type': 'docuseries', 'content_category': 'Lifestyle', 'content_group': 'campaign', 'attributes': {'reason': 'session_end'}},
            {'event_type': 'BROWSE_PAGE', 'device': 'Web', 'page_path': '/browse/wellness', 'content_category': 'Lifestyle', 'attributes': {'page_name': 'Wellness'}},
            {'event_type': 'PLAY_START', 'device': 'Mobile', 'content_type': 'short_form', 'content_category': 'Lifestyle', 'content_group': 'short_form', 'content_action': 'set', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'PLAY_STOP', 'device': 'Mobile', 'content_type': 'short_form', 'content_category': 'Lifestyle', 'content_group': 'short_form', 'attributes': {'reason': 'completed_clip'}},
            {'event_type': 'EMAIL_OPENED', 'device': 'Mobile', 'page_path': None, 'attributes': {'campaign': 'Docuseries Weekend'}}
        ]
    },
    'ACQUISITION': {
        'name': 'Subscriber Acquisition Funnel',
        'events': [
            {'event_type': 'CAMPAIGN_CLICK', 'device': 'Web', 'page_path': '/campaigns/original-premiere', 'content_type': 'preview', 'content_category': 'Originals', 'content_group': 'promo', 'content_action': 'set', 'attributes': {'campaign': 'Original Premiere'}},
            {'event_type': 'BROWSE_PAGE', 'device': 'Web', 'page_path': '/landing/originals', 'content_category': 'Originals', 'attributes': {'page_name': 'Landing'}},
            {'event_type': 'CLICK_CONTENT', 'device': 'Web', 'page_path': '/landing/originals', 'content_type': 'preview', 'content_category': 'Originals', 'content_group': 'promo', 'attributes': {'from_page': 'landing'}},
            {'event_type': 'PLAY_START', 'device': 'Web', 'content_type': 'preview', 'content_category': 'Originals', 'content_group': 'promo', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'PLAY_STOP', 'device': 'Web', 'content_type': 'preview', 'content_category': 'Originals', 'content_group': 'promo', 'attributes': {'reason': 'preview_end'}},
            {'event_type': 'VIEW_SUBSCRIPTION_PAGE', 'device': 'Web', 'page_path': '/account/subscription', 'attributes': {'current_tier': 'trial'}},
            {'event_type': 'UPGRADE_PAGE_VIEW', 'device': 'Web', 'page_path': '/account/upgrade', 'attributes': {'from_tier': 'trial', 'to_tier': 'standard'}},
            {'event_type': 'PROFILE_VIEW', 'device': 'Smart TV', 'page_path': '/profiles', 'attributes': {'profile_id': 'primary'}},
            {'event_type': 'PLAY_START', 'device': 'Smart TV', 'content_type': 'original', 'content_category': 'Originals', 'content_group': 'original', 'content_action': 'set', 'attributes': {'play_mode': 'on_demand'}},
            {'event_type': 'ADD_TO_MY_LIST', 'device': 'Smart TV', 'page_path': '/watchlist', 'content_type': 'original', 'content_category': 'Originals', 'content_group': 'original', 'attributes': {'action': 'add', 'list_name': 'Originals'}}
        ]
    }
}

def _persona_categories(persona: str) -> List[str]:
    return PERSONA_CONTENT_CATEGORIES.get((persona or '').upper(), [])


def _choose_content(content_type: Optional[str], persona: str, event_category: Optional[str], rng: random.Random) -> Optional[Dict]:
    if not content_type:
        return None

    candidates = [c for c in CONTENT_CATALOG if c['content_type'] == content_type]

    if event_category:
        target = event_category.upper()
        filtered = [c for c in candidates if c['content_category'].upper() == target]
        if filtered:
            candidates = filtered
    else:
        preferred = _persona_categories(persona)
        if preferred:
            filtered = [c for c in candidates if c['content_category'] in preferred]
            if filtered:
                candidates = filtered

    if not candidates:
        candidates = [c for c in CONTENT_CATALOG if c['content_type'] == content_type]
    if not candidates:
        candidates = CONTENT_CATALOG

    return dict(rng.choice(candidates))


PERSONA_MAP = {
    'HIGH-VALUE USER': ['HIGH_VALUE', 'ACQUISITION', 'RETARGETING'],
    'HIGH VALUE USER': ['HIGH_VALUE', 'ACQUISITION', 'RETARGETING'],
    'POWER USER': ['POWER_USER', 'HIGH_VALUE'],
    'DISAPPOINTED USER': ['DISAPPOINTED', 'CHURN_RISK'],
    'CROSS-PLATFORM USER': ['CROSS_PLATFORM', 'HIGH_VALUE'],
    'CHURN-RISK USER': ['CHURN_RISK', 'RETARGETING'],
    'RETARGETING CANDIDATE': ['RETARGETING', 'CROSS_PLATFORM'],
    'SUBSCRIBER ACQUISITION CANDIDATE': ['ACQUISITION', 'RETARGETING']
}

DEFAULT_JOURNEYS = ['HIGH_VALUE', 'POWER_USER', 'DISAPPOINTED', 'CROSS_PLATFORM', 'CHURN_RISK', 'RETARGETING', 'ACQUISITION']

DEVICE_FALLBACKS = ['Smart TV', 'Mobile', 'Tablet', 'Web']

MAX_EVENTS_PER_JOURNEY = 20
CHURN_JOURNEY_CODE = 'CHURN_RISK'
MONTHLY_JOURNEY_MIN = 1
MONTHLY_JOURNEY_MAX = 3


def _candidate_journeys(persona_key: str) -> List[str]:
    persona_key = (persona_key or '').upper()
    candidates = PERSONA_MAP.get(persona_key, DEFAULT_JOURNEYS)
    return candidates if candidates else DEFAULT_JOURNEYS.copy()


def _journey_events(journey_code: str) -> Dict:
    template = JOURNEY_LIBRARY.get(journey_code, JOURNEY_LIBRARY['HIGH_VALUE'])
    return template


def _generate_event_id(unique_id: str, index: int) -> str:
    raw = f"{unique_id}|{index}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:32]


def _isoformat(dt: pendulum.DateTime) -> str:
    return dt.to_iso8601_string()


class ClickstreamGenerator:
    def process(self, unique_id, persona, archetype, base_start_date, month_count, timezone):
        rng_seed = int(hashlib.sha256(f"{unique_id}|{persona}|{archetype}".encode('utf-8')).hexdigest(), 16) % (10**12)
        rng = random.Random(rng_seed)

        tz = timezone or 'UTC'

        if base_start_date is None:
            base_dt = pendulum.now(tz).subtract(months=month_count or 1)
        elif hasattr(base_start_date, 'year'):
            base_dt = pendulum.datetime(base_start_date.year, base_start_date.month, base_start_date.day, tz=tz)
        else:
            base_dt = pendulum.parse(str(base_start_date)).in_timezone(tz)

        month_count = max(int(month_count or 1), 1)
        window_start = base_dt.start_of('day')
        window_end = window_start.add(months=month_count)

        candidate_journeys = _candidate_journeys(persona)

        event_index = 0
        churned = False

        # Emit a SIGN_UP event at window_start to mark join time
        sign_up_id = _generate_event_id(unique_id, event_index)
        yield (
            sign_up_id,
            'ACQUISITION',
            'Subscriber Acquisition Funnel',
            f"SES_{unique_id}_SIGNUP_{rng.randint(1000,999999)}",
            'SIGN_UP',
            _isoformat(window_start),
            'Web',
            '/account/join',
            None,
            None,
            None,
            {'source': 'generator_signup'},
            persona,
            archetype
        )
        event_index += 1

        # Initialize sequential journey scheduling from signup time
        next_journey_start = window_start

        for month_offset in range(month_count):
            if churned:
                break

            month_anchor = window_start.add(months=month_offset)
            month_start = month_anchor.start_of('month')
            if month_start < window_start:
                month_start = window_start
            if month_start >= window_end:
                break
            month_end = month_start.add(months=1)
            days_in_month = max(1, (month_end - month_start).in_days())

            month_events = []

            # Lifecycle ramp: first month after signup is lighter
            journeys_this_month = rng.randint(MONTHLY_JOURNEY_MIN, MONTHLY_JOURNEY_MAX) if month_offset > 0 else 1

            # Ensure next journey start is not before the current month start
            if next_journey_start < month_start:
                next_journey_start = month_start

            for journey_iteration in range(journeys_this_month):
                if churned:
                    break

                journey_code = rng.choice(candidate_journeys)
                template = _journey_events(journey_code)
                journey_name = template.get('name', journey_code)
                events = template.get('events', [])[:MAX_EVENTS_PER_JOURNEY]
                if not events:
                    continue

                # Start the journey at the planned next_journey_start (with minor jitter)
                session_start = next_journey_start.add(minutes=rng.randint(0, 15))
                if session_start >= window_end:
                    continue
                if session_start >= month_end:
                    break

                session_id = f"SES_{unique_id}_{journey_code}_{month_offset}_{journey_iteration}_{rng.randint(1000, 999999)}"
                content_groups: Dict[str, Dict] = {}
                event_time = session_start
                last_event_time = session_start

                for event in events:
                    event_time = event_time.add(minutes=rng.randint(5, 120))
                    if event_time > window_end:
                        continue
                    last_event_time = event_time

                    device = event.get('device') or rng.choice(DEVICE_FALLBACKS)

                    base_attributes = dict(event.get('attributes', {}))
                    event_content_type = event.get('content_type')
                    event_category = event.get('content_category')
                    content_info: Optional[Dict] = None

                    if event_content_type:
                        group_name = event.get('content_group') or f"default_{month_offset}"
                        action = event.get('content_action', 'reuse').lower()
                        if action == 'set' or group_name not in content_groups:
                            content_groups[group_name] = _choose_content(event_content_type, persona, event_category, rng)
                        content_info = dict(content_groups[group_name])

                    content_category = event_category
                    if content_info:
                        base_attributes.setdefault('content_title', content_info.get('title'))
                        content_category = content_category or content_info.get('content_category')

                    page_path = event.get('page_path')
                    if not page_path and content_info:
                        page_path = content_info.get('page_path')
                    if not page_path and content_category:
                        slug = content_category.lower().replace(' ', '-').replace('&', 'and')
                        page_path = f"/browse/{slug}"
                    if page_path:
                        page_path = '/' + page_path.lstrip('/')

                    month_events.append({
                        'event_id': _generate_event_id(unique_id, event_index),
                        'journey_code': journey_code,
                        'journey_name': journey_name,
                        'event_type': (event.get('event_type') or '').upper(),
                        'timestamp': event_time,
                        'device': device,
                        'session_id': session_id,
                        'page_path': page_path,
                        'content_id': content_info.get('content_id') if content_info else None,
                        'content_type': content_info.get('content_type') if content_info else event_content_type,
                        'content_category': content_category,
                        'attributes': base_attributes
                    })
                    event_index += 1

                if journey_code.upper() == CHURN_JOURNEY_CODE:
                    churned = True
                    break

                # Schedule next journey after a short gap following the last event
                gap_days = rng.randint(1, 5)
                gap_minutes = rng.randint(30, 360)
                next_journey_start = last_event_time.add(days=gap_days, minutes=gap_minutes)

            month_events.sort(key=lambda e: e['timestamp'])
            for event in month_events:
                yield (
                    event['event_id'],
                    event['journey_code'],
                    event['journey_name'],
                    event['session_id'],
                    event['event_type'],
                    _isoformat(event['timestamp']),
                    event['device'],
                    event['page_path'],
                    event['content_id'],
                    event['content_type'],
                    event['content_category'],
                    event['attributes'],
                    persona,
                    archetype
                )
$$;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS AS
WITH params AS (
    SELECT
        TO_DATE($CLICKSTREAM_START_DATE) AS base_start_date,
        $CLICKSTREAM_MONTH_COUNT::INT AS month_count,
        $CLICKSTREAM_TIMEZONE::STRING AS timezone
),
subscriber_source AS (
    SELECT
        fp.UNIQUE_ID,
        fp.PERSONA,
        fp.ARCHETYPE,
        -- Respect signup_date by shifting start forward
        GREATEST(p.base_start_date, COALESCE(fp.SIGNUP_DATE, p.base_start_date)) AS base_start_date,
        -- Reduce month_count accordingly
        GREATEST(0, $CLICKSTREAM_MONTH_COUNT - DATEDIFF(month, p.base_start_date, GREATEST(p.base_start_date, COALESCE(fp.SIGNUP_DATE, p.base_start_date)))) AS month_count,
        p.timezone
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES fp
    CROSS JOIN params p
),
events AS (
    SELECT
        ss.UNIQUE_ID,
        e.event_id,
        e.journey_code,
        e.journey_name,
        e.session_id,
        e.event_type,
        e.event_ts,
        e.device,
        e.page_path,
        e.content_id,
        e.content_type,
        e.content_category,
        e.attributes,
        e.persona,
        e.archetype
    FROM subscriber_source ss,
         TABLE(
            AME_AD_SALES_DEMO.GENERATE.GENERATE_SUBSCRIBER_CLICKSTREAM(
                ss.UNIQUE_ID,
                ss.PERSONA,
                ss.ARCHETYPE,
                ss.base_start_date,
                ss.month_count,
                ss.timezone
            )
         ) e
)
SELECT
    events.UNIQUE_ID,
    events.event_id,
    events.session_id,
    events.journey_code,
    events.journey_name,
    events.persona,
    events.archetype,
    TO_TIMESTAMP_NTZ(events.event_ts) AS event_ts,
    events.event_type,
    events.device,
    events.page_path,
    events.content_id,
    events.content_type,
    events.content_category,
    events.attributes
FROM events;




SELECT 
    'Rows in GENERATE.CLICKSTREAM_EVENTS' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS
UNION ALL 
SELECT 
    'Unique profiles in GENERATE.CLICKSTREAM_EVENTS' as "Table created", 
    count(DISTINCT unique_id) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS
UNION ALL 
SELECT 
    'Event types in GENERATE.CLICKSTREAM_EVENTS' as "Table created", 
    count(DISTINCT event_type) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS

;



-- ---------------------------------------------------------------------------
-- 07-generate-ad-campaigns-and-events.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

SET ROLLING_MONTH_WINDOW = 6;
SET AD_CAMPAIGN_COUNT = 160;
SET AD_CAMPAIGN_START_DATE = DATEADD(month, -$ROLLING_MONTH_WINDOW, CURRENT_DATE());
SET AD_CAMPAIGN_END_DATE = CURRENT_DATE();
SET ADS_MAX_IMPRESSION_EVENTS = 1000;
SET ADS_MAX_CLICK_EVENTS = 200;
SET ADS_MIN_IMPRESSION_EVENTS = 20;
SET ADS_MIN_CLICK_EVENTS = 5;
SET ADS_EVENTS_IMPRESSION_SAMPLE_RATIO = 0.1;
SET ADS_EVENTS_CLICK_SAMPLE_RATIO = 0.15;
-- Seasonality base multipliers and ±25% randomization per month
CREATE OR REPLACE TEMP TABLE TMP_AD_SEASONAL_BASE(month_num INT, base_mult FLOAT) AS
SELECT * FROM VALUES
  (1, 0.92),(2, 0.95),(3, 0.98),(4, 1.00),(5, 1.03),(6, 1.06),
  (7, 1.10),(8, 1.08),(9, 1.05),(10, 1.07),(11, 1.12),(12, 1.15);
SET AD_SEASONAL_RANDOM_AMPLITUDE = 0.25;

/*
    Step 1: Create campaign catalogue for sold inventory.
    Campaign metadata mirrors the blueprint described in docs/ad_performance.md
*/
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.AD_CAMPAIGNS AS
WITH params AS (
    SELECT
        TO_DATE($AD_CAMPAIGN_START_DATE) AS global_start,
        TO_DATE($AD_CAMPAIGN_END_DATE) AS global_end,
        GREATEST(DATEDIFF(day, TO_DATE($AD_CAMPAIGN_START_DATE), TO_DATE($AD_CAMPAIGN_END_DATE)), 0) AS range_days
),
base AS (
    SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => $AD_CAMPAIGN_COUNT))
),
advertisers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY name) - 1 AS idx, name, vertical
    FROM (
        SELECT * FROM VALUES
            ('Falcon Motors', 'Auto'),
            ('Evergreen Outfitters', 'Retail'),
            ('BluePeak Financial', 'Finance'),
            ('Pulse Electronics', 'Technology'),
            ('Glow Cosmetics', 'Beauty'),
            ('TrailMix Snacks', 'CPG'),
            ('SwiftAirlines', 'Travel'),
            ('Urban Threads', 'Retail'),
            ('BrightFuture University', 'Education'),
            ('Northwind Insurance', 'Finance')
    ) v(name, vertical)
),
segments AS (
    SELECT ROW_NUMBER() OVER (ORDER BY category) - 1 AS idx,
           category,
           personas,
           devices,
           cpm_min,
           cpm_max,
           ctr_min,
           ctr_max,
           cap_min,
           cap_max
    FROM (
        SELECT 'Sports Enthusiasts' AS category,
               ARRAY_CONSTRUCT('Power User', 'High-Value User') AS personas,
               ARRAY_CONSTRUCT('Smart TV', 'Mobile') AS devices,
               12.0 AS cpm_min,
               24.0 AS cpm_max,
               0.004 AS ctr_min,
               0.018 AS ctr_max,
               600 AS cap_min,
               1400 AS cap_max
        UNION ALL SELECT 'Family Entertainment', ARRAY_CONSTRUCT('Cross-Platform User', 'Kids Content Guardian', 'Lifestyle Sampler'), ARRAY_CONSTRUCT('Smart TV', 'Tablet'), 10.0, 22.0, 0.0035, 0.015, 500, 1200
        UNION ALL SELECT 'Premium Drama', ARRAY_CONSTRUCT('High-Value User', 'Subscriber Acquisition Candidate'), ARRAY_CONSTRUCT('Smart TV', 'Web'), 16.0, 32.0, 0.005, 0.02, 550, 1300
        UNION ALL SELECT 'Lifestyle & Wellness', ARRAY_CONSTRUCT('Lifestyle Sampler', 'Retargeting Candidate'), ARRAY_CONSTRUCT('Mobile', 'Web'), 8.0, 18.0, 0.004, 0.016, 400, 1000
        UNION ALL SELECT 'Documentary & Knowledge', ARRAY_CONSTRUCT('Documentary Devotee', 'High-Value User'), ARRAY_CONSTRUCT('Smart TV', 'Web'), 11.0, 25.0, 0.003, 0.012, 450, 1100
        UNION ALL SELECT 'Comedy Nights', ARRAY_CONSTRUCT('Comedy Night Casual', 'Cross-Platform User'), ARRAY_CONSTRUCT('Smart TV', 'Mobile'), 9.0, 19.0, 0.0045, 0.014, 420, 950
    ) segment_rows
),
rate_types AS (
    SELECT ROW_NUMBER() OVER (ORDER BY rate_type) - 1 AS idx, rate_type
    FROM (
        SELECT 'CPM' UNION ALL SELECT 'Fixed' UNION ALL SELECT 'Hybrid'
    ) r(rate_type)
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM advertisers) AS adv_count,
        (SELECT COUNT(*) FROM segments) AS seg_count,
        (SELECT COUNT(*) FROM rate_types) AS rate_count
),
random_seed AS (
    SELECT UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT AS base_rand
),
campaigns AS (
    SELECT
        CONCAT('AD_', LPAD(base.seq::STRING, 4, '0')) AS campaign_id,
        adv.name AS advertiser_name,
        adv.vertical,
        seg.category AS content_category,
        rate.rate_type,
        seg.personas AS target_personas,
        seg.devices AS target_devices,
        DATEADD(day,
                CASE WHEN params.range_days > 0 THEN MOD(RANDOM(), params.range_days + 1) ELSE 0 END,
                params.global_start) AS raw_start,
        seg.cap_min,
        seg.cap_max,
        seg.cpm_min,
        seg.cpm_max,
        seg.ctr_min,
        seg.ctr_max,
        rate.rate_type AS rate_type_label,
        base.seq,
        params.global_end,
        params.range_days
    FROM base
    CROSS JOIN params
    CROSS JOIN counts
    JOIN advertisers adv
      ON adv.idx = MOD(base.seq - 1, counts.adv_count)
    JOIN segments seg
      ON seg.idx = MOD(base.seq + 16, counts.seg_count)
    JOIN rate_types rate
      ON rate.idx = MOD(base.seq + 32, counts.rate_count)
)
SELECT
    campaign_id,
    advertiser_name,
    vertical,
    content_category,
    rate_type,
    raw_start AS booking_start,
    LEAST(global_end,
          DATEADD(day,
                  14 + MOD(RANDOM(), 47),
                  raw_start)) AS booking_end,
    CAST(cap_min + FLOOR(UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT * GREATEST(cap_max - cap_min + 1, 1)) AS INT) AS daily_impression_cap,
    rate_type_label AS rate_type_detail,
    ROUND(cpm_min + (FLOOR(UNIFORM(0.0::FLOAT, 1.0::FLOAT, UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT)::FLOAT * GREATEST((cpm_max - cpm_min) * 100, 1)) / 100.0), 2) AS booked_cpm,
    ROUND(ctr_min + (FLOOR(UNIFORM(0.0::FLOAT, 1.0::FLOAT, UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT)::FLOAT * GREATEST((ctr_max - ctr_min) * 10000, 1)) / 10000.0), 5) AS booked_ctr,
    1 + CAST(FLOOR(UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT * 4) AS INT) AS creative_count,
    target_personas,
    target_devices
FROM campaigns;

/*
    Step 2: Generate daily performance results (AD_PERFORMANCE_EVENTS)
*/
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.AD_PERFORMANCE_EVENTS AS
WITH params AS (
    SELECT
        TO_DATE($AD_CAMPAIGN_START_DATE) AS global_start,
        TO_DATE($AD_CAMPAIGN_END_DATE) AS global_end
),
campaign_span AS (
    SELECT
        c.campaign_id,
        c.advertiser_name,
        c.vertical,
        c.content_category,
        c.rate_type,
        c.daily_impression_cap,
        c.booked_cpm,
        c.booked_ctr,
        c.creative_count,
        c.target_personas,
        c.target_devices,
        GREATEST(c.booking_start, params.global_start) AS effective_start,
        LEAST(c.booking_end, params.global_end) AS effective_end
    FROM AME_AD_SALES_DEMO.GENERATE.AD_CAMPAIGNS c
    CROSS JOIN params
    WHERE c.booking_end >= params.global_start
      AND c.booking_start <= params.global_end
),
calendar_expanded AS (
    SELECT
        cs.campaign_id,
        cs.advertiser_name,
        cs.vertical,
        cs.content_category,
        cs.rate_type,
        cs.daily_impression_cap,
        cs.booked_cpm,
        cs.booked_ctr,
        cs.creative_count,
        cs.target_personas,
        cs.target_devices,
        cs.effective_start,
        DATEADD(day, idx.value, cs.effective_start) AS metric_date
    FROM campaign_span cs,
         LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, DATEDIFF(day, cs.effective_start, cs.effective_end) + 1)) idx
),
metrics AS (
    SELECT
        ce.metric_date,
        ce.campaign_id,
        ce.advertiser_name,
        ce.vertical,
        ce.content_category,
        ce.rate_type,
        ce.daily_impression_cap,
        ce.booked_cpm,
        ce.booked_ctr,
        ce.creative_count,
        ce.target_personas,
        ce.target_devices,
        -- randomised delivery with seasonality (joined, no scalar subquery)
        GREATEST(
          1,
          FLOOR(
            ce.daily_impression_cap
            * (sb.base_mult * (1 + (UNIFORM(-$AD_SEASONAL_RANDOM_AMPLITUDE::FLOAT, $AD_SEASONAL_RANDOM_AMPLITUDE::FLOAT, RANDOM())::FLOAT)))
            * (1 + ((UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT * 0.30) - 0.15))
          )
        ) AS impressions_raw,
        ce.booked_cpm * (1 + ((UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT * 0.20) - 0.10)) AS cpm_raw,
        ce.booked_ctr * (1 + ((UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT * 0.30) - 0.15)) AS ctr_raw
    FROM calendar_expanded ce
    JOIN TMP_AD_SEASONAL_BASE sb
      ON sb.month_num = MONTH(ce.metric_date)
)
SELECT
    metric_date,
    campaign_id,
    advertiser_name,
    vertical,
    content_category,
    rate_type,
    impressions_raw AS impressions,
    LEAST(impressions_raw, CAST(impressions_raw * ctr_raw AS NUMBER(18,0))) AS clicks,
    ROUND(CASE WHEN impressions_raw > 0 THEN LEAST(1.0, ctr_raw)::FLOAT ELSE 0 END, 6) AS ctr,
    ROUND(GREATEST(0.01, cpm_raw)::FLOAT, 4) AS cpm,
    ROUND((impressions_raw * cpm_raw)::FLOAT / 1000.0, 4) AS spend,
    creative_count,
    target_personas,
    target_devices,
    booked_cpm,
    booked_ctr,
    daily_impression_cap
FROM metrics;

/*
    Step 3: Generate impression and click-through events with weights.
    Events align with campaign personas and subscriber profiles.
*/
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS AS
WITH metrics AS (
    SELECT
        ape.*,
        CASE
            WHEN ape.impressions <= 0 THEN 0
            ELSE LEAST(
                ape.impressions,
                $ADS_MAX_IMPRESSION_EVENTS,
                GREATEST($ADS_MIN_IMPRESSION_EVENTS, ROUND(ape.impressions * $ADS_EVENTS_IMPRESSION_SAMPLE_RATIO))
            )
        END AS impression_sample_size,
        CASE
            WHEN ape.clicks <= 0 THEN 0
            ELSE LEAST(
                ape.clicks,
                $ADS_MAX_CLICK_EVENTS,
                GREATEST($ADS_MIN_CLICK_EVENTS, ROUND(ape.clicks * $ADS_EVENTS_CLICK_SAMPLE_RATIO))
            )
        END AS click_sample_size
    FROM AME_AD_SALES_DEMO.GENERATE.AD_PERFORMANCE_EVENTS ape
),
persona_arrays AS (
    SELECT
        UPPER(persona) AS persona_key,
        ARRAY_AGG(unique_id) WITHIN GROUP (ORDER BY unique_id) AS subscriber_ids
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    GROUP BY 1
),
all_subscribers AS (
    SELECT ARRAY_AGG(unique_id) WITHIN GROUP (ORDER BY unique_id) AS subscriber_ids
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
),
device_pool AS (
    SELECT ARRAY_CONSTRUCT('Smart TV', 'Mobile', 'Tablet', 'Web') AS devices
),
impression_events AS (
    SELECT
        m.metric_date,
        m.campaign_id,
        m.content_category,
        'IMPRESSION' AS event_type,
        seq.value::INT AS sample_index_raw,
        CASE
            WHEN m.impression_sample_size = 0 THEN 0
            ELSE FLOOR(m.impressions / m.impression_sample_size) + CASE WHEN seq.value < (m.impressions % m.impression_sample_size) THEN 1 ELSE 0 END
        END AS event_weight,
        m.target_personas,
        m.target_devices,
        DATEADD(second, CAST(UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT * 86400 AS INT), m.metric_date::TIMESTAMP_NTZ) AS base_ts
    FROM metrics m,
         LATERAL FLATTEN(INPUT => CASE WHEN m.impression_sample_size > 0 THEN ARRAY_GENERATE_RANGE(0, m.impression_sample_size) ELSE ARRAY_CONSTRUCT() END) seq
    WHERE m.impression_sample_size > 0
),
click_events AS (
    SELECT
        m.metric_date,
        m.campaign_id,
        m.content_category,
        'CLICK' AS event_type,
        seq.value::INT AS sample_index_raw,
        CASE
            WHEN m.click_sample_size = 0 THEN 0
            ELSE FLOOR(m.clicks / m.click_sample_size) + CASE WHEN seq.value < (m.clicks % m.click_sample_size) THEN 1 ELSE 0 END
        END AS event_weight,
        m.target_personas,
        m.target_devices,
        DATEADD(second, CAST(UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM())::FLOAT * 86400 AS INT), m.metric_date::TIMESTAMP_NTZ) AS base_ts
    FROM metrics m,
         LATERAL FLATTEN(INPUT => CASE WHEN m.click_sample_size > 0 THEN ARRAY_GENERATE_RANGE(0, m.click_sample_size) ELSE ARRAY_CONSTRUCT() END) seq
    WHERE m.click_sample_size > 0
),
combined_events AS (
    SELECT metric_date, campaign_id, content_category, event_type, sample_index_raw, event_weight, target_personas, target_devices, base_ts
    FROM impression_events
    UNION ALL
    SELECT metric_date, campaign_id, content_category, event_type, sample_index_raw, event_weight, target_personas, target_devices, base_ts
    FROM click_events
),
annotated AS (
    SELECT
        ce.metric_date,
        ce.campaign_id,
        ce.content_category,
        ce.event_type,
        ce.sample_index_raw,
        ce.event_weight,
        ce.target_personas,
        ce.target_devices,
        ce.base_ts,
        CASE
            WHEN ARRAY_SIZE(ce.target_personas) > 0 THEN GET(ce.target_personas, MOD(ce.sample_index_raw, ARRAY_SIZE(ce.target_personas)))
            ELSE 'GLOBAL'
        END AS persona_key,
        pa.subscriber_ids,
        ARRAY_SIZE(pa.subscriber_ids) AS subscriber_count,
        CASE
            WHEN ARRAY_SIZE(ce.target_devices) > 0 THEN ce.target_devices
            ELSE dp.devices
        END AS device_array,
        CASE
            WHEN ARRAY_SIZE(ce.target_devices) > 0 THEN ARRAY_SIZE(ce.target_devices)
            ELSE ARRAY_SIZE(dp.devices)
        END AS device_count
    FROM combined_events ce
    LEFT JOIN persona_arrays pa
      ON pa.persona_key = UPPER(
            CASE
                WHEN ARRAY_SIZE(ce.target_personas) > 0 THEN GET(ce.target_personas, MOD(ce.sample_index_raw, ARRAY_SIZE(ce.target_personas)))
                ELSE 'GLOBAL'
            END)
    LEFT JOIN device_pool dp ON TRUE
    WHERE ce.event_weight > 0
),
subscriber_resolved AS (
    SELECT
        a.metric_date,
        a.campaign_id,
        a.content_category,
        a.event_type,
        a.event_weight,
        a.base_ts,
        UPPER(a.persona_key) AS persona_key,
        CASE
            WHEN ARRAY_SIZE(a.subscriber_ids) > 0 THEN GET(a.subscriber_ids, MOD(a.sample_index_raw, ARRAY_SIZE(a.subscriber_ids)))
            ELSE GET(allp.subscriber_ids, MOD(a.sample_index_raw, ARRAY_SIZE(allp.subscriber_ids)))
        END AS unique_id_variant,
        CASE
            WHEN a.device_count > 0 THEN GET(a.device_array, MOD(a.sample_index_raw, a.device_count))
            ELSE GET(dp.devices, MOD(a.sample_index_raw, ARRAY_SIZE(dp.devices)))
        END AS device_variant,
        MOD(a.sample_index_raw, 10000) AS sample_index
    FROM annotated a
    LEFT JOIN all_subscribers allp ON TRUE
    LEFT JOIN device_pool dp ON TRUE
),
active_filtered AS (
    SELECT
        sr.*,
        fp.SIGNUP_DATE
    FROM subscriber_resolved sr
    LEFT JOIN AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES fp
      ON fp.UNIQUE_ID = sr.unique_id_variant::STRING
    WHERE fp.SIGNUP_DATE IS NULL OR fp.SIGNUP_DATE <= sr.metric_date
)
SELECT
    event_id,
    event_ts,
    metric_date AS event_date,
    campaign_id,
    content_category,
    event_type,
    event_weight,
    unique_id,
    CASE WHEN persona IS NULL THEN NULL ELSE INITCAP(persona) END AS persona,
    device
FROM (
    SELECT
        UUID_STRING() AS event_id,
        base_ts AS event_ts,
        metric_date,
        campaign_id,
        content_category,
        event_type,
        event_weight,
        COALESCE(unique_id_variant::STRING, GET(allp.subscriber_ids, 0)::STRING) AS unique_id,
        CASE WHEN persona_key = 'GLOBAL' THEN NULL ELSE persona_key END AS persona,
        COALESCE(device_variant::STRING, 'Smart TV') AS device,
        sample_index
    FROM active_filtered
    LEFT JOIN all_subscribers allp ON TRUE
) final_events;



SELECT 
    'Rows in GENERATE.AD_CAMPAIGNS' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.AD_CAMPAIGNS
UNION ALL 
SELECT 
    'Rows in GENERATE.AD_PERFORMANCE_EVENTS' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.AD_PERFORMANCE_EVENTS
UNION ALL 
SELECT 
    'Rows in GENERATE.ADS_EVENTS' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS

UNION ALL 
SELECT 
    'Unique profiles in GENERATE.ADS_EVENTS' as "Table created", 
    count(DISTINCT unique_id) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS

UNION ALL 
SELECT 
    'Unique campaign in GENERATE.ADS_EVENTS' as "Table created", 
    count(DISTINCT campaign_id) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS

UNION ALL 
SELECT 
    'Unique content category in GENERATE.ADS_EVENTS' as "Table created", 
    count(DISTINCT content_category) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS

;



-- ---------------------------------------------------------------------------
-- 08-enrich-subscriber-demographics.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

CREATE OR REPLACE FUNCTION AME_AD_SALES_DEMO.GENERATE.GENERATE_LAD_DEMOGRAPHIC_PROFILE(
    lad_code STRING,
    unique_id STRING,
    age_distribution ARRAY,
    marital_distribution ARRAY,
    family_distribution ARRAY,
    base_income STRING,
    base_education STRING,
    digital_index FLOAT,
    fast_fashion_index FLOAT,
    grocery_index FLOAT,
    financial_index FLOAT
) RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ()
HANDLER = 'generate_profile'
AS
$$
import random
import hashlib
from decimal import Decimal

AGE_RANGES = {
    'EARLY_CHILDHOOD': (0, 5),
    'CHILDHOOD_ELEMENTARY_SCHOOL': (6, 11),
    'ADOLESCENCE_TEEN_YEARS': (12, 17),
    'YOUNG_ADULTS_EARLY': (18, 24),
    'PRIME_SETTLING_FAMILY_FORMATION': (25, 34),
    'ESTABLISHED_CAREER_MIDDLE_AGE': (35, 54),
    'PRE-RETIREMENT_EMPTY_NESTERS': (55, 64),
    'PRE_RETIREMENT_EMPTY_NESTERS': (55, 64),
    'RETIREMENT_SENIOR_YEARS': (65, 85)
}

INCOME_LEVELS = ['LOWER', 'MID', 'UPPER-MID', 'HIGH']
EDUCATION_LEVELS = ['HIGH SCHOOL', 'SOME COLLEGE', 'GRADUATE', 'POST-GRAD']

MARITAL_LABELS = {
    'DEMO_MARITAL_SINGLE_NEVER_MARRIED': 'Single - Never Married',
    'DEMO_MARITAL_MARRIED_OR_CIVIL_PARTNERSHIP': 'Married / Civil Partnership',
    'DEMO_MARITAL_DIVORCED_OR_SEPARATED': 'Divorced / Separated',
    'DEMO_MARITAL_WIDOWED': 'Widowed'
}

FAMILY_LABELS = {
    'DEMO_FAMILY_COUPLES_NO_CHILDREN': 'Couple - No Children',
    'DEMO_FAMILY_COUPLES_WITH_DEPENDENT_CHILDREN': 'Couple - Dependent Children',
    'DEMO_FAMILY_SINGLE_PARENT_WITH_DEPENDENT_CHILDREN': 'Single Parent - Dependent Children',
    'DEMO_FAMILY_SINGLE_PERSON_HOUSEHOLD': 'Single Person Household'
}


def _to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _normalise_label(text: str) -> str:
    if not text:
        return ''
    return text.replace('-', ' ').replace('/', ' ').replace('&', ' ').upper().strip()


def _weighted_choice(options, fallback):
    cleaned = [
        {
            'label': (_normalise_label(opt.get('label')) if isinstance(opt, dict) else ''),
            'weight': _to_float(opt.get('weight')) if isinstance(opt, dict) else 0.0
        }
        for opt in (options or [])
    ]
    total = sum(max(item['weight'], 0.0) for item in cleaned)
    if total <= 0:
        return fallback
    r = random.random() * total
    cumulative = 0.0
    for item in cleaned:
        weight = max(item['weight'], 0.0)
        cumulative += weight
        if r <= cumulative:
            return item['label'] or fallback
    return cleaned[-1]['label'] or fallback


def _level_choice(base_value: str, ordered_levels):
    canonical = _normalise_label(base_value)
    upper_levels = [_normalise_label(level) for level in ordered_levels]
    try:
        base_index = upper_levels.index(canonical)
    except ValueError:
        base_index = len(upper_levels) // 2

    neighbour_total = 0.3
    other_total = 0.1

    weights = []
    neighbour_count = sum(1 for idx in range(len(upper_levels)) if abs(idx - base_index) == 1)
    other_count = sum(1 for idx in range(len(upper_levels)) if abs(idx - base_index) > 1)

    for idx, level in enumerate(upper_levels):
        if idx == base_index:
            weight = 0.6
        elif abs(idx - base_index) == 1 and neighbour_count > 0:
            weight = neighbour_total / neighbour_count
        elif other_count > 0:
            weight = other_total / other_count
        else:
            weight = 0.0
        weights.append({'label': ordered_levels[idx], 'weight': weight})

    return _normalise_label(_weighted_choice(weights, ordered_levels[base_index]))


def _choose_age(age_distribution):
    label = _weighted_choice(age_distribution, 'ESTABLISHED_CAREER_MIDDLE_AGE')
    label = label.replace('PRE RETIREMENT', 'PRE-RETIREMENT')
    label = label.replace('  ', ' ').replace(' ', '_')
    label = label.upper()
    age_range = AGE_RANGES.get(label, (30, 45))
    return label, random.randint(age_range[0], age_range[1])


def _choose_marital(marital_distribution):
    label = _weighted_choice(marital_distribution, 'DEMO_MARITAL_SINGLE_NEVER_MARRIED')
    return MARITAL_LABELS.get(label, 'Single - Never Married')


def _choose_family(family_distribution):
    label = _weighted_choice(family_distribution, 'DEMO_FAMILY_SINGLE_PERSON_HOUSEHOLD')
    return FAMILY_LABELS.get(label, 'Single Person Household')


def _add_noise(value, stddev=0.35):
    base = _to_float(value)
    noisy = random.gauss(base, stddev)
    return round(max(0.0, noisy), 2)


def generate_profile(lad_code, unique_id, age_distribution, marital_distribution, family_distribution,
                     base_income, base_education, digital_index, fast_fashion_index, grocery_index, financial_index):
    seed_source = f"{lad_code}|{unique_id}"
    seed_val = int(hashlib.sha256(seed_source.encode('utf-8')).hexdigest(), 16) % 10**8
    random.seed(seed_val)

    income_choice = _level_choice(base_income or 'Mid', INCOME_LEVELS)
    education_choice = _level_choice(base_education or 'Some College', EDUCATION_LEVELS)
    age_band, age_value = _choose_age(age_distribution)
    marital_status = _choose_marital(marital_distribution)
    family_status = _choose_family(family_distribution)

    return {
        'age_band': age_band,
        'age': age_value,
        'income_level': income_choice.title().replace('Mid', 'Mid').replace('Upper-mid', 'Upper-Mid'),
        'education_level': education_choice.title().replace('Post-Grad', 'Post-Grad').replace('Some College', 'Some College'),
        'marital_status': marital_status,
        'family_status': family_status,
        'behavioral_digital_media_index': _add_noise(digital_index, 0.4),
        'behavioral_fast_fashion_index': _add_noise(fast_fashion_index, 0.4),
        'behavioral_grocery_delivery_index': _add_noise(grocery_index, 0.35),
        'behavioral_financial_investment_index': _add_noise(financial_index, 0.45)
    }
$$;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES AS
WITH subscriber_base AS (
    SELECT
        sp.UNIQUE_ID,
        sp.ARCHETYPE,
        sp.PERSONA,
        sp.TIER,
        ROW_NUMBER() OVER (PARTITION BY sp.ARCHETYPE ORDER BY sp.UNIQUE_ID) AS archetype_index,
        COUNT(*) OVER (PARTITION BY sp.ARCHETYPE) AS archetype_total
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES sp
),
lad_ratios AS (
    SELECT
        ARCHETYPE,
        LAD_CODE,
        CASE
            WHEN total_distributed > 0 THEN distributed_subscribers / total_distributed
            ELSE 1.0 / NULLIF(lad_count, 0)
        END AS ratio,
        OVERALL_SCORE
    FROM (
        SELECT
            sdpd.*,
            SUM(distributed_subscribers) OVER (PARTITION BY ARCHETYPE) AS total_distributed,
            COUNT(*) OVER (PARTITION BY ARCHETYPE) AS lad_count
        FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES_DISTRIBUTIONS sdpd
    ) q
),
lad_bounds AS (
    SELECT
        ARCHETYPE,
        LAD_CODE,
        ratio,
        cumulative,
        COALESCE(LAG(cumulative, 1, 0.0) OVER (PARTITION BY ARCHETYPE ORDER BY cumulative, LAD_CODE), 0.0) AS lower_bound,
        LEAST(1.0, cumulative) AS upper_bound
    FROM (
        SELECT
            ARCHETYPE,
            LAD_CODE,
            ratio,
            SUM(ratio) OVER (PARTITION BY ARCHETYPE ORDER BY ratio DESC, LAD_CODE) AS cumulative
        FROM lad_ratios
        WHERE ratio > 0
    ) ranked
),
lad_assignment AS (
    SELECT
        sb.UNIQUE_ID,
        sb.ARCHETYPE,
        sb.PERSONA,
        sb.TIER,
        lb.LAD_CODE
    FROM (
        SELECT
            sb.*,
            CASE
                WHEN sb.archetype_total > 0 THEN (sb.archetype_index - 1) / sb.archetype_total::FLOAT
                ELSE 0.0
            END AS norm_position
        FROM subscriber_base sb
    ) sb
    JOIN lad_bounds lb
      ON sb.ARCHETYPE = lb.ARCHETYPE
     AND sb.norm_position >= lb.lower_bound
     AND sb.norm_position < lb.upper_bound
),
archetype_default_lad AS (
    SELECT
        ARCHETYPE,
        LAD_CODE AS DEFAULT_LAD_CODE
    FROM (
        SELECT
            ARCHETYPE,
            LAD_CODE,
            OVERALL_SCORE,
            ROW_NUMBER() OVER (PARTITION BY ARCHETYPE ORDER BY OVERALL_SCORE DESC, LAD_CODE) AS rn
        FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES_DISTRIBUTIONS
    ) ranked
    WHERE rn = 1
),
global_default_lad AS (
    SELECT LAD_CODE AS DEFAULT_LAD_CODE
    FROM (
        SELECT
            LAD_CODE,
            TOTAL_POPULATION,
            ROW_NUMBER() OVER (ORDER BY TOTAL_POPULATION DESC, LAD_CODE) AS rn
        FROM AME_AD_SALES_DEMO.GENERATE.DEMOGRAPHICS_DISTRIBUTIONS
    ) ranked
    WHERE rn = 1
),
lad_assignment_fallback AS (
    SELECT
        sb.UNIQUE_ID,
        sb.ARCHETYPE,
        sb.PERSONA,
        sb.TIER,
        COALESCE(la.LAD_CODE, adl.DEFAULT_LAD_CODE, gdl.DEFAULT_LAD_CODE) AS LAD_CODE
    FROM subscriber_base sb
    LEFT JOIN lad_assignment la
      ON sb.UNIQUE_ID = la.UNIQUE_ID
    LEFT JOIN archetype_default_lad adl
      ON sb.ARCHETYPE = adl.ARCHETYPE
    CROSS JOIN global_default_lad gdl
),
lad_area_selection AS (
    SELECT
        laf.UNIQUE_ID,
        laf.ARCHETYPE,
        laf.PERSONA,
        laf.TIER,
        laf.LAD_CODE,
        area.AREA_NAME,
        area.LATITUDE,
        area.LONGITUDE
    FROM lad_assignment_fallback laf
    JOIN (
        SELECT
            LAD_CODE,
            AREA_NAME,
            LATITUDE,
            LONGITUDE,
            ROW_NUMBER() OVER (PARTITION BY LAD_CODE ORDER BY AREA_NAME, LATITUDE, LONGITUDE) AS area_index,
            COUNT(*) OVER (PARTITION BY LAD_CODE) AS area_count
        FROM AME_AD_SALES_DEMO.GENERATE.LAD_SAMPLE_AREAS
    ) area
      ON laf.LAD_CODE = area.LAD_CODE
    WHERE area.area_index = MOD(ABS(HASH(laf.UNIQUE_ID, area.LAD_CODE)), area.area_count) + 1
)
SELECT
    t.email,
    t.primary_mobile,
    t.ip_address,
    t.full_name,
    t.LAD_CODE,
    t.AREA_NAME,
    (t.LATITUDE + ((MOD(ABS(HASH(t.UNIQUE_ID, t.LAD_CODE, 'LAT')), 10000)::FLOAT / 10000.0 - 0.5) * 0.01)) AS LATITUDE,
    (t.LONGITUDE + ((MOD(ABS(HASH(t.UNIQUE_ID, t.LAD_CODE, 'LON')), 10000)::FLOAT / 10000.0 - 0.5) * 0.01)) AS LONGITUDE,
    t.profile:age::INT AS AGE,
    t.profile:age_band::STRING AS AGE_BAND,
    t.profile:income_level::STRING AS INCOME_LEVEL,
    t.profile:education_level::STRING AS EDUCATION_LEVEL,
    t.profile:marital_status::STRING AS MARITAL_STATUS,
    t.profile:family_status::STRING AS FAMILY_STATUS,
    t.profile:behavioral_digital_media_index::FLOAT AS BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX,
    t.profile:behavioral_fast_fashion_index::FLOAT AS BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY,
    t.profile:behavioral_grocery_delivery_index::FLOAT AS BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE,
    t.profile:behavioral_financial_investment_index::FLOAT AS BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST
FROM (
    SELECT
        la_unique.UNIQUE_ID,
        sp.email,
        sp.primary_mobile,
        sp.ip_address,
        sp.full_name,
        la_unique.ARCHETYPE,
        la_unique.PERSONA,
        la_unique.TIER,
        la_unique.LAD_CODE,
        la_unique.AREA_NAME,
        la_unique.LATITUDE,
        la_unique.LONGITUDE,
        AME_AD_SALES_DEMO.GENERATE.GENERATE_LAD_DEMOGRAPHIC_PROFILE(
            la_unique.LAD_CODE,
            la_unique.UNIQUE_ID,
            [
                {'label': 'EARLY_CHILDHOOD', 'weight': COALESCE(d.EARLY_CHILDHOOD, 0)},
                {'label': 'CHILDHOOD_ELEMENTARY_SCHOOL', 'weight': COALESCE(d.CHILDHOOD_ELEMENTARY_SCHOOL, 0)},
                {'label': 'ADOLESCENCE_TEEN_YEARS', 'weight': COALESCE(d.ADOLESCENSE_TEEN_YEARS, 0)},
                {'label': 'YOUNG_ADULTS_EARLY', 'weight': COALESCE(d.YOUNG_ADULTS_EARLY, 0)},
                {'label': 'PRIME_SETTLING_FAMILY_FORMATION', 'weight': COALESCE(d.PRIME_SETTLING_FAMILY_FORMATION, 0)},
                {'label': 'ESTABLISHED_CAREER_MIDDLE_AGE', 'weight': COALESCE(d.ESTABLISHED_CAREER_MIDDLE_AGE, 0)},
                {'label': 'PRE-RETIREMENT_EMPTY_NESTERS', 'weight': COALESCE(d.PRE_RETIREMENT_EMPTY_NESTERS, 0)},
                {'label': 'RETIREMENT_SENIOR_YEARS', 'weight': COALESCE(d.RETIREMENT_SENIOR_YEARS, 0)}
            ],
            [
                {'label': 'DEMO_MARITAL_SINGLE_NEVER_MARRIED', 'weight': COALESCE(d.DEMO_MARITAL_SINGLE_NEVER_MARRIED, 0)},
                {'label': 'DEMO_MARITAL_MARRIED_OR_CIVIL_PARTNERSHIP', 'weight': COALESCE(d.DEMO_MARITAL_MARRIED_OR_CIVIL_PARTNERSHIP, 0)},
                {'label': 'DEMO_MARITAL_DIVORCED_OR_SEPARATED', 'weight': COALESCE(d.DEMO_MARITAL_DIVORCED_OR_SEPARATED, 0)},
                {'label': 'DEMO_MARITAL_WIDOWED', 'weight': COALESCE(d.DEMO_MARITAL_WIDOWED, 0)}
            ],
            [
                {'label': 'DEMO_FAMILY_COUPLES_NO_CHILDREN', 'weight': COALESCE(d.DEMO_FAMILY_COUPLES_NO_CHILDREN, 0)},
                {'label': 'DEMO_FAMILY_COUPLES_WITH_DEPENDENT_CHILDREN', 'weight': COALESCE(d.DEMO_FAMILY_COUPLES_WITH_DEPENDENT_CHILDREN, 0)},
                {'label': 'DEMO_FAMILY_SINGLE_PARENT_WITH_DEPENDENT_CHILDREN', 'weight': COALESCE(d.DEMO_FAMILY_SINGLE_PARENT_WITH_DEPENDENT_CHILDREN, 0)},
                {'label': 'DEMO_FAMILY_SINGLE_PERSON_HOUSEHOLD', 'weight': COALESCE(d.DEMO_FAMILY_SINGLE_PERSON_HOUSEHOLD, 0)}
            ],
            d.INCOME_LEVEL,
            d.EDUCATION_LEVEL,
            d.BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX,
            d.BEHAVIORAL_FAST_FASHION_RETAIL_PROPENSITY,
            d.BEHAVIORAL_GROCERY_ONLINE_DELIVERY_USE,
            d.BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST
        ) AS profile
    FROM lad_area_selection la_unique
    JOIN AME_AD_SALES_DEMO.GENERATE.DEMOGRAPHICS_DISTRIBUTIONS d
      ON la_unique.LAD_CODE = d.LAD_CODE
    JOIN AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES sp
      ON la_unique.UNIQUE_ID = sp.UNIQUE_ID
) t;


SELECT 
    'Rows in GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES

;



-- ---------------------------------------------------------------------------
-- 09-validate-generate-outputs.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA GENERATE;

-- Expected baselines based on generation scripts
SET TOTAL_SUBSCRIBERS = 100000;
SET AD_SUPPORTED_SHARE = 0.35;
SET STANDARD_SHARE = 0.45;
SET PREMIUM_SHARE = 0.20;
SET DISTRIBUTION_TOLERANCE = 0.01;
SET EXPECTED_AD_CAMPAIGNS = 160;
SET ADS_MAX_IMPRESSION_EVENTS = 1000;
SET ADS_MAX_CLICK_EVENTS = 200;
SET ADS_MIN_IMPRESSION_EVENTS = 20;
SET ADS_MIN_CLICK_EVENTS = 5;
SET ADS_EVENTS_IMPRESSION_SAMPLE_RATIO = 0.1;
SET ADS_EVENTS_CLICK_SAMPLE_RATIO = 0.15;
SET CLICKSTREAM_MIN_EVENTS_PER_SUBSCRIBER = 5;
SET CLICKSTREAM_MAX_EVENTS_PER_SUBSCRIBER = 500;

SET VALIDATION_RUN_TS = CURRENT_TIMESTAMP();

CREATE TABLE IF NOT EXISTS AME_AD_SALES_DEMO.GENERATE.GENERATION_VALIDATION_RESULTS (
    check_name STRING,
    status STRING,
    expected FLOAT,
    actual FLOAT,
    details STRING,
    generated_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

DELETE FROM AME_AD_SALES_DEMO.GENERATE.GENERATION_VALIDATION_RESULTS
WHERE DATE(generated_ts) = CURRENT_DATE();

INSERT INTO AME_AD_SALES_DEMO.GENERATE.GENERATION_VALIDATION_RESULTS (check_name, status, expected, actual, details, generated_ts)
SELECT check_name, status, expected, actual, details, $VALIDATION_RUN_TS::TIMESTAMP_NTZ
FROM (
    --------------------------------------------------------------
    -- Subscriber profile counts
    --------------------------------------------------------------
    SELECT
        'subscriber_profiles.count' AS check_name,
        IFF(actual.actual_count = $TOTAL_SUBSCRIBERS, 'PASS', 'FAIL') AS status,
        $TOTAL_SUBSCRIBERS::FLOAT AS expected,
        actual.actual_count::FLOAT AS actual,
        'Total generated subscriber profiles' AS details
    FROM (
        SELECT COUNT(*) AS actual_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
    ) actual

    UNION ALL

    SELECT
        'subscriber_full_profiles.count_match' AS check_name,
        IFF(actual.actual_count = reference.reference_count, 'PASS', 'FAIL') AS status,
        reference.reference_count::FLOAT AS expected,
        actual.actual_count::FLOAT AS actual,
        'GENERATE.SUBSCRIBER_FULL_PROFILES rows should match SUBSCRIBER_PROFILES' AS details
    FROM (
        SELECT COUNT(*) AS actual_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    ) actual
    CROSS JOIN (
        SELECT COUNT(*) AS reference_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
    ) reference

    UNION ALL

    --------------------------------------------------------------
    -- Tier distribution validation
    --------------------------------------------------------------
    SELECT
        CONCAT('subscriber_profiles.tier_share.', expected.tier) AS check_name,
        IFF(ABS(COALESCE(actual.share, 0) - expected.share) <= $DISTRIBUTION_TOLERANCE, 'PASS', 'WARN') AS status,
        expected.share AS expected,
        COALESCE(actual.share, 0) AS actual,
        CONCAT('Tolerance ±', $DISTRIBUTION_TOLERANCE, ' (expected share)') AS details
    FROM (
        SELECT
            tier,
            COUNT(*) / SUM(COUNT(*)) OVER () AS share
        FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
        GROUP BY tier
    ) actual
    RIGHT JOIN (
        SELECT 'Ad-supported' AS tier, $AD_SUPPORTED_SHARE::FLOAT AS share UNION ALL
        SELECT 'Standard', $STANDARD_SHARE::FLOAT UNION ALL
        SELECT 'Premium', $PREMIUM_SHARE::FLOAT
    ) expected
      ON expected.tier = actual.tier

    UNION ALL

    --------------------------------------------------------------
    -- Subscriber identity column null checks
    --------------------------------------------------------------
    SELECT
        'subscriber_full_profiles.null_check.full_name' AS check_name,
        IFF(checks.null_count = 0, 'PASS', 'FAIL') AS status,
        0.0 AS expected,
        checks.null_count::FLOAT AS actual,
        'Rows with NULL full_name' AS details
    FROM (
        SELECT COUNT_IF(full_name IS NULL) AS null_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    ) checks

    UNION ALL

    SELECT
        'subscriber_full_profiles.null_check.email',
        IFF(checks.null_count = 0, 'PASS', 'FAIL'),
        0.0,
        checks.null_count::FLOAT,
        'Rows with NULL email'
    FROM (
        SELECT COUNT_IF(email IS NULL) AS null_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    ) checks

    UNION ALL

    SELECT
        'subscriber_full_profiles.null_check.primary_mobile',
        IFF(checks.null_count = 0, 'PASS', 'FAIL'),
        0.0,
        checks.null_count::FLOAT,
        'Rows with NULL primary_mobile'
    FROM (
        SELECT COUNT_IF(primary_mobile IS NULL) AS null_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    ) checks

    UNION ALL

    SELECT
        'subscriber_full_profiles.null_check.ip_address',
        IFF(checks.null_count = 0, 'PASS', 'FAIL'),
        0.0,
        checks.null_count::FLOAT,
        'Rows with NULL ip_address'
    FROM (
        SELECT COUNT_IF(ip_address IS NULL) AS null_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
    ) checks

    UNION ALL

    SELECT
        'subscriber_full_profiles.duplicate_email',
        IFF(checks.dup_count = 0, 'PASS', 'FAIL'),
        0.0,
        checks.dup_count::FLOAT,
        'Emails appearing more than once after dedupe' AS details
    FROM (
        SELECT COUNT(*) AS dup_count
        FROM (
            SELECT email FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
            GROUP BY email
            HAVING COUNT(*) > 1
        ) dup_emails
    ) checks

    UNION ALL

    --------------------------------------------------------------
    -- Ad campaign generation checks
    --------------------------------------------------------------
    SELECT
        'ad_campaigns.count',
        IFF(counts.actual_count = $EXPECTED_AD_CAMPAIGNS, 'PASS', 'FAIL'),
        $EXPECTED_AD_CAMPAIGNS::FLOAT,
        counts.actual_count::FLOAT,
        'Total campaigns generated'
    FROM (
        SELECT COUNT(*) AS actual_count FROM AME_AD_SALES_DEMO.GENERATE.AD_CAMPAIGNS
    ) counts

    UNION ALL

    SELECT
        'ad_performance_events.non_empty',
        IFF(counts.actual_count > 0, 'PASS', 'FAIL'),
        NULL,
        counts.actual_count::FLOAT,
        'Rows in AD_PERFORMANCE_EVENTS'
    FROM (
        SELECT COUNT(*) AS actual_count FROM AME_AD_SALES_DEMO.GENERATE.AD_PERFORMANCE_EVENTS
    ) counts

    UNION ALL

    SELECT
        'ads_events.non_empty',
        IFF(counts.actual_count > 0, 'PASS', 'FAIL'),
        NULL,
        counts.actual_count::FLOAT,
        'Rows in ADS_EVENTS'
    FROM (
        SELECT COUNT(*) AS actual_count FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS
    ) counts

    UNION ALL

    SELECT
        'ads_events.count_expected',
        IFF(ABS(actual.actual_count - expected.expected_count) <= expected.expected_count * $DISTRIBUTION_TOLERANCE, 'PASS', 'WARN'),
        expected.expected_count::FLOAT,
        actual.actual_count::FLOAT,
        CONCAT('Expected events derived from performance sampling (±', $DISTRIBUTION_TOLERANCE, ')') AS details
    FROM (
        SELECT COUNT(*) AS actual_count FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS
    ) actual
    CROSS JOIN (
        SELECT
            SUM(
                LEAST(
                    impressions,
                    $ADS_MAX_IMPRESSION_EVENTS,
                    GREATEST($ADS_MIN_IMPRESSION_EVENTS, ROUND(impressions * $ADS_EVENTS_IMPRESSION_SAMPLE_RATIO))
                )
            )
            +
            SUM(
                LEAST(
                    clicks,
                    $ADS_MAX_CLICK_EVENTS,
                    GREATEST($ADS_MIN_CLICK_EVENTS, ROUND(clicks * $ADS_EVENTS_CLICK_SAMPLE_RATIO))
                )
            ) AS expected_count
        FROM AME_AD_SALES_DEMO.GENERATE.AD_PERFORMANCE_EVENTS
    ) expected

    UNION ALL

    SELECT
        'ads_events.unique_id_membership',
        IFF(counts.missing = 0, 'PASS', 'FAIL'),
        0.0,
        counts.missing::FLOAT,
        'Unique IDs in ADS_EVENTS not present in SUBSCRIBER_PROFILES'
    FROM (
        SELECT COUNT(*) AS missing
        FROM (
            SELECT DISTINCT unique_id FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS
            MINUS
            SELECT DISTINCT unique_id FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
        ) missing_ids
    ) counts

    UNION ALL

    --------------------------------------------------------------
    -- Clickstream generation checks
    --------------------------------------------------------------
    SELECT
        'clickstream_events.non_empty',
        IFF(stats.total_events > 0, 'PASS', 'FAIL'),
        NULL,
        stats.total_events::FLOAT,
        'Rows in CLICKSTREAM_EVENTS'
    FROM (
        SELECT COUNT(*) AS total_events FROM AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS
    ) stats

    UNION ALL

    SELECT
        'clickstream_events.avg_per_subscriber',
        IFF(stats.avg_events BETWEEN $CLICKSTREAM_MIN_EVENTS_PER_SUBSCRIBER AND $CLICKSTREAM_MAX_EVENTS_PER_SUBSCRIBER, 'PASS', 'WARN'),
        $CLICKSTREAM_MIN_EVENTS_PER_SUBSCRIBER::FLOAT,
        stats.avg_events,
        CONCAT('Average events per subscriber (expected between ', $CLICKSTREAM_MIN_EVENTS_PER_SUBSCRIBER, ' and ', $CLICKSTREAM_MAX_EVENTS_PER_SUBSCRIBER, ')')
    FROM (
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT unique_id) AS unique_subs,
            CASE WHEN COUNT(DISTINCT unique_id) > 0 THEN COUNT(*) / COUNT(DISTINCT unique_id) ELSE 0 END AS avg_events
        FROM AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS
    ) stats

    UNION ALL

    SELECT
        'clickstream_events.unique_id_membership',
        IFF(stats.missing = 0, 'PASS', 'FAIL'),
        0.0,
        stats.missing::FLOAT,
        'Unique IDs in CLICKSTREAM_EVENTS not present in SUBSCRIBER_PROFILES'
    FROM (
        SELECT COUNT(*) AS missing
        FROM (
            SELECT DISTINCT unique_id FROM AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS
            MINUS
            SELECT DISTINCT unique_id FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
        ) missing_ids
    ) stats

    UNION ALL

    --------------------------------------------------------------
    -- Demographic enrichment checks
    --------------------------------------------------------------
    SELECT
        'subscriber_demographics_profiles.non_empty',
        IFF(stats.total_rows > 0, 'PASS', 'FAIL'),
        NULL,
        stats.total_rows::FLOAT,
        'Rows in SUBSCRIBER_DEMOGRAPHICS_PROFILES'
    FROM (
        SELECT COUNT(*) AS total_rows FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES
    ) stats

    UNION ALL

    SELECT
        'subscriber_demographics_profiles.email_non_null',
        IFF(stats.null_count = 0, 'PASS', 'FAIL'),
        0.0,
        stats.null_count::FLOAT,
        'Rows with NULL email in SUBSCRIBER_DEMOGRAPHICS_PROFILES'
    FROM (
        SELECT COUNT_IF(email IS NULL) AS null_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES
    ) stats

    UNION ALL

    SELECT
        'subscriber_demographics_profiles.email_membership',
        IFF(stats.missing = 0, 'PASS', 'FAIL'),
        0.0,
        stats.missing::FLOAT,
        'Emails in demographics not present in SUBSCRIBER_FULL_PROFILES'
    FROM (
        SELECT COUNT(*) AS missing
        FROM (
            SELECT DISTINCT email FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES
            MINUS
            SELECT DISTINCT email FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
        ) missing_emails
    ) stats

    UNION ALL

    --------------------------------------------------------------
    -- Stage profile relationship checks
    --------------------------------------------------------------
    SELECT
        'subscriber_profiles.unique_id_non_null',
        IFF(counts.null_count = 0, 'PASS', 'FAIL'),
        0.0,
        counts.null_count::FLOAT,
        'NULL unique_id values in SUBSCRIBER_PROFILES'
    FROM (
        SELECT COUNT_IF(unique_id IS NULL) AS null_count FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
    ) counts

    UNION ALL

    SELECT
        'subscriber_full_profiles.unique_id_match',
        IFF(counts.missing = 0, 'PASS', 'FAIL'),
        0.0,
        counts.missing::FLOAT,
        'Unique IDs missing from SUBSCRIBER_FULL_PROFILES'
    FROM (
        SELECT COUNT(*) AS missing
        FROM (
            SELECT DISTINCT unique_id FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_PROFILES
            MINUS
            SELECT DISTINCT unique_id FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES
        ) missing_ids
    ) counts
);

SELECT *
FROM AME_AD_SALES_DEMO.GENERATE.GENERATION_VALIDATION_RESULTS
WHERE generated_ts = $VALIDATION_RUN_TS::TIMESTAMP_NTZ
ORDER BY check_name;



-- ---------------------------------------------------------------------------
-- 10-load-subscriber-full-profiles.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;
USE SCHEMA INGEST;

/*
    Load subscriber identity data from the GENERATE layer into the INGEST schema.
    This copies the deterministic identity outputs produced by
    sql/generate/06. generate subscribers.sql for downstream enrichment steps.
*/
CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES AS
SELECT * EXCLUDE(PROFILE_SEQ_ID, ARCHETYPE, PERSONA)
FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_FULL_PROFILES;

SELECT 
    'Rows in INGEST.SUBSCRIBER_PROFILES' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES;

-- ---------------------------------------------------------------------------
-- 11-mount-demographics-profiles-share.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.DATA_SHARING.DEMOGRAPHICS_PROFILES AS
SELECT *
FROM AME_AD_SALES_DEMO.GENERATE.SUBSCRIBER_DEMOGRAPHICS_PROFILES;


SELECT 
    'Rows in DATA_SHARING.DEMOGRAPHICS_PROFILES' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.DATA_SHARING.DEMOGRAPHICS_PROFILES;

-- ---------------------------------------------------------------------------
-- 12-load-events.sql
-- ---------------------------------------------------------------------------
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS AS
SELECT
  g.* EXCLUDE (JOURNEY_NAME, JOURNEY_CODE, PERSONA, ARCHETYPE)
FROM AME_AD_SALES_DEMO.GENERATE.CLICKSTREAM_EVENTS g
LEFT JOIN AME_AD_SALES_DEMO.INGEST.SUBSCRIBER_PROFILES sp
  ON sp.UNIQUE_ID = g.UNIQUE_ID
WHERE sp.SIGNUP_DATE IS NULL OR g.EVENT_TS >= sp.SIGNUP_DATE;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.ADS_EVENTS AS
SELECT * EXCLUDE (PERSONA)
FROM AME_AD_SALES_DEMO.GENERATE.ADS_EVENTS;

SELECT 
    'Rows in INGEST.CLICKSTREAM_EVENTS' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.INGEST.CLICKSTREAM_EVENTS
UNION ALL 
SELECT 
    'Rows in INGEST.ADS_EVENTS' as "Table created", 
    count(*) as "Rows generated" 
    FROM AME_AD_SALES_DEMO.INGEST.ADS_EVENTS
;

-- Derive a SIGN_UP event per subscriber at the earliest observed interaction (ad or clickstream)
-- This ensures every subscriber has an explicit account creation/join marker.
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
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE AME_AD_SALES_DEMO;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.AD_CAMPAIGNS AS
SELECT * EXCLUDE (TARGET_PERSONAS, TARGET_DEVICES)
FROM AME_AD_SALES_DEMO.GENERATE.AD_CAMPAIGNS;

CREATE OR REPLACE TABLE AME_AD_SALES_DEMO.INGEST.AD_PERFORMANCE_EVENTS AS
SELECT * EXCLUDE (TARGET_PERSONAS, TARGET_DEVICES)
FROM AME_AD_SALES_DEMO.GENERATE.AD_PERFORMANCE_EVENTS;

SELECT
    'Rows in INGEST.AD_CAMPAIGNS' AS "Table created",
    COUNT(*) AS "Rows generated"
FROM AME_AD_SALES_DEMO.INGEST.AD_CAMPAIGNS
UNION ALL
SELECT
    'Rows in INGEST.AD_PERFORMANCE_EVENTS' AS "Table created",
    COUNT(*) AS "Rows generated"
FROM AME_AD_SALES_DEMO.INGEST.AD_PERFORMANCE_EVENTS;



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
    FROM AME_AD_SALES_DEMO.GENERATE.DEMOGRAPHICS_DISTRIBUTIONS
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
USE ROLE AME_AD_SALES_DEMO_ADMIN;
USE WAREHOUSE APP_WH;

-- APPS schema already created and owned by AME_AD_SALES_DEMO_ADMIN (see line ~70)

-- Create API integration for GitHub (public repo - no secrets needed)
CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs/')
    ENABLED = TRUE;

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
    QUERY_WAREHOUSE = 'APP_WH';

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
    QUERY_WAREHOUSE = 'APP_WH';

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
    QUERY_WAREHOUSE = 'APP_WH';

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
    QUERY_WAREHOUSE = 'APP_WH';

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

-- Copy semantic view YAML from Git repository to stage
COPY FILES
    INTO @AME_AD_SALES_DEMO.ANALYSE.SEMANTIC_MODELS/
    FROM @AME_AD_SALES_DEMO.GENERATE.SFGUIDE_MEA_REPO/branches/main/scripts/semantic_view.yaml;

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
        'claude-3-5-sonnet',
        CONCAT(
            'You are a personalized recommendation assistant for a media streaming platform. ',
            'Based on the subscriber data below, provide actionable recommendations. ',
            'Format your response as a clear, bulleted list with specific actions. ',
            'Be concise but insightful.\n\n',
            'SUBSCRIBER DATA:\n',
            (SELECT CONCAT(
                'Profile ID: ', f.PROFILE_ID, '\n',
                'Tier: ', f.TIER, '\n',
                'Persona: ', f.PERSONA, '\n',
                'Age: ', COALESCE(f.AGE::VARCHAR, 'Unknown'), '\n',
                'Income Level: ', COALESCE(f.INCOME_LEVEL, 'Unknown'), '\n',
                'Monthly Site Visits: ', ROUND(f.AVG_SITE_VISITS_PER_MONTH, 1), '\n',
                'Login Frequency/Week: ', ROUND(f.LOGIN_FREQUENCY_PER_WEEK, 1), '\n',
                'Watch Time (30d): ', ROUND(f.WATCH_TIME_30/3600, 1), ' hours\n',
                'Content Categories: ', COALESCE(f.CONTENT_CATEGORY_VECTOR, 'None'), '\n',
                'Churn Risk: ', COALESCE(cr.CHURN_RISK_SEGMENT, 'Unknown'), '\n',
                'Churn Probability: ', ROUND(COALESCE(cr.PREDICTED_CHURN_PROB, 0) * 100, 1), '%\n',
                'Predicted LTV: $', ROUND(COALESCE(ltv.PREDICTED_LTV, 0), 2), '\n',
                'LTV Segment: ', COALESCE(ltv.LTV_SEGMENT, 'Unknown')
            )
            FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_FEATURES f
            LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CHURN_RISK cr ON f.UNIQUE_ID = cr.UNIQUE_ID
            LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_LTV_SCORES ltv ON f.UNIQUE_ID = ltv.UNIQUE_ID
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
    "orchestration": "Data covers subscriber features, churn predictions, LTV scores, and ad performance. Use PROFILE_ID for subscriber lookups (format: SUB-XXXXXXXXX). Join FE_SUBSCRIBER_FEATURES with FE_SUBSCRIBER_CHURN_RISK and FE_SUBSCRIBER_LTV_SCORES using UNIQUE_ID. Default to last 30 days for time-based queries.",
    "sample_questions": [
      {"question":"How many subscribers are at high churn risk?"},
      {"question":"What is the average LTV by subscriber tier?"},
      {"question":"Show top 10 campaigns by CTR"},
      {"question":"Which content categories have highest engagement?"},
      {"question":"Compare impressions across verticals"},
      {"question":"What is the churn risk distribution by persona?"}
    ]
  },
  "tools": [{
    "tool_spec": {
      "type": "cortex_analyst_text_to_sql",
      "name": "Query Subscriber Data",
      "description": "Query subscriber demographics, engagement metrics, churn risk predictions, lifetime value scores, ad performance data, and content viewing patterns"
    }
  }],
  "tool_resources": {
    "Query Subscriber Data": {
      "semantic_view": "AME_AD_SALES_DEMO.ANALYSE.AME_AD_SALES_SEMANTIC_VIEW",
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

-- 1. Drop the database (cascades to all schemas, tables, views, stages, 
--    Streamlit apps, functions, procedures, git repos, secrets inside)
DROP DATABASE IF EXISTS AME_AD_SALES_DEMO CASCADE;

-- 2. Drop the API integration (must be done separately, not in database)
DROP INTEGRATION IF EXISTS GITHUB_API_INTEGRATION;

-- 3. Drop the warehouse
DROP WAREHOUSE IF EXISTS APP_WH;

-- 4. Drop the custom role (do this last)
DROP ROLE IF EXISTS AME_AD_SALES_DEMO_ADMIN;

-- Verify cleanup
SHOW DATABASES LIKE 'AME_AD_SALES_DEMO';
SHOW WAREHOUSES LIKE 'APP_WH';
SHOW ROLES LIKE 'AME_AD_SALES_DEMO_ADMIN';
SHOW INTEGRATIONS LIKE 'GITHUB_API_INTEGRATION';

-- Expected: All queries should return 0 rows

*/
