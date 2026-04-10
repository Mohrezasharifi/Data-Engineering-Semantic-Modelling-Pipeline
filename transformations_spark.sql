-- =============================================================
-- PROJECT 2: Data Engineering + Semantic Modelling
-- Spark SQL — Target Transformation Queries (Microsoft Fabric)
--
-- Direct translations from Oracle, fully annotated with
-- migration notes. Designed for Microsoft Fabric Lakehouse
-- using Delta Lake format.
-- =============================================================


-- -----------------------------------------------------------
-- T1: Build FACT_CLAIMS — Spark SQL version
-- [MIGRATION NOTE] NVL → COALESCE
-- [MIGRATION NOTE] DECODE → CASE WHEN
-- [MIGRATION NOTE] TO_NUMBER(TO_CHAR(...)) → CAST(DATE_FORMAT(...) AS INT)
-- [MIGRATION NOTE] TRUNC(date1) - TRUNC(date2) → DATEDIFF(date1, date2)
-- -----------------------------------------------------------
CREATE OR REPLACE TABLE silver.fact_claims
USING DELTA AS
SELECT
    c.claim_id,
    c.nhs_trust_id,
    c.provider_id,
    c.procedure_code,
    c.diagnosis_code,
    -- Date dimension keys
    CAST(DATE_FORMAT(c.service_date, 'yyyyMMdd') AS INT)    AS service_date_key,
    CAST(DATE_FORMAT(c.submission_date, 'yyyyMMdd') AS INT) AS submission_date_key,
    c.financial_year,
    -- Measures
    c.claimed_amount,
    COALESCE(c.approved_amount, 0)                          AS approved_amount,
    c.claimed_amount - COALESCE(c.approved_amount, 0)       AS variance_amount,
    -- Derived flags
    CASE WHEN c.claim_status = 'APPROVED' THEN 1 ELSE 0 END AS is_approved_flag,
    CASE WHEN c.claim_status = 'REJECTED' THEN 1 ELSE 0 END AS is_rejected_flag,
    -- DECODE replaced with CASE WHEN
    CASE c.claim_status
        WHEN 'APPROVED' THEN 'Closed'
        WHEN 'REJECTED' THEN 'Closed'
        ELSE 'Open'
    END                                                     AS claim_lifecycle_stage,
    -- Age banding
    CASE
        WHEN c.patient_age BETWEEN 0  AND 17 THEN '0-17'
        WHEN c.patient_age BETWEEN 18 AND 34 THEN '18-34'
        WHEN c.patient_age BETWEEN 35 AND 49 THEN '35-49'
        WHEN c.patient_age BETWEEN 50 AND 64 THEN '50-64'
        WHEN c.patient_age >= 65             THEN '65+'
        ELSE 'Unknown'
    END                                                     AS patient_age_band,
    c.patient_gender,
    c.patient_ethnicity_code,
    -- Submission lag: TRUNC subtraction → DATEDIFF
    DATEDIFF(c.submission_date, c.service_date)             AS submission_lag_days
FROM
    bronze.nhs_claims c
WHERE
    c.service_date IS NOT NULL;


-- -----------------------------------------------------------
-- T2: Build DIM_DATE — Spark SQL version
-- [MIGRATION NOTE] CONNECT BY LEVEL → SEQUENCE() + EXPLODE()
--                  Oracle's hierarchical row generation does not exist in Spark.
--                  Use SEQUENCE to generate a date range, then EXPLODE.
-- [MIGRATION NOTE] ADD_MONTHS → ADD_MONTHS (works in Spark too)
-- [MIGRATION NOTE] TO_CHAR(date, 'IW') → WEEKOFYEAR(date)
-- [MIGRATION NOTE] TO_CHAR(date, 'DY') → DAYOFWEEK(date) (returns 1=Sun..7=Sat)
-- -----------------------------------------------------------
CREATE OR REPLACE TABLE silver.dim_date
USING DELTA AS
WITH date_series AS (
    -- SEQUENCE generates an array; EXPLODE turns it into rows
    SELECT EXPLODE(
        SEQUENCE(DATE '2022-01-01', DATE '2024-12-31', INTERVAL 1 DAY)
    ) AS calendar_date
)
SELECT
    CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT)   AS date_key,
    calendar_date,
    YEAR(calendar_date)                                    AS calendar_year,
    LPAD(MONTH(calendar_date), 2, '0')                    AS calendar_month_num,
    DATE_FORMAT(calendar_date, 'MMMM')                    AS calendar_month_name,
    QUARTER(calendar_date)                                 AS calendar_quarter,
    WEEKOFYEAR(calendar_date)                              AS iso_week,
    -- NHS Financial Year (April 1 start)
    CASE
        WHEN MONTH(calendar_date) >= 4
        THEN CONCAT('FY', YEAR(calendar_date), '-', RIGHT(YEAR(calendar_date)+1, 2))
        ELSE CONCAT('FY', YEAR(calendar_date)-1, '-', RIGHT(YEAR(calendar_date), 2))
    END                                                    AS nhs_financial_year,
    CASE
        WHEN MONTH(calendar_date) IN (4, 5, 6)   THEN 'Q1'
        WHEN MONTH(calendar_date) IN (7, 8, 9)   THEN 'Q2'
        WHEN MONTH(calendar_date) IN (10, 11, 12) THEN 'Q3'
        ELSE 'Q4'
    END                                                    AS nhs_financial_quarter,
    -- DAYOFWEEK: 1=Sunday, 7=Saturday in Spark
    CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN 0 ELSE 1 END AS is_weekday
FROM
    date_series;


-- -----------------------------------------------------------
-- T3: KPI — Approval Rate by Trust and Financial Year
-- [MIGRATION NOTE] Syntax largely identical; NVL → COALESCE
-- -----------------------------------------------------------
SELECT
    t.nhs_trust_name,
    f.financial_year,
    COUNT(*)                                                AS total_claims,
    SUM(f.is_approved_flag)                                 AS approved_claims,
    ROUND(
        SUM(f.is_approved_flag) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS approval_rate_pct,
    SUM(f.claimed_amount)                                   AS total_claimed_gbp,
    SUM(f.approved_amount)                                  AS total_approved_gbp,
    ROUND(
        SUM(f.approved_amount) /
        NULLIF(SUM(f.claimed_amount), 0) * 100, 2
    )                                                       AS financial_approval_rate_pct
FROM
    silver.fact_claims f
    JOIN silver.dim_trust t ON f.nhs_trust_id = t.nhs_trust_id
GROUP BY
    t.nhs_trust_name,
    f.financial_year
ORDER BY
    f.financial_year,
    approval_rate_pct DESC;


-- -----------------------------------------------------------
-- T4: Provider Activity Summary
-- [MIGRATION NOTE] LISTAGG → ARRAY_JOIN(SORT_ARRAY(COLLECT_SET()))
-- [MIGRATION NOTE] RANK() OVER → identical syntax
-- -----------------------------------------------------------
SELECT
    p.provider_id,
    p.provider_name,
    p.provider_type,
    t.nhs_trust_name,
    COUNT(f.claim_id)                                   AS total_claims,
    SUM(f.approved_amount)                              AS total_approved_gbp,
    ROUND(AVG(f.submission_lag_days), 1)                AS avg_submission_lag,
    RANK() OVER (ORDER BY SUM(f.approved_amount) DESC)  AS provider_rank,
    -- LISTAGG(DISTINCT ...) WITHIN GROUP (ORDER BY ...) replaced:
    ARRAY_JOIN(
        SORT_ARRAY(COLLECT_SET(f.procedure_code)), ' | '
    )                                                   AS procedures_delivered
FROM
    silver.fact_claims f
    JOIN silver.dim_provider p ON f.provider_id   = p.provider_id
    JOIN silver.dim_trust    t ON f.nhs_trust_id  = t.nhs_trust_id
GROUP BY
    p.provider_id,
    p.provider_name,
    p.provider_type,
    t.nhs_trust_name;


-- -----------------------------------------------------------
-- T5: Monthly Trend for Power BI Time Intelligence
-- Suitable for a Line Chart visual in Power BI
-- -----------------------------------------------------------
SELECT
    d.nhs_financial_year,
    d.nhs_financial_quarter,
    DATE_FORMAT(f.service_date, 'yyyy-MM')   AS service_month,
    t.nhs_trust_name,
    COUNT(*)                                 AS claim_volume,
    SUM(f.claimed_amount)                    AS total_claimed,
    SUM(f.approved_amount)                   AS total_approved,
    ROUND(AVG(f.submission_lag_days), 1)     AS avg_submission_lag,
    SUM(SUM(f.claimed_amount)) OVER (
        PARTITION BY t.nhs_trust_name, d.nhs_financial_year
        ORDER BY DATE_FORMAT(f.service_date, 'yyyy-MM')
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                        AS ytd_claimed_cumulative
FROM
    silver.fact_claims f
    JOIN silver.dim_date  d ON f.service_date_key = d.date_key
    JOIN silver.dim_trust t ON f.nhs_trust_id     = t.nhs_trust_id
GROUP BY
    d.nhs_financial_year,
    d.nhs_financial_quarter,
    DATE_FORMAT(f.service_date, 'yyyy-MM'),
    t.nhs_trust_name
ORDER BY
    service_month;
