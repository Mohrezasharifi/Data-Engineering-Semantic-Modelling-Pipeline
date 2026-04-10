-- =============================================================
-- PROJECT 2: Data Engineering + Semantic Modelling
-- Oracle SQL — Source Transformation Queries
--
-- These queries represent the transformation logic that ran
-- in the legacy Oracle system, producing reporting-ready data.
-- Translated to Spark SQL in the target system.
-- =============================================================


-- -----------------------------------------------------------
-- T1: Build FACT_CLAIMS — the central fact table
-- Joins claims to dimension keys, calculates derived metrics
-- Oracle-specific: TO_CHAR date masking, NVL, DECODE
-- -----------------------------------------------------------
CREATE TABLE fact_claims AS
SELECT
    c.claim_id,
    c.nhs_trust_id,
    c.provider_id,
    c.procedure_code,
    c.diagnosis_code,
    -- Date dimension keys (Oracle: TO_CHAR for surrogate key)
    TO_NUMBER(TO_CHAR(c.service_date, 'YYYYMMDD'))   AS service_date_key,
    TO_NUMBER(TO_CHAR(c.submission_date, 'YYYYMMDD')) AS submission_date_key,
    c.financial_year,
    -- Measures
    c.claimed_amount,
    NVL(c.approved_amount, 0)                         AS approved_amount,
    c.claimed_amount - NVL(c.approved_amount, 0)      AS variance_amount,
    -- Derived flags
    CASE
        WHEN c.claim_status = 'APPROVED'   THEN 1 ELSE 0
    END                                               AS is_approved_flag,
    CASE
        WHEN c.claim_status = 'REJECTED'   THEN 1 ELSE 0
    END                                               AS is_rejected_flag,
    DECODE(c.claim_status,
        'APPROVED',    'Closed',
        'REJECTED',    'Closed',
        'Open'
    )                                                 AS claim_lifecycle_stage,
    -- Age banding for demographic dimension
    CASE
        WHEN c.patient_age BETWEEN 0  AND 17 THEN '0-17'
        WHEN c.patient_age BETWEEN 18 AND 34 THEN '18-34'
        WHEN c.patient_age BETWEEN 35 AND 49 THEN '35-49'
        WHEN c.patient_age BETWEEN 50 AND 64 THEN '50-64'
        WHEN c.patient_age >= 65             THEN '65+'
        ELSE 'Unknown'
    END                                               AS patient_age_band,
    c.patient_gender,
    c.patient_ethnicity_code,
    -- Submission lag (business days approximation)
    TRUNC(c.submission_date) - TRUNC(c.service_date) AS submission_lag_days
FROM
    nhs_claims c
WHERE
    c.service_date IS NOT NULL;


-- -----------------------------------------------------------
-- T2: Build DIM_DATE — calendar dimension
-- Oracle-specific: CONNECT BY LEVEL, ADD_MONTHS
-- -----------------------------------------------------------
CREATE TABLE dim_date AS
WITH date_series AS (
    SELECT
        TRUNC(DATE '2022-01-01') + LEVEL - 1 AS calendar_date
    FROM
        DUAL
    CONNECT BY LEVEL <= 1095  -- ~3 years
)
SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD'))  AS date_key,
    calendar_date,
    TO_CHAR(calendar_date, 'YYYY')                 AS calendar_year,
    TO_CHAR(calendar_date, 'MM')                   AS calendar_month_num,
    TO_CHAR(calendar_date, 'Month')                AS calendar_month_name,
    TO_CHAR(calendar_date, 'Q')                    AS calendar_quarter,
    TO_CHAR(calendar_date, 'IW')                   AS iso_week,
    -- NHS Financial Year: starts April 1
    CASE
        WHEN TO_NUMBER(TO_CHAR(calendar_date, 'MM')) >= 4
        THEN 'FY' || TO_CHAR(calendar_date, 'YYYY') || '-' ||
             TO_CHAR(ADD_MONTHS(calendar_date, 12), 'YY')
        ELSE 'FY' || TO_CHAR(ADD_MONTHS(calendar_date, -12), 'YYYY') || '-' ||
             TO_CHAR(calendar_date, 'YY')
    END                                            AS nhs_financial_year,
    CASE
        WHEN TO_NUMBER(TO_CHAR(calendar_date, 'MM')) IN (4, 5, 6)  THEN 'Q1'
        WHEN TO_NUMBER(TO_CHAR(calendar_date, 'MM')) IN (7, 8, 9)  THEN 'Q2'
        WHEN TO_NUMBER(TO_CHAR(calendar_date, 'MM')) IN (10,11,12) THEN 'Q3'
        ELSE 'Q4'
    END                                            AS nhs_financial_quarter,
    CASE TO_CHAR(calendar_date, 'DY')
        WHEN 'SAT' THEN 0
        WHEN 'SUN' THEN 0
        ELSE 1
    END                                            AS is_weekday
FROM
    date_series;


-- -----------------------------------------------------------
-- T3: KPI — Approval Rate by Trust and Financial Year
-- Oracle-specific: ROUND, NULLIF, NVL
-- -----------------------------------------------------------
SELECT
    t.nhs_trust_name,
    f.financial_year,
    COUNT(*)                                               AS total_claims,
    SUM(f.is_approved_flag)                                AS approved_claims,
    ROUND(
        SUM(f.is_approved_flag) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                      AS approval_rate_pct,
    SUM(f.claimed_amount)                                  AS total_claimed_gbp,
    SUM(f.approved_amount)                                 AS total_approved_gbp,
    ROUND(
        SUM(f.approved_amount) /
        NULLIF(SUM(f.claimed_amount), 0) * 100, 2
    )                                                      AS financial_approval_rate_pct
FROM
    fact_claims f
    JOIN nhs_trusts t ON f.nhs_trust_id = t.nhs_trust_id
GROUP BY
    t.nhs_trust_name,
    f.financial_year
ORDER BY
    f.financial_year,
    approval_rate_pct DESC;


-- -----------------------------------------------------------
-- T4: Provider Activity Summary
-- Oracle-specific: LISTAGG, RANK()
-- -----------------------------------------------------------
SELECT
    p.provider_id,
    p.provider_name,
    p.provider_type,
    t.nhs_trust_name,
    COUNT(f.claim_id)                                 AS total_claims,
    SUM(f.approved_amount)                            AS total_approved_gbp,
    ROUND(AVG(f.submission_lag_days), 1)              AS avg_submission_lag,
    RANK() OVER (ORDER BY SUM(f.approved_amount) DESC) AS provider_rank,
    LISTAGG(DISTINCT f.procedure_code, ' | ')
        WITHIN GROUP (ORDER BY f.procedure_code)     AS procedures_delivered
FROM
    fact_claims f
    JOIN providers p    ON f.provider_id    = p.provider_id
    JOIN nhs_trusts t   ON f.nhs_trust_id   = t.nhs_trust_id
GROUP BY
    p.provider_id,
    p.provider_name,
    p.provider_type,
    t.nhs_trust_name;
