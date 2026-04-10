# Semantic Model Design
## NHS Claims Analytics — Microsoft Fabric / Power BI

> **Portfolio Notice:** This is a mocked portfolio project. All entities, KPIs, and definitions 
> are illustrative examples based on generic healthcare data engineering patterns.

---

## Overview

This document defines the semantic model that sits between the **Gold layer** of the 
Fabric Lakehouse and the **Power BI reporting layer**. It describes tables, relationships, 
measures, and KPIs in a format suitable for implementation as a Power BI Dataset or 
Fabric Direct Lake model.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│            MICROSOFT FABRIC LAKEHOUSE (OneLake)             │
│                                                             │
│  Bronze          Silver                Gold                 │
│  ─────────       ────────────────      ──────────────────   │
│  Raw CSV   →     fact_claims      →    trust_kpis           │
│  Parquet         dim_date              monthly_trends       │
│                  dim_trust             procedure_summary    │
│                  dim_provider                               │
│                  dim_procedure                              │
└────────────────────────┬────────────────────────────────────┘
                         │ Direct Lake / SQL Endpoint
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           POWER BI SEMANTIC MODEL (Dataset)                 │
│                                                             │
│  Tables + Relationships + Calculated Measures               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           POWER BI REPORTS                                  │
│                                                             │
│  • Executive Dashboard    • Trust Performance Report        │
│  • Operational Claims View • Provider Activity Report       │
└─────────────────────────────────────────────────────────────┘
```

---

## Tables & Relationships

### Fact Table

#### `fact_claims`
The central grain of the model. One row per claim submission.

| Column | Type | Description |
|--------|------|-------------|
| `claim_id` | String | Natural key (e.g. CLM100001) |
| `nhs_trust_id` | String | FK → dim_trust |
| `provider_id` | String | FK → dim_provider |
| `procedure_code` | String | FK → dim_procedure |
| `service_date_key` | Integer | FK → dim_date (YYYYMMDD) |
| `financial_year` | String | e.g. FY2023-24 |
| `claimed_amount` | Decimal | Amount submitted by provider |
| `approved_amount` | Decimal | Amount approved for payment |
| `variance_amount` | Decimal | Claimed minus approved |
| `is_approved_flag` | Integer | 1 if approved, 0 otherwise |
| `is_rejected_flag` | Integer | 1 if rejected, 0 otherwise |
| `claim_lifecycle_stage` | String | Open / Closed |
| `patient_age_band` | String | 0-17, 18-34, 35-49, 50-64, 65+ |
| `patient_gender` | String | M / F / U |
| `submission_lag_days` | Integer | Days between service and submission |

---

### Dimension Tables

#### `dim_date`
| Column | Type | Description |
|--------|------|-------------|
| `date_key` | Integer | PK (YYYYMMDD) |
| `calendar_date` | Date | Actual date |
| `calendar_year` | Integer | 2022 / 2023 / 2024 |
| `calendar_month_name` | String | January … December |
| `nhs_financial_year` | String | FY2023-24 etc |
| `nhs_financial_quarter` | String | Q1 / Q2 / Q3 / Q4 |
| `is_weekday` | Integer | 1 = weekday, 0 = weekend |

#### `dim_trust`
| Column | Type | Description |
|--------|------|-------------|
| `nhs_trust_id` | String | PK |
| `nhs_trust_name` | String | Display name |
| `region` | String | Geographic region |
| `budget_gbp` | Integer | Annual budget |

#### `dim_provider`
| Column | Type | Description |
|--------|------|-------------|
| `provider_id` | String | PK |
| `provider_name` | String | Display name |
| `provider_type` | String | HOSPITAL / CLINIC / GP_PRACTICE etc |
| `region` | String | Geographic region |

#### `dim_procedure`
| Column | Type | Description |
|--------|------|-------------|
| `procedure_code` | String | PK |
| `procedure_desc` | String | Human-readable description |
| `procedure_category` | String | Elective / Surgical / Therapy etc |
| `standard_tariff_gbp` | Decimal | Reference tariff price |

---

## Relationships

```
fact_claims.service_date_key  ──→  dim_date.date_key        (Many-to-One)
fact_claims.nhs_trust_id      ──→  dim_trust.nhs_trust_id   (Many-to-One)
fact_claims.provider_id       ──→  dim_provider.provider_id (Many-to-One)
fact_claims.procedure_code    ──→  dim_procedure.procedure_code (Many-to-One)
```

---

## Calculated Measures (DAX / Power BI)

These measures would be defined in the Power BI semantic model.  
Listed here in pseudo-DAX for documentation purposes.

### Financial KPIs

```dax
-- Total Claimed Amount
Total Claimed (£) =
    SUM(fact_claims[claimed_amount])

-- Total Approved Amount
Total Approved (£) =
    SUM(fact_claims[approved_amount])

-- Total Variance
Total Variance (£) =
    [Total Claimed (£)] - [Total Approved (£)]

-- Financial Approval Rate
Financial Approval Rate % =
    DIVIDE(
        SUM(fact_claims[approved_amount]),
        SUM(fact_claims[claimed_amount]),
        0
    ) * 100

-- Average Claim Value
Average Claimed Per Claim (£) =
    AVERAGEX(fact_claims, fact_claims[claimed_amount])
```

### Volume KPIs

```dax
-- Total Claims
Total Claims =
    COUNTROWS(fact_claims)

-- Approved Claims
Approved Claims =
    CALCULATE(COUNTROWS(fact_claims), fact_claims[is_approved_flag] = 1)

-- Claim Approval Rate
Claim Approval Rate % =
    DIVIDE([Approved Claims], [Total Claims], 0) * 100

-- Pending Claims
Open Claims =
    CALCULATE(COUNTROWS(fact_claims), fact_claims[claim_lifecycle_stage] = "Open")
```

### Operational KPIs

```dax
-- Average Submission Lag
Avg Submission Lag (Days) =
    AVERAGE(fact_claims[submission_lag_days])

-- Claims Submitted Late (> 14 days)
Late Submissions =
    CALCULATE(COUNTROWS(fact_claims), fact_claims[submission_lag_days] > 14)

-- Late Submission Rate
Late Submission Rate % =
    DIVIDE([Late Submissions], [Total Claims], 0) * 100
```

### Time Intelligence

```dax
-- Claims YTD
Claims YTD =
    CALCULATE([Total Claims], DATESYTD(dim_date[calendar_date]))

-- Approved £ YTD (NHS FY)
Approved YTD NHS FY (£) =
    CALCULATE(
        [Total Approved (£)],
        DATESYTD(dim_date[calendar_date], "31 Mar")
    )

-- Prior Year Claims (for variance)
Claims Prior Year =
    CALCULATE([Total Claims], SAMEPERIODLASTYEAR(dim_date[calendar_date]))

-- YoY Claims Growth %
Claims YoY Growth % =
    DIVIDE([Total Claims] - [Claims Prior Year], [Claims Prior Year], 0) * 100
```

---

## Report Pages (Power BI)

### Page 1: Executive Summary
| Visual | Measure | Slicers |
|--------|---------|---------|
| Card | Total Claims | Financial Year |
| Card | Total Approved (£) | NHS Trust |
| Card | Claim Approval Rate % | — |
| Card | Avg Submission Lag (Days) | — |
| Line Chart | Monthly claims volume (trend) | Financial Year |
| Bar Chart | Approved (£) by Trust | — |
| Donut | Claims by Status | — |

### Page 2: Trust Performance
| Visual | Measure | Slicers |
|--------|---------|---------|
| Table | All KPIs by Trust + FY | Financial Year |
| Bar Chart | Financial Approval Rate % by Trust | — |
| Scatter | Claim Volume vs Approval Rate | Trust |

### Page 3: Provider Activity
| Visual | Measure | Slicers |
|--------|---------|---------|
| Table | Provider, Claims, Approved (£), Lag | Provider Type |
| Bar Chart | Top providers by approved value | Region |

### Page 4: Procedure Analytics
| Visual | Measure | Slicers |
|--------|---------|---------|
| Matrix | Procedure × Trust, Approved (£) | Category |
| Bar Chart | Claim volume by procedure category | — |

---

## Data Refresh Strategy

| Layer | Refresh Pattern | Frequency |
|-------|-----------------|-----------|
| Bronze | Full or incremental (by `submission_date`) | Daily |
| Silver | Incremental using Delta merge on `claim_id` | Daily |
| Gold | Full recompute (small table, fast) | Daily |
| Semantic model | Direct Lake (no import) or Scheduled Import | Hourly / Daily |

---

*Semantic model design — portfolio project. All definitions are illustrative.*
