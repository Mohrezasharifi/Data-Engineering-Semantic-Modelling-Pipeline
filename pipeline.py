"""
pipeline.py
===========
End-to-end data transformation pipeline for NHS claims data.

Simulates a Bronze → Silver → Gold Medallion Architecture
as implemented in Microsoft Fabric / Delta Lake.

In production, pandas DataFrames would be PySpark DataFrames and
this script would run as a Fabric Notebook or Data Factory activity.

Layers:
    Bronze  → Raw ingested data (no transformation)
    Silver  → Cleansed, typed, validated data + dimension tables
    Gold    → Aggregated, reporting-ready data (semantic layer inputs)
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)


def log(msg: str) -> None:
    """Simple timestamped logger."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ═══════════════════════════════════════════════════════════════
# BRONZE LAYER — Raw Ingestion
# ═══════════════════════════════════════════════════════════════

def load_bronze() -> dict[str, pd.DataFrame]:
    """
    Simulates reading raw source data into the Bronze layer.
    In Fabric, this would be Delta tables loaded via COPY INTO
    or a Data Factory pipeline.

    Returns a dict of DataFrames keyed by table name.
    """
    log("BRONZE: Loading raw source files...")

    claims = pd.read_csv(
        os.path.join(DATA_DIR, 'healthcare_claims.csv'),
        parse_dates=['service_date', 'submission_date'],
    )
    providers = pd.read_csv(os.path.join(DATA_DIR, 'providers.csv'))
    trusts = pd.read_csv(
        os.path.join(DATA_DIR, 'nhs_trusts.csv'),
        parse_dates=['established_year'],
        dtype={'established_year': str},
    )

    log(f"  nhs_claims : {len(claims):,} rows")
    log(f"  providers  : {len(providers):,} rows")
    log(f"  nhs_trusts : {len(trusts):,} rows")

    return {'nhs_claims': claims, 'providers': providers, 'nhs_trusts': trusts}


# ═══════════════════════════════════════════════════════════════
# SILVER LAYER — Cleansed + Dimensions
# ═══════════════════════════════════════════════════════════════

def build_dim_date(start: str = '2022-01-01', end: str = '2024-12-31') -> pd.DataFrame:
    """
    Generates a calendar/date dimension table.

    Spark SQL equivalent uses SEQUENCE + EXPLODE (see sql/spark/transformations_spark.sql T2).
    Oracle used CONNECT BY LEVEL.

    Returns a complete dim_date DataFrame.
    """
    log("SILVER: Building dim_date...")

    dates = pd.date_range(start=start, end=end, freq='D')
    df = pd.DataFrame({'calendar_date': dates})

    df['date_key'] = df['calendar_date'].dt.strftime('%Y%m%d').astype(int)
    df['calendar_year'] = df['calendar_date'].dt.year
    df['calendar_month_num'] = df['calendar_date'].dt.month.astype(str).str.zfill(2)
    df['calendar_month_name'] = df['calendar_date'].dt.strftime('%B')
    df['calendar_quarter'] = df['calendar_date'].dt.quarter
    df['iso_week'] = df['calendar_date'].dt.isocalendar().week.astype(int)
    df['is_weekday'] = (df['calendar_date'].dt.dayofweek < 5).astype(int)

    # NHS Financial Year (starts April 1)
    def nhs_fy(row):
        month = row['calendar_date'].month
        year = row['calendar_date'].year
        if month >= 4:
            return f"FY{year}-{str(year + 1)[2:]}"
        else:
            return f"FY{year - 1}-{str(year)[2:]}"

    def nhs_fq(month):
        if month in (4, 5, 6):   return 'Q1'
        elif month in (7, 8, 9): return 'Q2'
        elif month in (10,11,12):return 'Q3'
        else:                     return 'Q4'

    df['nhs_financial_year'] = df.apply(nhs_fy, axis=1)
    df['nhs_financial_quarter'] = df['calendar_date'].dt.month.map(nhs_fq)

    log(f"  dim_date: {len(df):,} rows ({start} → {end})")
    return df


def build_dim_trust(trusts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the NHS Trust dimension.
    Adds a surrogate key for joining.
    """
    log("SILVER: Building dim_trust...")
    dim = trusts_df.copy()
    dim['trust_sk'] = range(1, len(dim) + 1)  # Surrogate key
    # Reorder columns: SK first
    cols = ['trust_sk'] + [c for c in dim.columns if c != 'trust_sk']
    return dim[cols]


def build_dim_provider(providers_df: pd.DataFrame) -> pd.DataFrame:
    """Builds the Provider dimension with surrogate key."""
    log("SILVER: Building dim_provider...")
    dim = providers_df.copy()
    dim['provider_sk'] = range(1, len(dim) + 1)
    cols = ['provider_sk'] + [c for c in dim.columns if c != 'provider_sk']
    return dim[cols]


def build_dim_procedure() -> pd.DataFrame:
    """
    Builds a procedure code dimension from the embedded reference data.
    In production this would come from a clinical coding reference table.
    """
    log("SILVER: Building dim_procedure...")
    procedures = [
        ("EA01", "Initial Assessment",           "Elective",   180.00),
        ("EA02", "Follow-up Consultation",        "Elective",    95.00),
        ("RD01", "Diagnostic Imaging - Standard", "Diagnostic", 240.00),
        ("RD02", "Diagnostic Imaging - Complex",  "Diagnostic", 450.00),
        ("TH01", "Physiotherapy Session",         "Therapy",     75.00),
        ("TH02", "Occupational Therapy",          "Therapy",     90.00),
        ("SU01", "Minor Surgical Procedure",      "Surgical",   850.00),
        ("SU02", "Day Case Surgery",              "Surgical",  1400.00),
        ("PM01", "Medication Management Review",  "Pharmacy",    65.00),
        ("MH01", "Mental Health Assessment",      "Mental Health", 200.00),
    ]
    return pd.DataFrame(procedures, columns=[
        'procedure_code', 'procedure_desc', 'procedure_category', 'standard_tariff_gbp'
    ])


def build_fact_claims(
    claims_df: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    """
    Builds the central FACT_CLAIMS table by:
    - Cleaning and typing source data
    - Deriving calculated fields (flags, bands, lags)
    - Joining date dimension keys

    This mirrors the logic in sql/spark/transformations_spark.sql T1.

    NOTE: In production Spark, this would be a Spark SQL CREATE TABLE AS SELECT
    or a PySpark DataFrame transformation chain.
    """
    log("SILVER: Building fact_claims...")

    f = claims_df.copy()

    # 1. Handle nulls — mirrors COALESCE(approved_amount, 0)
    f['approved_amount'] = f['approved_amount'].fillna(0.0)

    # 2. Derived financial measures
    f['variance_amount'] = f['claimed_amount'] - f['approved_amount']

    # 3. Status flags — mirrors CASE WHEN claim_status = 'APPROVED' THEN 1 ELSE 0
    f['is_approved_flag'] = (f['claim_status'] == 'APPROVED').astype(int)
    f['is_rejected_flag'] = (f['claim_status'] == 'REJECTED').astype(int)

    # 4. Lifecycle stage — mirrors DECODE replacement
    lifecycle_map = {'APPROVED': 'Closed', 'REJECTED': 'Closed'}
    f['claim_lifecycle_stage'] = f['claim_status'].map(lifecycle_map).fillna('Open')

    # 5. Patient age band
    bins = [0, 17, 34, 49, 64, 200]
    labels = ['0-17', '18-34', '35-49', '50-64', '65+']
    f['patient_age_band'] = pd.cut(
        f['patient_age'], bins=bins, labels=labels, right=True
    ).astype(str)
    f['patient_age_band'] = f['patient_age_band'].replace('nan', 'Unknown')

    # 6. Submission lag in days — mirrors DATEDIFF()
    f['submission_lag_days'] = (f['submission_date'] - f['service_date']).dt.days

    # 7. Date dimension keys — mirrors CAST(DATE_FORMAT(..., 'yyyyMMdd') AS INT)
    f['service_date_key'] = f['service_date'].dt.strftime('%Y%m%d').astype(int)
    f['submission_date_key'] = f['submission_date'].dt.strftime('%Y%m%d').astype(int)

    # 8. Select and order final columns
    fact_cols = [
        'claim_id', 'nhs_trust_id', 'provider_id', 'procedure_code',
        'diagnosis_code', 'service_date_key', 'submission_date_key',
        'financial_year', 'claimed_amount', 'approved_amount', 'variance_amount',
        'is_approved_flag', 'is_rejected_flag', 'claim_lifecycle_stage',
        'patient_age_band', 'patient_gender', 'patient_ethnicity_code',
        'submission_lag_days',
    ]
    fact = f[fact_cols]

    log(f"  fact_claims: {len(fact):,} rows, {len(fact.columns)} columns")
    return fact


# ═══════════════════════════════════════════════════════════════
# GOLD LAYER — Aggregated Reporting Tables
# ═══════════════════════════════════════════════════════════════

def build_gold_trust_kpis(
    fact: pd.DataFrame,
    dim_trust: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    """
    KPI summary by Trust and Financial Year.
    Feeds directly into a Power BI matrix or summary card.

    Mirrors sql/spark/transformations_spark.sql T3.
    """
    log("GOLD: Building trust_kpis...")

    # Merge trust name
    f = fact.merge(
        dim_trust[['nhs_trust_id', 'nhs_trust_name']],
        on='nhs_trust_id', how='left'
    )

    agg = (
        f.groupby(['nhs_trust_name', 'financial_year'])
        .agg(
            total_claims=('claim_id', 'count'),
            approved_claims=('is_approved_flag', 'sum'),
            rejected_claims=('is_rejected_flag', 'sum'),
            total_claimed_gbp=('claimed_amount', 'sum'),
            total_approved_gbp=('approved_amount', 'sum'),
            avg_submission_lag=('submission_lag_days', 'mean'),
        )
        .round(2)
        .reset_index()
    )

    # Derived KPIs
    agg['approval_rate_pct'] = (
        agg['approved_claims'] / agg['total_claims'] * 100
    ).round(2)
    agg['financial_approval_rate_pct'] = (
        agg['total_approved_gbp'] / agg['total_claimed_gbp'].replace(0, np.nan) * 100
    ).round(2)
    agg['variance_gbp'] = (agg['total_claimed_gbp'] - agg['total_approved_gbp']).round(2)

    log(f"  trust_kpis: {len(agg):,} rows")
    return agg


def build_gold_monthly_trends(
    fact: pd.DataFrame,
    dim_trust: pd.DataFrame,
) -> pd.DataFrame:
    """
    Monthly claim volume and value trends.
    Designed for a time-series line chart in Power BI.
    Includes a running YTD cumulative total.

    Mirrors sql/spark/transformations_spark.sql T5.
    """
    log("GOLD: Building monthly_trends...")

    f = fact.merge(
        dim_trust[['nhs_trust_id', 'nhs_trust_name']],
        on='nhs_trust_id', how='left'
    )

    # Re-derive service_date from date_key for grouping
    f['service_month'] = pd.to_datetime(
        f['service_date_key'].astype(str), format='%Y%m%d'
    ).dt.to_period('M').dt.to_timestamp()

    monthly = (
        f.groupby(['financial_year', 'service_month', 'nhs_trust_name'])
        .agg(
            claim_volume=('claim_id', 'count'),
            total_claimed=('claimed_amount', 'sum'),
            total_approved=('approved_amount', 'sum'),
            avg_submission_lag=('submission_lag_days', 'mean'),
        )
        .round(2)
        .reset_index()
        .sort_values(['nhs_trust_name', 'service_month'])
    )

    # YTD cumulative — mirrors window function in Spark SQL
    monthly['ytd_claimed_cumulative'] = (
        monthly.groupby(['nhs_trust_name', 'financial_year'])['total_claimed']
        .cumsum()
        .round(2)
    )

    log(f"  monthly_trends: {len(monthly):,} rows")
    return monthly


def build_gold_procedure_summary(fact: pd.DataFrame, dim_proc: pd.DataFrame) -> pd.DataFrame:
    """
    Procedure-level performance summary.
    Useful for clinical commissioners reviewing spend by procedure type.
    """
    log("GOLD: Building procedure_summary...")

    f = fact.merge(dim_proc, on='procedure_code', how='left')

    agg = (
        f.groupby(['procedure_code', 'procedure_desc', 'procedure_category'])
        .agg(
            total_claims=('claim_id', 'count'),
            total_claimed_gbp=('claimed_amount', 'sum'),
            total_approved_gbp=('approved_amount', 'sum'),
            approval_count=('is_approved_flag', 'sum'),
            avg_tariff_variance=('variance_amount', 'mean'),
        )
        .round(2)
        .reset_index()
    )

    agg['approval_rate_pct'] = (
        agg['approval_count'] / agg['total_claims'] * 100
    ).round(2)

    log(f"  procedure_summary: {len(agg):,} rows")
    return agg


# ═══════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRATION
# ═══════════════════════════════════════════════════════════════

def run_pipeline() -> dict[str, pd.DataFrame]:
    """
    Orchestrates the full Bronze → Silver → Gold pipeline.

    In Microsoft Fabric, this would be:
    1. Data Factory pipeline triggering Notebooks
    2. Each Notebook writes a Delta table to OneLake
    3. Lakehouse automatically updates the SQL Analytics Endpoint
    4. Power BI semantic model reads from the SQL endpoint

    Returns all output tables as a dict.
    """
    log("=" * 60)
    log("NHS CLAIMS DATA PIPELINE — Starting")
    log("=" * 60)

    # BRONZE
    raw = load_bronze()

    # SILVER — Dimensions
    dim_date = build_dim_date()
    dim_trust = build_dim_trust(raw['nhs_trusts'])
    dim_provider = build_dim_provider(raw['providers'])
    dim_procedure = build_dim_procedure()

    # SILVER — Fact
    fact_claims = build_fact_claims(raw['nhs_claims'], dim_date)

    # GOLD — Aggregations
    trust_kpis = build_gold_trust_kpis(fact_claims, dim_trust, dim_date)
    monthly_trends = build_gold_monthly_trends(fact_claims, dim_trust)
    procedure_summary = build_gold_procedure_summary(fact_claims, dim_procedure)

    # Write outputs
    outputs = {
        'dim_date': dim_date,
        'dim_trust': dim_trust,
        'dim_provider': dim_provider,
        'dim_procedure': dim_procedure,
        'fact_claims': fact_claims,
        'gold_trust_kpis': trust_kpis,
        'gold_monthly_trends': monthly_trends,
        'gold_procedure_summary': procedure_summary,
    }

    log("\nWriting outputs...")
    for name, df in outputs.items():
        path = os.path.join(OUTPUT_DIR, f'{name}.csv')
        df.to_csv(path, index=False)
        log(f"  ✅ {name}.csv ({len(df):,} rows)")

    log("\n" + "=" * 60)
    log("PIPELINE COMPLETE")
    log("=" * 60)

    return outputs


if __name__ == '__main__':
    results = run_pipeline()
    print("\nFact table sample:")
    print(results['fact_claims'].head(3).to_string())
    print("\nTrust KPIs:")
    print(results['gold_trust_kpis'].to_string(index=False))
