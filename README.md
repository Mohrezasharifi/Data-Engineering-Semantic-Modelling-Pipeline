# 🏥 Data Migration Validation Framework
### Oracle SQL → Apache Spark (Microsoft Fabric) | Healthcare Claims Data

> ⚠️ **Portfolio Notice:** This is a mocked portfolio project inspired by real-world experience 
> working on NHS-adjacent data migration projects. All data, schemas, organisations, and code 
> are entirely fictional and do not represent any real system, patient, or organisation.

---

## Tech Stack

![Oracle](https://img.shields.io/badge/Oracle-F80000?style=flat&logo=oracle&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat&logo=mysql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Spark SQL](https://img.shields.io/badge/Spark%20SQL-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=flat&logo=microsoft&logoColor=white)
![Lakehouse](https://img.shields.io/badge/Lakehouse-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=databricks&logoColor=white)
![Semantic Model](https://img.shields.io/badge/Semantic%20Model-F2C811?style=flat&logo=powerbi&logoColor=black)

---

## Overview

This project demonstrates a production-style **data migration validation framework** built to 
ensure data fidelity when migrating healthcare claims data from a legacy **Oracle database** 
to **Apache Spark** within a **Microsoft Fabric** Lakehouse environment.

The core challenge in any data migration is proving that the target system contains exactly 
the same data as the source — no rows lost, no values corrupted, no nulls introduced silently. 
This framework makes that verification systematic, repeatable, and auditable.

---

## Problem Statement

A healthcare data team was tasked with migrating several years of NHS-style claims data 
from an on-premise Oracle relational database to a cloud-based Microsoft Fabric Lakehouse 
using Apache Spark. The key concerns were:

- **Data loss:** Would all 5,000+ claim records survive the migration intact?
- **Financial accuracy:** Would aggregate values (total approved spend by trust) match exactly?
- **Null integrity:** Could the ETL pipeline introduce unexpected NULL values in financial fields?
- **SQL compatibility:** Many legacy Oracle queries used Oracle-specific functions (`NVL`, `DECODE`, `LISTAGG`, `TO_CHAR`) that do not exist in Spark SQL.

---

## Solution Approach

```
Oracle DB → CSV Extract → Fabric OneLake (Bronze) → Spark Transform → Validated Lakehouse (Silver)
                                                           ↕
                                               Python Validation Framework
```

The solution has three components:

### 1. SQL Translation Layer
A systematic mapping of all Oracle-specific SQL functions to their Spark SQL equivalents, 
documented in `sql_translation_helper.py`. Key translations:

| Oracle | Spark SQL | Risk |
|--------|-----------|------|
| `NVL(x, 0)` | `COALESCE(x, 0)` | 🟢 Low |
| `DECODE(x, a, b, c)` | `CASE WHEN x=a THEN b ELSE c END` | 🔴 High |
| `TO_CHAR(d, 'YYYY-MM')` | `DATE_FORMAT(d, 'yyyy-MM')` | 🔴 High |
| `TRUNC(d, 'MM')` | `DATE_TRUNC('month', d)` | 🟡 Medium |
| `LISTAGG(x, ',') WITHIN GROUP (...)` | `ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(x)), ',')` | 🔴 High |
| `SYSDATE` | `current_date()` | 🟢 Low |
| `NVL2(x, a, b)` | `CASE WHEN x IS NOT NULL THEN a ELSE b END` | 🟢 Low |

### 2. Python Validation Framework
A modular `MigrationValidator` class in `src/migration_validator.py` that runs:
- **Row count checks** — with configurable tolerance thresholds
- **Schema validation** — catches missing or renamed columns
- **Null analysis** — per-column comparison of null counts
- **Aggregate validation** — SUM and AVG comparison with numeric tolerance
- **Duplicate detection** — source and target separately
- **Categorical distributions** — ensures reference values haven't changed
- **PK completeness** — set-based comparison of primary key values

### 3. Jupyter Notebook Walkthrough
An interactive notebook demonstrating the full validation run against the mocked dataset, 
including charts and a simulated ETL defect for demonstration purposes.

---

## Tools & Technologies

| Tool | Role |
|------|------|
| **Python / Pandas** | Local validation framework (mirrors PySpark patterns) |
| **Apache Spark / Spark SQL** | Target query engine (Microsoft Fabric) |
| **Microsoft Fabric** | Cloud Lakehouse platform (OneLake, Delta Lake) |
| **Oracle SQL** | Source system query language |
| **Delta Lake** | Target storage format (ACID, time travel) |
| **Jupyter Notebook** | Interactive documentation and demonstration |
| **Matplotlib** | Validation result visualisation |

> In production, `pandas` DataFrames would be replaced with PySpark DataFrames. 
> The validation logic is structurally identical — Spark's DataFrame API mirrors pandas closely.

---

## Repository Structure

```
project1-migration-validation/
│
├── README.md                          ← This file
│
├── data/
│   ├── healthcare_claims.csv          ← Mocked NHS-style claims (5,000 rows)
│   ├── providers.csv                  ← Provider reference data
│   └── nhs_trusts.csv                 ← Trust reference data
│
├── sql/
│   ├── oracle/
│   │   └── claims_analysis.sql        ← Original Oracle SQL queries
│   └── spark/
│       └── claims_analysis_spark.sql  ← Spark SQL equivalents (with migration notes)
│
├── src/
│   ├── migration_validator.py         ← Core validation framework
│   └── sql_translation_helper.py     ← Oracle → Spark function reference + scanner
│
├── notebooks/
│   └── 01_migration_validation.ipynb ← Full walkthrough notebook
│
└── reports/
    ├── data_risk_assessment.md        ← Formal risk assessment document
    └── validation_overview.png        ← Generated chart (run notebook to produce)
```

---

## Getting Started

### Prerequisites
```bash
python >= 3.10
pip install pandas numpy matplotlib jupyter
```

### Run the Validation Framework
```python
import pandas as pd
import sys
sys.path.insert(0, 'src')

from migration_validator import MigrationValidator

source_df = pd.read_csv('data/healthcare_claims.csv')
target_df = pd.read_csv('data/healthcare_claims.csv')  # Replace with actual target

validator = MigrationValidator(
    source_df=source_df,
    target_df=target_df,
    table_name='nhs_claims',
    pk_column='claim_id',
)

report = validator.run_all_checks()
MigrationValidator.print_report(report)
```

### Scan SQL for Oracle-specific Functions
```python
from sql_translation_helper import scan_sql_file
scan_sql_file('sql/oracle/claims_analysis.sql')
```

### Run the Notebook
```bash
cd notebooks
jupyter notebook 01_migration_validation.ipynb
```

---

## Sample Validation Output

```
======================================================================
  MIGRATION VALIDATION REPORT — NHS_CLAIMS
======================================================================
  Source rows : 5,000
  Target rows : 5,000
  Total checks: 18
  ✅ Passed   : 15
  ❌ Failed   : 1
  ⚠️  Warnings : 2

  OVERALL STATUS: >>> FAILED <<<
======================================================================

  ❌ [CRITICAL] Row Count
     Both source and target have 5,000 rows.

  ❌ [HIGH] Aggregate Check: approved_amount
     SUM mismatch: Source £2,341,890 vs Target £2,339,440 (Δ 0.1046%).

  ⚠️  [MEDIUM] Null Check: approved_amount
     Target has 12 MORE nulls than source (Source: 47 | Target: 59).
======================================================================
```

---

## Key Learnings

**1. Silent failures are the biggest migration risk.**  
Date format mask differences (`YYYY` vs `yyyy`) don't throw errors — they produce wrong values. 
Only systematic aggregate comparison catches these.

**2. NULL handling is financially significant.**  
In claims data, a NULL in `approved_amount` means that record contributes £0 to totals. 
12 unexpectedly nulled records = thousands of pounds in reporting discrepancy.

**3. DECODE → CASE WHEN requires careful NULL review.**  
Oracle's `DECODE` treats `NULL = NULL` as true. Standard `CASE WHEN x = NULL` never matches.
Migration must handle this edge case explicitly.

**4. Validation should be automated and re-run on every load.**  
A one-time migration check is not enough. Incremental loads need the same framework applied 
on every batch, ideally triggered as part of the Fabric Data Factory pipeline.

---

## Related Project

**[Project 2: Data Engineering & Semantic Modelling Pipeline](../project2-semantic-pipeline)**  
Extends this dataset into a full end-to-end transformation pipeline with a semantic model 
designed for Power BI reporting.

---

*Portfolio project — mocked data, real techniques.*
