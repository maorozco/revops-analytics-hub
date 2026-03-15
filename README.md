# RevOps Analytics Hub

**End-to-end ELT pipeline for Revenue Operations analytics** — from raw data ingestion through Google BigQuery, dbt transformations with dimensional modeling, to executive-ready dashboards in Power BI.

This project replicates the same architecture a modern data team would build in production to answer critical business questions:

- Which sales reps are hitting quota? Who needs coaching?
- What is the true profitability per product after all costs?
- Which accounts are at risk of churning?
- How does sales activity effort correlate with closed revenue?

> **Note:** This project uses CSV datasets and Python scripts for ingestion. In a production environment, the data sources would be SaaS platforms (Salesforce, HubSpot, Stripe) and the ingestion layer would use managed connectors like Fivetran or Airbyte. The transformation and modeling layers (dbt + BigQuery) are identical to what runs in enterprise environments. See the [Enterprise Equivalents](#enterprise-equivalents) section for a full comparison.

---

## Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   DATA SOURCES   │     │    INGESTION     │     │   TRANSFORM      │     │    CONSUME       │
│                  │     │    (Python)      │     │   (dbt Core)     │     │   (Power BI)     │
├──────────────────┤     ├──────────────────┤     ├──────────────────┤     ├──────────────────┤
│ Kaggle CRM data  │────>│ load_to_         │────>│ staging (views)  │────>│ Executive        │
│ Faker synthetic  │     │ bigquery.py      │     │ intermediate     │     │ dashboards       │
│ (9 CSV files)    │     │                  │     │ marts (tables)   │     │                  │
└──────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
        │                        │                        │                        │
   9 source tables        BigQuery: raw            BigQuery: analytics       Star schema
   60,181 rows            (9 tables)               (17 models)              (3 dim + 2 fct)
```

### What each layer demonstrates

| Layer | What I built | Skill demonstrated |
|-------|-------------|-------------------|
| Ingestion | Python script with explicit schemas loading CSVs to BigQuery | **Data Engineering** — custom ELT pipeline |
| Transformation | 17 dbt models across 3 layers with Jinja templating | **Analytics Engineering** — dbt + SQL |
| Modeling | Star schema with 3 dimensions + 2 fact tables | **Dimensional modeling** — Kimball methodology |
| Data quality | 30 automated dbt tests catching real data issues | **Data governance** — testing & validation |
| Synthetic data | Python Faker generating 5 realistic enterprise tables | **Automation** — reproducible data generation |
| Dashboards | Power BI connected to BigQuery marts | **BI & storytelling** — actionable insights |

---

## Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Data Sources | Kaggle + Python Faker | Raw CRM data + synthetic enterprise data |
| Ingestion | Python (pandas + google-cloud-bigquery) | Extract CSVs, load to BigQuery with explicit schemas |
| Data Warehouse | Google BigQuery | Serverless cloud warehouse — 2 datasets: `raw` + `analytics` |
| Transformation | dbt Core 1.11 | SQL-based transformations with testing and documentation |
| BI Layer | Power BI | Interactive dashboards connected to BigQuery marts |
| Version Control | Git + GitHub | Code, models, and documentation |

---

## Data Sources

### Raw data (Kaggle — CRM Sales Pipeline)

These 4 tables simulate what would come from a **CRM system like Salesforce or HubSpot** via a connector like Fivetran:

| Table | Rows | Simulates | Description |
|-------|------|-----------|-------------|
| `accounts` | 85 | CRM Accounts | Customer accounts with annual revenue, sector, and employee count |
| `products` | 7 | Product Catalog | Product lines (GTX, MG, Alpha series) with list prices |
| `sales_teams` | 35 | CRM Users | Sales agent hierarchy: Agent → Manager → Regional Office |
| `sales_pipeline` | 8,800 | CRM Opportunities | Sales deals with stages (Prospecting → Won/Lost), values, and dates |

### Synthetic data (Python Faker — Enterprise systems)

These 5 tables were generated with Python Faker to simulate data from **separate enterprise systems** that don't have pre-built connectors — exactly the scenario where a Data Engineer writes custom ingestion:

| Table | Rows | Simulates | Description |
|-------|------|-----------|-------------|
| `quotas` | 420 | CRM Quotas (Xactly/Salesforce) | Monthly sales targets per agent with seasonal adjustments |
| `activities` | 48,983 | CRM Activities | Calls, emails, meetings, demos linked to opportunities |
| `costs` | 7 | ERP (SAP/NetSuite) | Product cost structure: COGS, shipping, support, commission % |
| `nps_surveys` | 262 | Survey Tool (Qualtrics) | Quarterly NPS surveys per account with scores and comments |
| `support_tickets` | 1,582 | Support Platform (Zendesk) | Tickets with priority, category, resolution time, satisfaction |

**Total: 60,181 rows across 9 tables from 5 different source systems**

---

## dbt Transformation Layers

### Layer 1: Staging (`stg_`) — Clean & Standardize

**Materialized as: VIEWS** (no storage cost, recalculated on query)

One model per raw table. No business logic — only rename columns to consistent conventions, cast data types, and handle nulls. This is the single point of contact with raw data.

```
raw.accounts       →  stg_accounts       (account → account_name, revenue → annual_revenue_usd)
raw.products       →  stg_products       (product → product_name, sales_price → list_price_usd)
raw.sales_pipeline →  stg_sales_pipeline (close_value → close_value_usd, deal_stage standardized)
raw.quotas         →  stg_quotas         (includes deduplication window function — see Data Quality)
...9 models total
```

### Layer 2: Intermediate (`int_`) — Business Logic

**Materialized as: VIEWS** (reusable building blocks, never exposed to dashboards)

This is where tables are joined and business metrics are calculated:

| Model | Joins | Key metrics |
|-------|-------|-------------|
| `int_deal_economics` | pipeline + costs | `net_profit_usd` (revenue - COGS - shipping - support - commission), `days_in_pipeline`, `is_won` |
| `int_sales_performance` | deals + quotas + activities | `quota_attainment_pct`, `total_revenue_usd`, `total_activities`, `positive_outcomes` |
| `int_customer_health` | accounts + NPS + tickets + deals | `health_score` (0-100), `churn_risk` (High/Medium/Low), `avg_nps_score` |

**Health score formula:**
```
health_score = (avg_nps * 0.4) + (avg_satisfaction * 0.3) - high_priority_ticket_penalty + buyer_bonus
```
- 40% weight on NPS score
- 30% weight on CSAT (support satisfaction)
- Penalty for high-priority unresolved tickets
- +15 bonus for accounts with active purchases

**Churn risk classification:**
- **High**: NPS ≤ 3 AND ≥ 2 high-priority tickets
- **Medium**: NPS ≤ 5 OR CSAT < 3
- **Low**: Everything else

### Layer 3: Marts (`dim_` / `fct_`) — Star Schema

**Materialized as: TABLES** (pre-computed for fast dashboard queries)

Final analytical models consumed by Power BI. Designed using Kimball dimensional modeling:

#### Dimensions

| Model | Primary key | Business classifications |
|-------|------------|------------------------|
| `dim_accounts` | `account_name` | `account_tier` (Enterprise ≥$500M / Mid-Market ≥$100M / SMB), `health_score`, `churn_risk` |
| `dim_products` | `product_name` | `margin_tier` (High ≥40% / Medium ≥25% / Low), full cost structure |
| `dim_sales_teams` | `agent_name` | `manager_name`, `regional_office` |

#### Facts

| Model | Grain | Key measures |
|-------|-------|-------------|
| `fct_sales_pipeline` | One row per opportunity | `close_value_usd`, `net_profit_usd`, `days_in_pipeline`, `gross_margin_pct` |
| `fct_sales_performance` | One row per agent per month | `quota_attainment_pct`, `deals_won`, `total_revenue_usd`, `performance_tier` |

**Performance tier classification:**
- Star Performer: ≥120% quota attainment
- On Target: ≥100%
- Needs Improvement: ≥80%
- Underperforming: <80%

---

## Data Quality

### 30 dbt tests across all layers

Every primary key is validated with `unique` + `not_null` tests. dbt runs these automatically after each transformation:

```bash
dbt test --profiles-dir .
# Finished running 30 tests — all passed
```

### Real issue caught: duplicate `quota_id`

During development, `dbt test` flagged duplicate `quota_id` values in the raw quotas table — the same agent had two quota entries for the same month.

**Resolution:** Added a `ROW_NUMBER()` window function in `stg_quotas` that deduplicates by keeping the highest quota amount per agent per month:

```sql
-- stg_quotas.sql (simplified)
with deduplicated as (
    select *,
        row_number() over (
            partition by quota_id
            order by quota desc
        ) as row_num
    from {{ source('raw', 'quotas') }}
)
select ... from deduplicated where row_num = 1
```

This follows the best practice of resolving data quality issues at the **earliest transformation layer** (staging), so downstream models always work with clean data.

---

## BigQuery Setup

### Datasets

| Dataset | Purpose | Contents |
|---------|---------|----------|
| `raw` | Landing zone for ingested data | 9 tables (loaded by Python script) |
| `analytics` | dbt-managed transformations | 12 views (staging + intermediate) + 5 tables (marts) |

### Authentication

```bash
# Development (what this project uses): personal OAuth
gcloud auth application-default login

# Production (what enterprises use): Service Account
# - JSON key stored in Secrets Manager
# - Attached to Airflow/Cloud Function/ECS task
# - Granular IAM roles (bigquery.dataEditor, bigquery.jobUser)
```

### Screenshots 
  ![Raw Dataset](docs/screenshots/bigquery_raw_dataset.jpeg)                                   ![Analytics Dataset](docs/screenshots/bigquery_analytics_dataset.jpeg)
  ![dbt run](docs/screenshots/dbt_run_result.jpeg)                                             ![dbt test](docs/screenshots/dbt_test_result.jpeg)

---

## Enterprise Equivalents

Each component of this project maps directly to tools used in production data teams. The architecture and logic are the same — only the source of data and the level of automation differ:

### This project vs. production

```
THIS PROJECT:
  Kaggle CSVs ──→ Python script ──→ BigQuery (raw) ──→ dbt ──→ BigQuery (marts) ──→ Power BI

ENTERPRISE:
  Salesforce  ──┐
  Stripe      ──┤──→ Fivetran ──→ BigQuery (raw) ──→ dbt Cloud ──→ BigQuery (marts) ──→ Looker
  Google Ads  ──┤                      │
  Internal DB ──┘               Airflow (orchestration)
                          (Python custom for sources without connectors)
```

### Component-by-component comparison

| This project | Production equivalent | When each is used |
|--------------|----------------------|-------------------|
| **CSV files** | Salesforce, HubSpot, SAP, Zendesk, Stripe APIs | Real SaaS/ERP systems as data sources |
| **Python ingestion** (`load_to_bigquery.py`) | **Fivetran / Airbyte** (300+ pre-built connectors) | Standard SaaS sources with existing connectors |
| **Python custom script** (same approach) | **Custom Python** — exactly what we built | When no connector exists: internal APIs, FTP servers, legacy ERPs |
| **Manual execution** | **Apache Airflow / Dagster / Prefect** | Schedule pipelines: "run daily at 6am, alert on failure" |
| **BigQuery** | **Snowflake / Redshift / Databricks** | Alternative cloud data warehouses |
| **dbt Core** (CLI) | **dbt Cloud** | Managed dbt with scheduling, CI/CD, docs hosting |
| **`WRITE_TRUNCATE`** (full reload) | **`WRITE_APPEND`** + deduplication (incremental) | Production uses incremental loads for large datasets |
| **`gcloud auth login`** (personal) | **Service Account + Workload Identity Federation** | Production auth is machine-based, not person-based |
| **Power BI** | **Looker / Tableau / Metabase** | Alternative BI platforms |

### Ingestion: connectors vs. custom code

| Approach | Tool | Best for | Cost |
|----------|------|----------|------|
| **Managed connector** | Fivetran, Airbyte, Stitch, Hevo Data | SaaS sources with APIs (Salesforce, Stripe, Google Ads) | $$$$ (Fivetran) to free (Airbyte self-hosted) |
| **Custom Python** (what we built) | pandas + BigQuery SDK | Internal APIs, legacy systems, FTP, sources without connectors | Cloud compute cost only |
| **Cloud-native ETL** | AWS Glue, GCP Dataflow | Heavy data processing at scale | Pay per job |

> **Why the Python script matters for your portfolio:** Custom ingestion scripts are exactly what a Data Engineer builds when no pre-built connector exists. This is the hardest part of data engineering — and the most valuable skill to demonstrate.

### Data volume: when to scale beyond this approach

| Data size | Method | Tools |
|-----------|--------|-------|
| < 1 GB (our case) | `pd.read_csv` → load to BigQuery | pandas + BigQuery SDK |
| 1–10 GB | Python with chunked reads | `pd.read_csv(chunksize=50000)` |
| 10–100 GB | Upload to Cloud Storage first, then BigQuery LOAD | GCS + `bq load` |
| 100 GB – TB | Distributed processing | Spark (Databricks / Dataproc / EMR) |
| > TB | Streaming or incremental loads | Kafka → BigQuery, CDC pipelines |

---

## Project Structure

```
revops-analytics-hub/
├── python/
│   ├── generate_synthetic_data.py    # Faker: generates 5 realistic enterprise tables (467 lines)
│   └── load_to_bigquery.py           # ELT: loads 9 CSVs to BigQuery with explicit schemas (186 lines)
├── data/
│   ├── raw/                          # Kaggle CRM dataset (4 CSVs, 8,927 rows)
│   │   ├── accounts.csv
│   │   ├── products.csv
│   │   ├── sales_teams.csv
│   │   └── sales_pipeline.csv
│   └── generated/                    # Faker synthetic data (5 CSVs, 51,254 rows)
│       ├── quotas.csv
│       ├── activities.csv
│       ├── costs.csv
│       ├── nps_surveys.csv
│       └── support_tickets.csv
├── revops_analytics/                 # dbt project
│   ├── dbt_project.yml               # Project config: name, profile, materialization rules
│   ├── profiles.yml                  # BigQuery connection: project, dataset, auth method
│   └── models/
│       ├── staging/                  # 9 stg_ models + src_raw.yml + stg_models.yml
│       ├── intermediate/            # 3 int_ models + int_models.yml
│       └── marts/                   # 5 dim_/fct_ models + marts_models.yml
└── README.md
```

---

## How to Run

### Prerequisites
- Python 3.10+
- Google Cloud SDK (`gcloud`)
- A GCP project with BigQuery enabled

### Step-by-step

```bash
# 1. Clone and set up environment
git clone https://github.com/maorozco/revops-analytics-hub.git
cd revops-analytics-hub
python -m venv .venv
source .venv/bin/activate
pip install pandas faker numpy google-cloud-bigquery pyarrow db-dtypes dbt-bigquery

# 2. Authenticate with GCP
gcloud auth application-default login

# 3. Generate synthetic data (creates 5 CSVs in data/generated/)
cd python
python generate_synthetic_data.py

# 4. Load all 9 tables to BigQuery (raw dataset)
python load_to_bigquery.py

# 5. Run dbt transformations (creates 17 models in analytics dataset)
cd ../revops_analytics
dbt run --profiles-dir .

# 6. Run data quality tests (30 tests)
dbt test --profiles-dir .
```

---

## Key Design Decisions

1. **ELT over ETL** — Raw data is loaded as-is to BigQuery. All transformations happen in SQL via dbt, making them version-controlled, testable, and auditable. This is the modern standard (2015+) replacing legacy ETL tools like Informatica and SSIS.

2. **Star schema (Kimball)** — Marts use dimensional modeling (`dim_` / `fct_`) — the standard for analytical workloads and BI tools. Dimensions contain descriptive attributes; facts contain measurable events.

3. **Three transformation layers** — Staging (clean), Intermediate (logic), Marts (serve). Each layer has a clear responsibility, making the pipeline debuggable and maintainable.

4. **Views for staging/intermediate, Tables for marts** — Views cost $0 in storage and always reflect the latest data. Tables are pre-computed for the models that dashboards query hundreds of times per day.

5. **Explicit schemas on ingestion** — BigQuery load uses `SchemaField` definitions instead of auto-detect, ensuring type safety and catching schema drift early.

6. **Deduplication at the earliest layer** — Data quality issues (duplicate quota_ids) are resolved in staging so all downstream models work with clean data.

7. **Synthetic data with realistic patterns** — Generated tables follow real business logic: NPS scores correlate with deal activity, resolution times vary by ticket priority, quotas have seasonal adjustments (Q4 targets are 15% higher).

---

## Author

**Manuela Orozco** — Analytics Engineer

Built as a portfolio project demonstrating end-to-end data pipeline design, from raw ingestion to executive insights.

[LinkedIn](https://linkedin.com/in/manuela-orozco-ochoa) · [Email](mailto:maorozcooc@unal.edu.co)

