"""
RevOps Analytics Hub — Load to BigQuery
========================================
ELT Pipeline: Extract CSVs from local storage, Load to BigQuery raw dataset.
Transform layer is handled by dbt (not here).

In production, this would be replaced by:
  - Fivetran/Airbyte for SaaS sources (CRM, support, surveys)
  - Custom Python for internal APIs or ERP exports
  - Airflow/Dagster for orchestration

This script demonstrates the Python-based ingestion approach,
which is what a Data Engineer builds when no pre-built connector exists.

Usage:
  1. Authenticate: gcloud auth application-default login
  2. Run: python load_to_bigquery.py
"""

import os
import pandas as pd
from google.cloud import bigquery

# --- Configuration ---
PROJECT_ID = "revops-analytics-hub"
DATASET_RAW = "raw"

BASE_DIR = os.path.dirname(__file__)
RAW_PATH = os.path.join(BASE_DIR, "..", "data", "raw")
GEN_PATH = os.path.join(BASE_DIR, "..", "data", "generated")

# Tables to load: (csv_path, table_name, source_system)
TABLES = [
    # Raw from Kaggle (simulates CRM + product catalog)
    (os.path.join(RAW_PATH, "accounts.csv"), "accounts", "crm"),
    (os.path.join(RAW_PATH, "products.csv"), "products", "product_catalog"),
    (os.path.join(RAW_PATH, "sales_teams.csv"), "sales_teams", "crm"),
    (os.path.join(RAW_PATH, "sales_pipeline.csv"), "sales_pipeline", "crm"),
    # Generated (simulates other enterprise systems)
    (os.path.join(GEN_PATH, "quotas.csv"), "quotas", "crm_quotas"),
    (os.path.join(GEN_PATH, "activities.csv"), "activities", "crm_activities"),
    (os.path.join(GEN_PATH, "costs.csv"), "costs", "erp"),
    (os.path.join(GEN_PATH, "nps_surveys.csv"), "nps_surveys", "survey_tool"),
    (os.path.join(GEN_PATH, "support_tickets.csv"), "support_tickets", "support_platform"),
]

# Schema definitions for type safety (BigQuery auto-detects but explicit is better)
SCHEMAS = {
    "accounts": [
        bigquery.SchemaField("account", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("year_established", "INTEGER"),
        bigquery.SchemaField("revenue", "FLOAT"),
        bigquery.SchemaField("employees", "INTEGER"),
        bigquery.SchemaField("office_location", "STRING"),
        bigquery.SchemaField("subsidiary_of", "STRING"),
    ],
    "products": [
        bigquery.SchemaField("product", "STRING"),
        bigquery.SchemaField("series", "STRING"),
        bigquery.SchemaField("sales_price", "INTEGER"),
    ],
    "sales_teams": [
        bigquery.SchemaField("sales_agent", "STRING"),
        bigquery.SchemaField("manager", "STRING"),
        bigquery.SchemaField("regional_office", "STRING"),
    ],
    "sales_pipeline": [
        bigquery.SchemaField("opportunity_id", "STRING"),
        bigquery.SchemaField("sales_agent", "STRING"),
        bigquery.SchemaField("product", "STRING"),
        bigquery.SchemaField("account", "STRING"),
        bigquery.SchemaField("deal_stage", "STRING"),
        bigquery.SchemaField("engage_date", "DATE"),
        bigquery.SchemaField("close_date", "DATE"),
        bigquery.SchemaField("close_value", "FLOAT"),
    ],
    "quotas": [
        bigquery.SchemaField("quota_id", "STRING"),
        bigquery.SchemaField("sales_agent", "STRING"),
        bigquery.SchemaField("manager", "STRING"),
        bigquery.SchemaField("regional_office", "STRING"),
        bigquery.SchemaField("quota_month", "STRING"),
        bigquery.SchemaField("quota_amount_usd", "FLOAT"),
        bigquery.SchemaField("quota_type", "STRING"),
    ],
    "activities": [
        bigquery.SchemaField("activity_id", "STRING"),
        bigquery.SchemaField("opportunity_id", "STRING"),
        bigquery.SchemaField("sales_agent", "STRING"),
        bigquery.SchemaField("activity_type", "STRING"),
        bigquery.SchemaField("activity_date", "DATE"),
        bigquery.SchemaField("duration_minutes", "INTEGER"),
        bigquery.SchemaField("outcome", "STRING"),
    ],
    "costs": [
        bigquery.SchemaField("product", "STRING"),
        bigquery.SchemaField("series", "STRING"),
        bigquery.SchemaField("sales_price_usd", "INTEGER"),
        bigquery.SchemaField("cogs_usd", "FLOAT"),
        bigquery.SchemaField("shipping_cost_usd", "FLOAT"),
        bigquery.SchemaField("support_cost_per_unit_usd", "FLOAT"),
        bigquery.SchemaField("sales_commission_pct", "FLOAT"),
        bigquery.SchemaField("gross_margin_pct", "FLOAT"),
    ],
    "nps_surveys": [
        bigquery.SchemaField("survey_id", "STRING"),
        bigquery.SchemaField("account", "STRING"),
        bigquery.SchemaField("survey_sent_date", "DATE"),
        bigquery.SchemaField("response_date", "DATE"),
        bigquery.SchemaField("quarter", "STRING"),
        bigquery.SchemaField("nps_score", "INTEGER"),
        bigquery.SchemaField("nps_category", "STRING"),
        bigquery.SchemaField("comment", "STRING"),
        bigquery.SchemaField("survey_channel", "STRING"),
    ],
    "support_tickets": [
        bigquery.SchemaField("ticket_id", "STRING"),
        bigquery.SchemaField("account", "STRING"),
        bigquery.SchemaField("created_date", "DATE"),
        bigquery.SchemaField("resolved_date", "DATE"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("priority", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("resolution_hours", "FLOAT"),
        bigquery.SchemaField("satisfaction_rating", "FLOAT"),
        bigquery.SchemaField("agent_name", "STRING"),
    ],
}


def load_table(client, csv_path, table_name, source_system):
    """Load a single CSV to BigQuery with explicit schema."""
    table_id = f"{PROJECT_ID}.{DATASET_RAW}.{table_name}"

    df = pd.read_csv(csv_path)

    # Parse date columns before loading
    schema = SCHEMAS.get(table_name, None)
    if schema:
        date_cols = [f.name for f in schema if f.field_type == "DATE"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion

    table = client.get_table(table_id)
    print(f"  OK  {table_name:20s} | {table.num_rows:>8,} rows | source: {source_system}")
    return table.num_rows


def main():
    print("=" * 60)
    print("  RevOps Analytics Hub — Load to BigQuery")
    print("=" * 60)
    print(f"  Project:  {PROJECT_ID}")
    print(f"  Dataset:  {DATASET_RAW}")
    print()

    client = bigquery.Client(project=PROJECT_ID)

    total_rows = 0
    for csv_path, table_name, source_system in TABLES:
        if not os.path.exists(csv_path):
            print(f"  XX  {table_name:20s} | FILE NOT FOUND: {csv_path}")
            continue
        rows = load_table(client, csv_path, table_name, source_system)
        total_rows += rows

    print()
    print(f"  Total: {total_rows:,} rows loaded to {PROJECT_ID}.{DATASET_RAW}")
    print("=" * 60)
    print("  Done! Data is ready for dbt transformation.")
    print("=" * 60)


if __name__ == "__main__":
    main()
