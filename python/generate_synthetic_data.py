"""
RevOps Analytics Hub — Synthetic Data Generation
=================================================
Generates complementary tables using Faker + real data patterns
to enrich the CRM dataset for advanced RevOps analytics.

Source (Kaggle):  accounts, products, sales_teams, sales_pipeline
Generated (raw):  quotas, activities, costs, nps_surveys, support_tickets

These tables simulate data from real enterprise systems:
  - quotas          → from CRM Quotas module (Salesforce/Xactly)
  - activities      → from CRM Activity Tracking (Salesforce/HubSpot)
  - costs           → from ERP system (SAP/Netsuite)
  - nps_surveys     → from survey tool (Delighted/Typeform)
  - support_tickets → from support platform (Zendesk/Intercom)


"""

import pandas as pd
import numpy as np
from faker import Faker
import os
from datetime import timedelta

fake = Faker()
Faker.seed(42)
np.random.seed(42)

# --- Paths ---
RAW_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
GEN_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "generated")
os.makedirs(GEN_PATH, exist_ok=True)


def load_raw_data():
    """Load raw CSVs from Kaggle dataset."""
    print("[1/7] Loading raw data...")
    accounts = pd.read_csv(os.path.join(RAW_PATH, "accounts.csv"))
    products = pd.read_csv(os.path.join(RAW_PATH, "products.csv"))
    sales_teams = pd.read_csv(os.path.join(RAW_PATH, "sales_teams.csv"))
    pipeline = pd.read_csv(os.path.join(RAW_PATH, "sales_pipeline.csv"))

    print(f"      accounts:       {len(accounts)} rows")
    print(f"      products:       {len(products)} rows")
    print(f"      sales_teams:    {len(sales_teams)} rows")
    print(f"      sales_pipeline: {len(pipeline)} rows")

    return accounts, products, sales_teams, pipeline


def generate_quotas(sales_teams, pipeline):
    """
    Simulates: CRM Quotas module (Salesforce/Xactly/CaptivateIQ)
    Monthly revenue targets per sales agent, set by management.
    """
    print("[2/7] Generating quotas (source: CRM Quotas)...")

    won_deals = pipeline[pipeline["deal_stage"] == "Won"].copy()
    won_deals["close_date"] = pd.to_datetime(won_deals["close_date"])
    won_deals["month"] = won_deals["close_date"].dt.to_period("M")

    monthly_rev = (
        won_deals.groupby(["sales_agent", "month"])["close_value"]
        .sum()
        .reset_index()
    )
    avg_monthly = (
        monthly_rev.groupby("sales_agent")["close_value"]
        .mean()
        .to_dict()
    )

    months = pd.period_range("2017-01", "2017-12", freq="M")
    rows = []

    for _, agent_row in sales_teams.iterrows():
        agent = agent_row["sales_agent"]
        manager = agent_row["manager"]
        region = agent_row["regional_office"]
        base_performance = avg_monthly.get(agent, 15000)

        for month in months:
            quarter = month.quarter
            seasonal = {1: 0.85, 2: 1.0, 3: 1.05, 4: 1.15}[quarter]
            quota = round(base_performance * 1.15 * seasonal + np.random.normal(0, 2000), 2)
            quota = max(quota, 5000)

            rows.append({
                "quota_id": f"Q-{agent[:3].upper()}-{month}",
                "sales_agent": agent,
                "manager": manager,
                "regional_office": region,
                "quota_month": str(month),
                "quota_amount_usd": round(quota, 2),
                "quota_type": "revenue",
            })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(GEN_PATH, "quotas.csv"), index=False)
    print(f"      -> {len(df)} rows generated")
    return df


def generate_activities(sales_teams, pipeline):
    """
    Simulates: CRM Activity Tracking (Salesforce/HubSpot)
    Every call, email, meeting, demo logged per opportunity.
    """
    print("[3/7] Generating activities (source: CRM Activities)...")

    rows = []
    activity_id = 1

    for _, deal in pipeline.iterrows():
        opp_id = deal["opportunity_id"]
        agent = deal["sales_agent"]
        stage = deal["deal_stage"]

        engage_date = deal.get("engage_date")
        close_date = deal.get("close_date")

        if pd.isna(engage_date):
            engage_date = "2017-06-01"
        start = pd.to_datetime(engage_date)

        if pd.isna(close_date):
            end = start + timedelta(days=np.random.randint(7, 60))
        else:
            end = pd.to_datetime(close_date)

        if end <= start:
            end = start + timedelta(days=1)

        if stage == "Won":
            n_activities = np.random.randint(4, 13)
        elif stage == "Lost":
            n_activities = np.random.randint(2, 7)
        else:
            n_activities = np.random.randint(1, 5)

        type_weights = {
            "email": 0.45,
            "call": 0.30,
            "meeting": 0.15,
            "demo": 0.07,
            "follow_up": 0.03,
        }
        types = list(type_weights.keys())
        probs = list(type_weights.values())

        for _ in range(n_activities):
            days_offset = np.random.randint(0, max((end - start).days, 1))
            activity_date = start + timedelta(days=days_offset)
            activity_type = np.random.choice(types, p=probs)

            duration_map = {
                "email": np.random.randint(2, 15),
                "call": np.random.randint(5, 45),
                "meeting": np.random.randint(15, 90),
                "demo": np.random.randint(30, 120),
                "follow_up": np.random.randint(3, 20),
            }

            rows.append({
                "activity_id": f"ACT-{activity_id:06d}",
                "opportunity_id": opp_id,
                "sales_agent": agent,
                "activity_type": activity_type,
                "activity_date": activity_date.strftime("%Y-%m-%d"),
                "duration_minutes": duration_map[activity_type],
                "outcome": np.random.choice(
                    ["completed", "no_answer", "rescheduled", "cancelled"],
                    p=[0.70, 0.15, 0.10, 0.05],
                ),
            })
            activity_id += 1

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(GEN_PATH, "activities.csv"), index=False)
    print(f"      -> {len(df)} rows generated")
    return df


def generate_costs(products):
    """
    Simulates: ERP system (SAP/Netsuite/Oracle)
    Product cost structure: COGS, shipping, support, commission.
    """
    print("[4/7] Generating costs (source: ERP)...")

    rows = []
    for _, prod in products.iterrows():
        product = prod["product"]
        sales_price = prod["sales_price"]
        series = prod["series"]

        if series == "Alpha":
            cogs_pct = np.random.uniform(0.30, 0.40)
        elif series == "GTX":
            cogs_pct = np.random.uniform(0.40, 0.50)
        else:
            cogs_pct = np.random.uniform(0.50, 0.65)

        cogs = round(sales_price * cogs_pct, 2)
        shipping = round(sales_price * np.random.uniform(0.02, 0.05), 2)
        support_cost = round(sales_price * np.random.uniform(0.03, 0.08), 2)
        commission_pct = round(np.random.uniform(0.05, 0.12), 4)

        rows.append({
            "product": product,
            "series": series,
            "sales_price_usd": sales_price,
            "cogs_usd": cogs,
            "shipping_cost_usd": shipping,
            "support_cost_per_unit_usd": support_cost,
            "sales_commission_pct": commission_pct,
            "gross_margin_pct": round(1 - cogs_pct, 4),
        })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(GEN_PATH, "costs.csv"), index=False)
    print(f"      -> {len(df)} rows generated")
    return df


def generate_nps_surveys(accounts, pipeline):
    """
    Simulates: Survey tool (Delighted/Typeform/Medallia)
    NPS surveys sent quarterly to active accounts.
    Score correlated with deal volume and recency.
    """
    print("[5/7] Generating nps_surveys (source: Delighted/Typeform)...")

    won = pipeline[pipeline["deal_stage"] == "Won"].copy()
    won["close_date"] = pd.to_datetime(won["close_date"])
    acct_deals = won.groupby("account")["opportunity_id"].count().to_dict()

    survey_quarters = [
        ("2017-02-15", "2017-Q1"),
        ("2017-05-15", "2017-Q2"),
        ("2017-08-15", "2017-Q3"),
        ("2017-11-15", "2017-Q4"),
    ]

    positive_comments = [
        "Great product, easy to use",
        "Support team is very responsive",
        "Helped us scale our operations",
        "Good value for the price",
        "Smooth onboarding experience",
        "The product exceeded our expectations",
        "Very satisfied with the service",
        "Would definitely recommend to others",
    ]
    neutral_comments = [
        "It works fine for our needs",
        "Some features could be improved",
        "Average experience overall",
        "Meets basic requirements",
        "Nothing special but gets the job done",
    ]
    negative_comments = [
        "Too expensive for what it offers",
        "Support response times are slow",
        "Missing key features we need",
        "Had issues with implementation",
        "Not intuitive for new users",
        "Considering switching to a competitor",
    ]

    rows = []
    survey_id = 1

    for _, acct in accounts.iterrows():
        account_name = acct["account"]
        deals = acct_deals.get(account_name, 0)

        # Not all accounts respond every quarter
        respond_probability = min(0.85, 0.4 + deals * 0.01)

        for survey_date, quarter in survey_quarters:
            if np.random.random() > respond_probability:
                continue

            # NPS correlated with deal activity
            if deals > 30:
                score = np.random.choice([8, 9, 10], p=[0.3, 0.4, 0.3])
            elif deals > 15:
                score = np.random.choice([6, 7, 8, 9], p=[0.15, 0.35, 0.35, 0.15])
            elif deals > 0:
                score = np.random.choice([4, 5, 6, 7, 8], p=[0.1, 0.2, 0.3, 0.25, 0.15])
            else:
                score = np.random.choice([2, 3, 4, 5, 6], p=[0.15, 0.2, 0.3, 0.2, 0.15])

            # Comment based on score
            if score >= 8:
                comment = np.random.choice(positive_comments)
            elif score >= 6:
                comment = np.random.choice(neutral_comments)
            else:
                comment = np.random.choice(negative_comments)

            # Some surveys have no comment
            if np.random.random() < 0.3:
                comment = None

            # Response time: 0-14 days after survey sent
            response_days = np.random.randint(0, 15)
            response_date = pd.to_datetime(survey_date) + timedelta(days=response_days)

            rows.append({
                "survey_id": f"NPS-{survey_id:05d}",
                "account": account_name,
                "survey_sent_date": survey_date,
                "response_date": response_date.strftime("%Y-%m-%d"),
                "quarter": quarter,
                "nps_score": score,
                "nps_category": "promoter" if score >= 9 else ("passive" if score >= 7 else "detractor"),
                "comment": comment,
                "survey_channel": np.random.choice(
                    ["email", "in_app", "sms"],
                    p=[0.60, 0.30, 0.10],
                ),
            })
            survey_id += 1

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(GEN_PATH, "nps_surveys.csv"), index=False)
    print(f"      -> {len(df)} rows generated")
    return df


def generate_support_tickets(accounts, pipeline):
    """
    Simulates: Support platform (Zendesk/Intercom/Freshdesk)
    Tickets with priority, category, status, and resolution time.
    Accounts with lower satisfaction generate more tickets.
    """
    print("[6/7] Generating support_tickets (source: Zendesk/Intercom)...")

    won = pipeline[pipeline["deal_stage"] == "Won"].copy()
    won["close_date"] = pd.to_datetime(won["close_date"])
    acct_deals = won.groupby("account")["opportunity_id"].count().to_dict()
    acct_revenue = won.groupby("account")["close_value"].sum().to_dict()

    categories = [
        ("billing", 0.25),
        ("technical", 0.30),
        ("onboarding", 0.15),
        ("feature_request", 0.12),
        ("account_management", 0.10),
        ("bug_report", 0.08),
    ]
    cat_names = [c[0] for c in categories]
    cat_probs = [c[1] for c in categories]

    rows = []
    ticket_id = 1

    date_range = pd.date_range("2017-01-01", "2017-12-31")

    for _, acct in accounts.iterrows():
        account_name = acct["account"]
        deals = acct_deals.get(account_name, 0)
        revenue = acct_revenue.get(account_name, 0)

        # More deals = more usage = more tickets (but also better experience)
        # Newer/smaller accounts generate more support tickets per deal
        if deals == 0:
            tickets_per_year = np.random.randint(0, 3)
        elif deals < 10:
            tickets_per_year = np.random.randint(2, 12)
        elif deals < 30:
            tickets_per_year = np.random.randint(5, 20)
        else:
            tickets_per_year = np.random.randint(8, 30)

        for _ in range(tickets_per_year):
            created = pd.Timestamp(np.random.choice(date_range))
            priority = np.random.choice(
                ["low", "medium", "high", "critical"],
                p=[0.30, 0.40, 0.20, 0.10],
            )
            category = np.random.choice(cat_names, p=cat_probs)

            # Resolution time based on priority
            resolution_hours = {
                "critical": np.random.randint(1, 8),
                "high": np.random.randint(4, 48),
                "medium": np.random.randint(12, 120),
                "low": np.random.randint(24, 240),
            }[priority]

            # 90% resolved, 5% open, 5% escalated
            status = np.random.choice(
                ["resolved", "open", "escalated", "waiting_on_customer"],
                p=[0.80, 0.08, 0.05, 0.07],
            )

            resolved_date = None
            if status == "resolved":
                resolved_date = (created + timedelta(hours=resolution_hours)).strftime("%Y-%m-%d")

            # Satisfaction rating (only for resolved tickets)
            satisfaction = None
            if status == "resolved" and np.random.random() < 0.6:
                if resolution_hours < 24:
                    satisfaction = np.random.choice([4, 5], p=[0.3, 0.7])
                elif resolution_hours < 72:
                    satisfaction = np.random.choice([3, 4, 5], p=[0.2, 0.5, 0.3])
                else:
                    satisfaction = np.random.choice([1, 2, 3, 4], p=[0.1, 0.3, 0.4, 0.2])

            rows.append({
                "ticket_id": f"TKT-{ticket_id:06d}",
                "account": account_name,
                "created_date": created.strftime("%Y-%m-%d"),
                "resolved_date": resolved_date,
                "category": category,
                "priority": priority,
                "status": status,
                "resolution_hours": resolution_hours if status == "resolved" else None,
                "satisfaction_rating": satisfaction,
                "agent_name": fake.name(),
            })
            ticket_id += 1

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(GEN_PATH, "support_tickets.csv"), index=False)
    print(f"      -> {len(df)} rows generated")
    return df


def print_summary(quotas, activities, costs, nps, tickets):
    """Print a summary of all generated data."""
    print("\n[7/7] Summary")
    print("=" * 60)
    print(f"  quotas.csv:           {len(quotas):>8,} rows  (CRM Quotas)")
    print(f"  activities.csv:       {len(activities):>8,} rows  (CRM Activities)")
    print(f"  costs.csv:            {len(costs):>8,} rows  (ERP Costs)")
    print(f"  nps_surveys.csv:      {len(nps):>8,} rows  (Delighted/Typeform)")
    print(f"  support_tickets.csv:  {len(tickets):>8,} rows  (Zendesk/Intercom)")
    print("=" * 60)
    total = len(quotas) + len(activities) + len(costs) + len(nps) + len(tickets)
    print(f"  Total generated:      {total:>8,} rows")
    print(f"  Output folder:        {os.path.abspath(GEN_PATH)}")
    print("\n  customer_health will be BUILT in dbt (not generated here)")
    print("  Ready for BigQuery load!")


def main():
    print("=" * 60)
    print("  RevOps Analytics Hub — Synthetic Data Generation")
    print("=" * 60)

    accounts, products, sales_teams, pipeline = load_raw_data()
    quotas = generate_quotas(sales_teams, pipeline)
    activities = generate_activities(sales_teams, pipeline)
    costs = generate_costs(products)
    nps = generate_nps_surveys(accounts, pipeline)
    tickets = generate_support_tickets(accounts, pipeline)
    print_summary(quotas, activities, costs, nps, tickets)


if __name__ == "__main__":
    main()
