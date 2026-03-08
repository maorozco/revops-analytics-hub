with customer_health as (
    select * from {{ ref('int_customer_health') }}
)

select
    account_name,
    sector,
    annual_revenue_usd,
    employee_count,

    -- Account size tier
    case
        when annual_revenue_usd >= 500000000 then 'Enterprise'
        when annual_revenue_usd >= 100000000 then 'Mid-Market'
        else 'SMB'
    end as account_tier,

    -- Health metrics
    latest_nps_score,
    latest_nps_category,
    avg_nps_score,
    total_tickets,
    high_priority_tickets,
    avg_satisfaction_rating,
    health_score,
    churn_risk,

    -- Revenue metrics
    total_deals,
    deals_won,
    total_revenue_usd as lifetime_revenue_usd

from customer_health
