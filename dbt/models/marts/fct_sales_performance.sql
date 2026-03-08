with performance as (
    select * from {{ ref('int_sales_performance') }}
)

select
    agent_name,
    period_month,
    manager_name,
    regional_office,
    quota_amount_usd,
    deals_won,
    total_revenue_usd,
    total_profit_usd,
    quota_attainment_pct,
    avg_days_in_pipeline,
    total_activities,
    total_effort_minutes,
    positive_outcomes,

    -- Performance tier
    case
        when quota_attainment_pct >= 120 then 'Star Performer'
        when quota_attainment_pct >= 100 then 'On Target'
        when quota_attainment_pct >= 80  then 'Needs Improvement'
        when quota_attainment_pct is not null then 'Underperforming'
        else 'No Quota'
    end as performance_tier

from performance
