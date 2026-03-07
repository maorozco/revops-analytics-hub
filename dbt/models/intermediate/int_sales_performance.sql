with deals as (
    select * from {{ ref('int_deal_economics') }}
),

quotas as (
    select * from {{ ref('stg_quotas') }}
),

activities as (
    select * from {{ ref('stg_activities') }}
),

-- Revenue per agent per month (from won deals)
agent_revenue as (
    select
        agent_name,
        format_date('%Y-%m', close_date)    as revenue_month,
        count(*)                            as deals_won,
        sum(close_value_usd)                as total_revenue_usd,
        sum(net_profit_usd)                 as total_profit_usd,
        avg(days_in_pipeline)               as avg_days_in_pipeline
    from deals
    where is_won
    group by 1, 2
),

-- Activity effort per agent per month
agent_effort as (
    select
        agent_name,
        format_date('%Y-%m', activity_date) as activity_month,
        count(*)                            as total_activities,
        sum(duration_minutes)               as total_minutes,
        countif(outcome = 'positive')       as positive_outcomes
    from activities
    group by 1, 2
),

-- Join revenue + quota + effort
joined as (
    select
        coalesce(r.agent_name, q.agent_name, e.agent_name)  as agent_name,
        coalesce(r.revenue_month, q.quota_month, e.activity_month) as period_month,
        q.manager_name,
        q.regional_office,
        q.quota_amount_usd,

        coalesce(r.deals_won, 0)            as deals_won,
        coalesce(r.total_revenue_usd, 0)    as total_revenue_usd,
        coalesce(r.total_profit_usd, 0)     as total_profit_usd,
        coalesce(r.avg_days_in_pipeline, 0) as avg_days_in_pipeline,

        coalesce(e.total_activities, 0)     as total_activities,
        coalesce(e.total_minutes, 0)        as total_effort_minutes,
        coalesce(e.positive_outcomes, 0)    as positive_outcomes,

        -- Quota attainment
        case
            when q.quota_amount_usd > 0
            then round(coalesce(r.total_revenue_usd, 0) / q.quota_amount_usd * 100, 1)
            else null
        end as quota_attainment_pct

    from agent_revenue r
    full outer join quotas q
        on r.agent_name = q.agent_name
        and r.revenue_month = q.quota_month
    full outer join agent_effort e
        on coalesce(r.agent_name, q.agent_name) = e.agent_name
        and coalesce(r.revenue_month, q.quota_month) = e.activity_month
)

select * from joined
