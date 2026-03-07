with accounts as (
    select * from {{ ref('stg_accounts') }}
),

nps as (
    select * from {{ ref('stg_nps_surveys') }}
),

tickets as (
    select * from {{ ref('stg_support_tickets') }}
),

deals as (
    select * from {{ ref('int_deal_economics') }}
),

-- Latest NPS per account
latest_nps as (
    select
        account_name,
        nps_score                           as latest_nps_score,
        nps_category                        as latest_nps_category,
        row_number() over (
            partition by account_name
            order by response_date desc
        ) as rn
    from nps
    where response_date is not null
),

-- NPS averages per account
nps_summary as (
    select
        account_name,
        avg(nps_score)                      as avg_nps_score,
        count(*)                            as total_surveys
    from nps
    group by 1
),

-- Support ticket metrics per account
ticket_summary as (
    select
        account_name,
        count(*)                            as total_tickets,
        countif(ticket_priority = 'High')   as high_priority_tickets,
        avg(resolution_hours)               as avg_resolution_hours,
        avg(satisfaction_rating)            as avg_satisfaction_rating
    from tickets
    group by 1
),

-- Revenue per account
revenue_summary as (
    select
        account_name,
        count(*)                            as total_deals,
        countif(is_won)                     as deals_won,
        sum(case when is_won then close_value_usd else 0 end) as total_revenue_usd,
        max(close_date)                     as last_deal_date
    from deals
    group by 1
),

-- Combine into health score
joined as (
    select
        a.account_name,
        a.sector,
        a.annual_revenue_usd,
        a.employee_count,

        ln.latest_nps_score,
        ln.latest_nps_category,
        ns.avg_nps_score,
        ns.total_surveys,

        coalesce(ts.total_tickets, 0)       as total_tickets,
        coalesce(ts.high_priority_tickets, 0) as high_priority_tickets,
        ts.avg_resolution_hours,
        ts.avg_satisfaction_rating,

        coalesce(rs.total_deals, 0)         as total_deals,
        coalesce(rs.deals_won, 0)           as deals_won,
        coalesce(rs.total_revenue_usd, 0)   as total_revenue_usd,
        rs.last_deal_date,

        -- Health score (0-100): weighted NPS + satisfaction - ticket severity
        round(
            coalesce(ns.avg_nps_score * 10, 50)  * 0.4   -- NPS component (40%)
            + coalesce(ts.avg_satisfaction_rating * 20, 50) * 0.3  -- CSAT component (30%)
            - coalesce(ts.high_priority_tickets * 5, 0)   -- Penalty for high-priority tickets
            + case
                when rs.deals_won > 0 then 15
                else 0
              end                                          -- Bonus for active buyer (30% base)
        , 1) as health_score,

        -- Churn risk based on health signals
        case
            when coalesce(ns.avg_nps_score, 5) <= 3
                 and coalesce(ts.high_priority_tickets, 0) >= 2
                then 'High'
            when coalesce(ns.avg_nps_score, 5) <= 5
                 or coalesce(ts.avg_satisfaction_rating, 3) < 3
                then 'Medium'
            else 'Low'
        end as churn_risk

    from accounts a
    left join latest_nps ln
        on a.account_name = ln.account_name and ln.rn = 1
    left join nps_summary ns
        on a.account_name = ns.account_name
    left join ticket_summary ts
        on a.account_name = ts.account_name
    left join revenue_summary rs
        on a.account_name = rs.account_name
)

select * from joined
