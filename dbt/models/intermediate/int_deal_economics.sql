with pipeline as (
    select * from {{ ref('stg_sales_pipeline') }}
),

costs as (
    select * from {{ ref('stg_costs') }}
),

joined as (
    select
        p.opportunity_id,
        p.agent_name,
        p.product_name,
        p.account_name,
        p.deal_stage,
        p.engage_date,
        p.close_date,
        p.close_value_usd,

        c.cogs_usd,
        c.shipping_cost_usd,
        c.support_cost_per_unit_usd,
        c.sales_commission_pct,
        c.gross_margin_pct,

        -- Unit economics per deal
        round(p.close_value_usd - c.cogs_usd - c.shipping_cost_usd
              - c.support_cost_per_unit_usd
              - (p.close_value_usd * c.sales_commission_pct), 2)
            as net_profit_usd,

        -- Days in pipeline
        date_diff(p.close_date, p.engage_date, day) as days_in_pipeline,

        -- Deal outcome flags
        case
            when p.deal_stage = 'Won' then true
            else false
        end as is_won,

        case
            when p.deal_stage = 'Lost' then true
            else false
        end as is_lost

    from pipeline p
    left join costs c
        on p.product_name = c.product_name
)

select * from joined
