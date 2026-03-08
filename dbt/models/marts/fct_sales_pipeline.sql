with deals as (
    select * from {{ ref('int_deal_economics') }}
)

select
    opportunity_id,
    agent_name,
    product_name,
    account_name,
    deal_stage,
    engage_date,
    close_date,
    close_value_usd,
    cogs_usd,
    net_profit_usd,
    days_in_pipeline,
    is_won,
    is_lost,
    gross_margin_pct

from deals
