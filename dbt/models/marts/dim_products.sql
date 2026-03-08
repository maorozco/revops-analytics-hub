with products as (
    select * from {{ ref('stg_products') }}
),

costs as (
    select * from {{ ref('stg_costs') }}
)

select
    p.product_name,
    p.product_series,
    p.list_price_usd,
    c.cogs_usd,
    c.shipping_cost_usd,
    c.support_cost_per_unit_usd,
    c.sales_commission_pct,
    c.gross_margin_pct,

    -- Profitability tier
    case
        when c.gross_margin_pct >= 40 then 'High Margin'
        when c.gross_margin_pct >= 25 then 'Medium Margin'
        else 'Low Margin'
    end as margin_tier

from products p
left join costs c
    on p.product_name = c.product_name
