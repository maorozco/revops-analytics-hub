with source as (
    select * from {{ source('raw', 'costs') }}
),

renamed as (
    select
        product                             as product_name,
        series                              as product_series,
        sales_price_usd                     as list_price_usd,
        cogs_usd,
        shipping_cost_usd,
        support_cost_per_unit_usd,
        sales_commission_pct,
        gross_margin_pct
    from source
)

select * from renamed
