with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        product                             as product_name,
        series                              as product_series,
        sales_price                         as list_price_usd
    from source
)

select * from renamed
