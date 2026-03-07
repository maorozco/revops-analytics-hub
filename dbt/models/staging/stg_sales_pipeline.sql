with source as (
    select * from {{ source('raw', 'sales_pipeline') }}
),

renamed as (
    select
        opportunity_id,
        sales_agent                         as agent_name,
        product                             as product_name,
        account                             as account_name,
        deal_stage,
        engage_date,
        close_date,
        close_value                         as close_value_usd
    from source
)

select * from renamed
