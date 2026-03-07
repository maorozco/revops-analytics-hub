with source as (
    select * from {{ source('raw', 'quotas') }}
),

renamed as (
    select
        quota_id,
        sales_agent                         as agent_name,
        manager                             as manager_name,
        regional_office,
        quota_month,
        quota_amount_usd,
        quota_type
    from source
)

select * from renamed
