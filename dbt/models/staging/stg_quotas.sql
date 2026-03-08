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
        quota_type,
        row_number() over (partition by quota_id order by quota_amount_usd desc) as rn
    from source
)

-- Deduplicate: keep highest quota when quota_id is duplicated
select * except(rn) from renamed where rn = 1
