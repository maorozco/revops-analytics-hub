with source as (
    select * from {{ source('raw', 'activities') }}
),

renamed as (
    select
        activity_id,
        opportunity_id,
        sales_agent                         as agent_name,
        activity_type,
        activity_date,
        duration_minutes,
        outcome
    from source
)

select * from renamed
