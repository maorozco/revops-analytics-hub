with source as (
    select * from {{ source('raw', 'support_tickets') }}
),

renamed as (
    select
        ticket_id,
        account                             as account_name,
        created_date,
        resolved_date,
        category                            as ticket_category,
        priority                            as ticket_priority,
        status                              as ticket_status,
        resolution_hours,
        satisfaction_rating,
        agent_name                          as support_agent_name
    from source
)

select * from renamed
