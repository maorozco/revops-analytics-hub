with source as (
    select * from {{ source('raw', 'sales_teams') }}
),

renamed as (
    select
        sales_agent                         as agent_name,
        manager                             as manager_name,
        regional_office
    from source
)

select * from renamed
