with teams as (
    select * from {{ ref('stg_sales_teams') }}
)

select
    agent_name,
    manager_name,
    regional_office

from teams
