with source as (
    select * from {{ source('raw', 'accounts') }}
),

renamed as (
    select
        account                             as account_name,
        sector,
        year_established,
        revenue                             as annual_revenue_usd,
        employees                           as employee_count,
        office_location,
        subsidiary_of
    from source
)

select * from renamed
