with source as (
    select * from {{ source('raw', 'nps_surveys') }}
),

renamed as (
    select
        survey_id,
        account                             as account_name,
        survey_sent_date,
        response_date,
        quarter                             as survey_quarter,
        nps_score,
        nps_category,
        comment                             as survey_comment,
        survey_channel
    from source
)

select * from renamed
