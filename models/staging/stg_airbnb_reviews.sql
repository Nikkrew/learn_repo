with source as (

    select * from {{ source('airbnb', 'reviews') }}

),

renamed as (

    select

        id as review_id,
        review,
        listing_id,
        reviewer_name,
        comments,
        date as review_date

    from source

)

select * from renamed
