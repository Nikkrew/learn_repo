with source as (

    select * from {{ source('airbnb', 'listings') }}

),

renamed as (

    select

        id as listing_id,
        url,

--listing info
        name,
        summary,
        space,
        description,
        experiences_offered,
        neighborhood_overview,
        notes,
        transit,
        access,
        interaction,
        house_rules,
        thumbnail_url,
        property_type,
        room_type,
        accommodates,
        bathrooms,
        bedrooms,
        beds,
        bed_type,
        amenities,
        square_feet,
        guests_included,
        extra_people,
        calendar_updated,
        has_availability,
        availability_30,
        availability_60,
        availability_90,
        availability_365,
        calendar_last_scraped,
        medium_url,
        picture_url,
        xl_picture_url,

    --host info
        host_id,
        host_url,
        host_name,
        host_since,
        host_location,
        host_about,
        response_time,
        response_rate,
        acceptance_rate,
        is_superhost,
        host_thumbnail_url,
        host_picture_url,
        host_neighbourhood,
        host_verifications,
        host_has_profile_pic,
        host_identity_verified,

--geography
        street,
        neighbourhood,
        neighbourhood_cleansed,
        neighbourhood_group_cleansed,
        city,
        state,
        zipcode,
        market,
        smart_location,
        country_code,
        country,
        latitude,
        longitude,
        is_location_exact,

--requirements
        requires_license,
        license,
        jurisdiction_names,
        instant_bookable,
        is_business_travel_ready,
        cancellation_policy,
        require_guest_profile_picture,
        require_guest_phone_verification,

--pricing
        cleaning_fee,
        security_deposit,
        nullif(replace(split_part(monthly_price, '$', 2), ',', ''), '') as monthly_price,
        nullif(replace(split_part(weekly_price, '$', 2), ',', ''), '') as weekly_price,
        nullif(replace(split_part(price, '$', 2), ',', ''), '') as price

    from source

)

select * from renamed
