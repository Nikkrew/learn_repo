{%- set property_types = dbt_utils.get_column_values(table= ref('stg_airbnb_listings'), column='property_type') -%}


with listings as (
    select * from {{ref('stg_airbnb_listings')}}

),

pivoted as (

    SELECT
    neighbourhood,

    {%- for property_type in property_types %}

    {%- set property_type_cleaned = property_type | lower | replace(' ','_') | replace ('/','_') %}

    sum(case when porperty_type = '{{property_type}}' then 1 else 0 end) as {{property_type_cleaned}}_count{{-',' if not loop.last -}}

      {%- if not loop.last -%} , {%- endif -%}

    {% endfor %}
    from listings
    group by 1

  )

select * from pivoted
