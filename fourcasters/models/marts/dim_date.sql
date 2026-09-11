{{
    config(
        materialized='table'
    )
}}

with dates as (

    select date
    from unnest(
        generate_date_array(date('2000-01-01'), current_date(), interval 1 day)
    ) as date

)

select
    date,
    extract(year from date) as annee,
    extract(quarter from date) as trimestre,
    extract(month from date) as mois,
    extract(week from date) as semaine,
    extract(day from date) as jour,
    extract(dayofweek from date) as jour_semaine

from dates
