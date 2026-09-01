{{
    config(
        materialized='table'
    )
}}

select distinct
    date,
    extract(year from date) as annee,
    extract(quarter from date) as trimestre,
    extract(month from date) as mois,
    extract(week from date) as semaine,
    extract(day from date) as jour,
    extract(dayofweek from date) as jour_semaine

from {{ ref('stg_openmeteo') }}