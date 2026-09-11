{{ config(materialized='table') }}

with stations as (

    select *
    from {{ ref('stg_hubeau_stations') }}

),

stations_observees as (

    select distinct code_station
    from {{ ref('stg_hubeau_obs_elab') }}

),

final as (

    select
        s.code_station,
        s.code_site,
        s.libelle_station,
        s.code_commune_station,
        s.libelle_commune,
        s.code_departement,
        s.libelle_departement,
        s.code_region,
        s.libelle_region,
        s.code_cours_eau,
        s.libelle_cours_eau,
        s.latitude_station,
        s.longitude_station,
        s.date_ouverture_station,
        s.date_fermeture_station,
        s.en_service

    from stations s

    inner join stations_observees o
        on s.code_station = o.code_station

)

select *
from final
