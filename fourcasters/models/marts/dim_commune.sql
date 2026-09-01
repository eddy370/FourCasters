{{
    config(
        materialized='table'
    )
}}

select distinct

    lpad(
        trim(cast(code_INSEE as string)),
        5,
        '0'
    ) as code_insee,

    Commune as ville,

    lpad(
        trim(cast(Numero_Departement as string)),
        2,
        '0'
    ) as departement,

    Region as region,
    Latitude as latitude,
    Longitude as longitude

from {{ ref('referentiel_geographique') }}