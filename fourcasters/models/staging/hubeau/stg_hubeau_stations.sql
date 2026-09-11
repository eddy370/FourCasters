with source as (

    select *
    from {{ source('hubeau_raw', 'stations_hydrometriques') }}

),

final as (

    select

        trim(code_station) as code_station,
        trim(code_site) as code_site,
        trim(libelle_station) as libelle_station,

        -- Même format que dim_commune.code_insee (lpad 5 caractères),
        -- pour permettre la future jointure dim_station <-> dim_commune.
        lpad(
            trim(cast(code_commune_station as string)),
            5,
            '0'
        ) as code_commune_station,

        trim(libelle_commune) as libelle_commune,
        trim(code_departement) as code_departement,
        trim(libelle_departement) as libelle_departement,
        trim(code_region) as code_region,
        trim(libelle_region) as libelle_region,
        trim(code_cours_eau) as code_cours_eau,
        trim(libelle_cours_eau) as libelle_cours_eau,

        safe_cast(latitude_station as float64) as latitude_station,
        safe_cast(longitude_station as float64) as longitude_station,

        safe_cast(date_ouverture_station as timestamp) as date_ouverture_station,
        safe_cast(date_fermeture_station as timestamp) as date_fermeture_station,

        en_service

    from source

)

select *
from final
