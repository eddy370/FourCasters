{{
    config(
        materialized='incremental',
        unique_key='row_hash',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["code_insee"]
    )
}}

with meteo as (

    select *
    from {{ ref('stg_openmeteo') }}

)

select
    row_hash,
    ville,
    code_insee,
    departement,
    region,
    latitude,
    longitude,
    date,
    code_meteo,

    temperature_moyenne,
    temperature_minimale,
    temperature_maximale,

    temperature_ressentie_moyenne,
    temperature_ressentie_minimale,
    temperature_ressentie_maximale,

    humidite_moyenne,
    humidite_minimale,
    humidite_maximale,
    point_de_rosee_moyen,

    precipitations_totales,
    pluie_totale,
    neige_totale,
    heures_de_precipitations,

    vitesse_vent_moyenne,
    vitesse_vent_maximale,
    rafale_vent_maximale,
    direction_vent_dominante,

    couverture_nuageuse_moyenne,
    pression_moyenne,
    duree_ensoleillement,
    rayonnement_solaire_total,
    evapotranspiration,
    deficit_pression_vapeur_maximal,

    humidite_sol_0_7cm,
    humidite_sol_7_28cm,
    humidite_sol_28_100cm,
    temperature_sol_0_7cm

from meteo

{% if is_incremental() %}

where date > (
    select max(date)
    from {{ this }}
)

{% endif %}