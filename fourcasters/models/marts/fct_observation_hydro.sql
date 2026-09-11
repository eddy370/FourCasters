{{
    config(
        materialized='incremental',
        unique_key='row_hash',
        partition_by={
            "field": "date_obs_elab",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["code_station"]
    )
}}

with obs as (

    select *
    from {{ ref('stg_hubeau_obs_elab') }}

)

select
    row_hash,
    code_station,
    date_obs_elab,
    grandeur_hydro_elab,
    resultat_obs_elab,
    date_prod,
    code_statut,
    libelle_statut,
    code_methode,
    libelle_methode,
    code_qualification,
    libelle_qualification

from obs

{% if is_incremental() %}

where date_obs_elab >= date_sub(
    (
        select max(date_obs_elab)
        from {{ this }}
    ),
    interval 35 day
)

{% endif %}
