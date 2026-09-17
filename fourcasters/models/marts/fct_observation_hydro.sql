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

),

seuils as (

    select
        code_station,
        extract(month from date_obs_elab) as mois,
        approx_quantiles(resultat_obs_elab, 100)[offset(90)] as p90,
        approx_quantiles(resultat_obs_elab, 100)[offset(95)] as p95,
        approx_quantiles(resultat_obs_elab, 100)[offset(99)] as p99

    from obs

    where code_qualification = '20'
        and code_statut in ('12', '16')
        and resultat_obs_elab is not null

    group by
        code_station,
        mois

),

final as (

    select
        obs.row_hash,
        obs.code_station,
        obs.date_obs_elab,
        obs.grandeur_hydro_elab,
        obs.resultat_obs_elab,
        obs.date_prod,
        obs.code_statut,
        obs.libelle_statut,
        obs.code_methode,
        obs.libelle_methode,
        obs.code_qualification,
        obs.libelle_qualification,

        seuils.p90,
        seuils.p95,
        seuils.p99,

        case
            when seuils.p99 > seuils.p95
                and obs.resultat_obs_elab >= seuils.p99
                then 'Exceptionnel'

            when seuils.p95 > seuils.p90
                and obs.resultat_obs_elab >= seuils.p95
                then 'Élevé'

            else 'Normal'
        end as niveau_hydrologique

    from obs

    left join seuils
        on obs.code_station = seuils.code_station
        and extract(month from obs.date_obs_elab) = seuils.mois

)

select *
from final

{% if is_incremental() %}

where date_obs_elab >= date_sub(
    (
        select max(date_obs_elab)
        from {{ this }}
    ),
    interval 35 day
)

{% endif %}