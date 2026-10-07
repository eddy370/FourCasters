{{ config(materialized='table') }}

{#-
    Features du modèle ML de prévision de dépassement du P95 à J+1
    (consommées par predict.py, une ligne par station et par jour).

    Reconstruit à partir des valeurs de marts.ml_features (30/09/2026),
    la requête d'origine n'ayant jamais été versionnée :

    - position_p95          : (résultat - p95) / (p99 - p90), seuils mensuels
                              par station issus de fct_observation_hydro
    - variation_position_1j : position(j) - position(j-1), NULL si j-1 absent
    - pluie_j / pluie_3j    : pluie_totale de la commune de la station,
                              jour j et cumul j-2..j
    - humidite_sol          : humidite_sol_7_28cm de la commune
    - périmètre             : stations rattachées à une commune de dim_commune
                              avec au moins 100 jours exploitables ; jours
                              sans pluie/humidité Open-Meteo écartés
    - depasse_p95_j         : résultat >= p95
    - target_j1             : depasse_p95_j de j+1, NULL si j+1 absent.
                              Cette cible est NULL pour la dernière date :
                              la ligne est conservée volontairement, c'est
                              celle que predict.py utilise pour l'inférence.

    Aucun filtre de date : la table va jusqu'à la dernière date consolidée
    (le recul de RECUL_JOURS jours est appliqué à l'ingestion). Ne pas
    filtrer sur target_j1 IS NOT NULL ici : l'entraînement doit le faire
    lui-même, sinon la dernière date disparaît et predict.py prédit la veille.
-#}

with stations as (

    select
        s.code_station,
        s.code_commune_station as code_insee,
        s.code_departement

    from {{ ref('dim_station') }} s

    inner join {{ ref('dim_commune') }} c
        on s.code_commune_station = c.code_insee

),

meteo as (

    select
        code_insee,
        date,
        pluie_totale as pluie_j,
        -- NULL si un des jours de la fenêtre j-2..j n'a pas de pluie
        -- (trou amont Open-Meteo) : pas de cumul partiel silencieux.
        case
            when count(pluie_totale) over w = count(*) over w
                then sum(pluie_totale) over w
        end as pluie_3j,
        humidite_sol_7_28cm as humidite_sol

    from {{ ref('fct_meteo_journaliere') }}

    window w as (
        partition by code_insee
        order by unix_date(date)
        range between 2 preceding and current row
    )

),

position as (

    select
        o.code_station,
        o.date_obs_elab as date,
        safe_divide(o.resultat_obs_elab - o.p95, o.p99 - o.p90) as position_p95,
        cast(o.resultat_obs_elab >= o.p95 as int64) as depasse_p95_j

    from {{ ref('fct_observation_hydro') }} o

    where o.resultat_obs_elab is not null
        and o.p95 is not null

),

-- Variation et cible calculées sur la série hydro seule, avant la jointure
-- météo : un jour sans météo ne doit pas annuler la variation du lendemain
-- ni la cible de la veille.
hydro as (

    select
        code_station,
        date,
        position_p95,

        case
            when date_diff(date, lag(date) over w, day) = 1
                then position_p95 - lag(position_p95) over w
        end as variation_position_1j,

        depasse_p95_j,

        case
            when date_diff(lead(date) over w, date, day) = 1
                then lead(depasse_p95_j) over w
        end as target_j1

    from position

    where position_p95 is not null

    window w as (partition by code_station order by date)

),

joint as (

    select
        h.date,
        h.code_station,
        s.code_insee,
        s.code_departement,
        h.position_p95,
        h.variation_position_1j,
        m.pluie_j,
        m.pluie_3j,
        m.humidite_sol,
        extract(month from h.date) as mois,
        h.depasse_p95_j,
        h.target_j1

    from hydro h

    inner join stations s
        on h.code_station = s.code_station

    inner join meteo m
        on s.code_insee = m.code_insee
        and h.date = m.date

    where m.pluie_j is not null
        and m.pluie_3j is not null
        and m.humidite_sol is not null

),

joint_historique as (

    -- Stations avec au moins 100 jours exploitables : écarte les stations
    -- récentes dont les seuils P90/P95/P99 ne sont pas encore fiables.
    select *
    from joint

    qualify count(*) over (partition by code_station) >= 100

),

final as (

    select
        date,
        code_station,
        code_insee,
        code_departement,
        position_p95,
        variation_position_1j,
        pluie_j,
        pluie_3j,
        humidite_sol,
        mois,
        depasse_p95_j,
        target_j1

    from joint_historique

)

select *
from final
