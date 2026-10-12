-- Un seul jeu de seuils par station et mois, quelle que soit l'année observée.
select code_station, extract(month from date_obs_elab) as mois
from {{ ref('fct_observation_hydro') }}
group by code_station, mois
having count(distinct p90) > 1
    or count(distinct p95) > 1
    or count(distinct p99) > 1
    or count(distinct n_reference) > 1
    or count(distinct n_annees_reference) > 1
    or count(p95) not in (0, count(*))
