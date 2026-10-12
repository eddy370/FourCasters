-- Les observations utilisées par le ML respectent la règle hydro du dashboard.
select f.code_station, f.date
from {{ ref('ml_features') }} f
left join {{ ref('fct_observation_hydro') }} o
    on f.code_station = o.code_station
    and f.date = o.date_obs_elab
    and o.grandeur_hydro_elab = 'HIXnJ'
where o.niveau_hydrologique is null
