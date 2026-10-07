-- La dernière date doit rester disponible pour l'inférence (predict.py) :
-- target_j1 y est NULL par construction, mais les features doivent être
-- renseignées. Renvoie la dernière date si aucune ligne n'est exploitable.
with derniere_date as (

    select max(date) as date
    from {{ ref('ml_features') }}

)

select d.date

from derniere_date d

left join {{ ref('ml_features') }} f
    on f.date = d.date
    and f.position_p95 is not null
    and f.variation_position_1j is not null
    and f.pluie_j is not null
    and f.pluie_3j is not null
    and f.humidite_sol is not null

group by d.date

having count(f.code_station) = 0
