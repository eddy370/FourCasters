-- Une seule ligne par station et par date dans ml_features.
select
    code_station,
    date,
    count(*) as nb_lignes

from {{ ref('ml_features') }}

group by
    code_station,
    date

having count(*) > 1
