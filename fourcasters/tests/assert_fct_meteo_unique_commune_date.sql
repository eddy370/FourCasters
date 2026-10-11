-- Le hash technique ne suffit pas à garantir le grain commune × date.
-- Toute date dupliquée est signalée, même avec deux hashes différents.
select
    code_insee,
    date,
    count(*) as nb_lignes

from {{ ref('fct_meteo_journaliere') }}

group by code_insee, date

having count(*) > 1
