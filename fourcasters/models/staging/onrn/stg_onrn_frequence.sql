with source as (

    select *
    from {{ source('onrn_raw', 'onrn_frequence_inondation') }}

),

final as (

    select

        trim(code_insee) as code_insee,
        trim(commune) as commune,
        trim(frequence) as classe_frequence

    from source

    -- Exclut les lignes entièrement NULL et la ligne d'en-tête
    -- importée comme donnée dans la table RAW.
    where code_insee is not null
      and trim(code_insee) != ''
      and trim(code_insee) != 'Code_INSEE'

)

select *
from final
