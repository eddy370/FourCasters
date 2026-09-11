with source as (

    select *
    from {{ source('hubeau_raw', 'observations_hydrometriques_elaborees') }}

),

derniere_version as (

    select *
    from source

    qualify row_number() over (
        partition by
            trim(code_station),
            safe_cast(date_obs_elab as date),
            trim(grandeur_hydro_elab)
        order by
            inserted_at desc,
            date_prod desc,
            row_hash desc
    ) = 1

),

final as (

    select

        -- row_hash d'IDENTITE : hash de la clé métier
        -- (code_station + date_obs_elab + grandeur_hydro_elab).
        -- A ne pas confondre avec le row_hash de la table RAW,
        -- qui est un hash de CONTENU utilisé côté ingestion Python
        -- pour détecter les changements de contenu.
        to_hex(
            md5(
                concat(
                    trim(code_station),
                    '|',
                    cast(date_obs_elab as string),
                    '|',
                    trim(grandeur_hydro_elab)
                )
            )
        ) as row_hash,

        trim(code_station) as code_station,
        safe_cast(date_obs_elab as date) as date_obs_elab,
        trim(grandeur_hydro_elab) as grandeur_hydro_elab,

        safe_cast(resultat_obs_elab as float64) as resultat_obs_elab,
        safe_cast(date_prod as timestamp) as date_prod,

        trim(code_statut) as code_statut,
        trim(libelle_statut) as libelle_statut,
        trim(code_methode) as code_methode,
        trim(libelle_methode) as libelle_methode,
        trim(code_qualification) as code_qualification,
        trim(libelle_qualification) as libelle_qualification,

        trim(code_site) as code_site,
        safe_cast(longitude as float64) as longitude,
        safe_cast(latitude as float64) as latitude,

        inserted_at

    from derniere_version

)

select *
from final