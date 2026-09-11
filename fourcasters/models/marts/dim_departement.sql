{{
    config(
        materialized='table'
    )
}}

select distinct

    case
        when upper(trim(cast(Numero_Departement as string))) in ('2A', '2B')
            then upper(trim(cast(Numero_Departement as string)))
        else lpad(trim(cast(Numero_Departement as string)), 2, '0')
    end as code_departement,

    trim(Departement) as departement,
    trim(Region) as region

from {{ ref('referentiel_geographique') }}