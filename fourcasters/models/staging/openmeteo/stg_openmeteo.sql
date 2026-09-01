with source_historique as ( 
 
    select * 
    from {{ source('openmeteo_raw', 'open_meteo_corrigé') }} 
 
), 
 
source_quotidienne as ( 
 
    select * 
    from {{ source('openmeteo_raw', 'meteo_journaliere_raw') }} 
 
), 
 
geo as ( 
 
    select * 
    from {{ ref('referentiel_geographique') }} 
 
), 
 
historique_cleaned as ( 
 
    select 
 
        -- clé temporelle 
        safe_cast(m.time as date) as date, 
 
        -- géographie harmonisée 
        coalesce(m.nom_poi, m.Ville) as ville, 
 
        -- code INSEE enrichi depuis le référentiel 
        lpad( 
            trim(cast(g.code_INSEE as string)), 
            5, 
            '0' 
        ) as code_insee, 
 
        coalesce( 
            m.numero_departement, 
            lpad( 
                cast(cast(m.Departement as int64) as string), 
                2, 
                '0' 
            ) 
        ) as departement, 
 
        g.Region as region, 
 
        coalesce(m.latitude_poi, m.Latitude) as latitude, 
        coalesce(m.longitude_poi, m.Longitude) as longitude, 
 
        -- météo 
        safe_cast(m.weather_code as int64) as code_meteo, 
 
        -- températures 
        m.temperature_2m_mean as temperature_moyenne, 
        m.temperature_2m_min as temperature_minimale, 
        m.temperature_2m_max as temperature_maximale, 
 
        m.apparent_temperature_mean as temperature_ressentie_moyenne, 
        m.apparent_temperature_min as temperature_ressentie_minimale, 
        m.apparent_temperature_max as temperature_ressentie_maximale, 
 
        -- humidité 
        m.relative_humidity_2m_mean as humidite_moyenne, 
        m.relative_humidity_2m_min as humidite_minimale, 
        m.relative_humidity_2m_max as humidite_maximale, 
        m.dew_point_2m_mean as point_de_rosee_moyen, 
 
        -- précipitations 
        m.precipitation_sum as precipitations_totales, 
        m.rain_sum as pluie_totale, 
        m.snowfall_sum as neige_totale, 
        m.precipitation_hours as heures_de_precipitations, 
 
        -- vent 
        m.wind_speed_10m_mean as vitesse_vent_moyenne, 
        m.wind_speed_10m_max as vitesse_vent_maximale, 
        m.wind_gusts_10m_max as rafale_vent_maximale, 
        m.wind_direction_10m_dominant as direction_vent_dominante, 
 
        -- autres variables météo 
        m.cloud_cover_mean as couverture_nuageuse_moyenne, 
        m.pressure_msl_mean as pression_moyenne, 
        m.sunshine_duration as duree_ensoleillement, 
        m.shortwave_radiation_sum as rayonnement_solaire_total, 
        m.et0_fao_evapotranspiration as evapotranspiration, 
        m.vapour_pressure_deficit_max as deficit_pression_vapeur_maximal, 
 
        -- sol 
        m.soil_moisture_0_to_7cm_mean as humidite_sol_0_7cm, 
        m.soil_moisture_7_to_28cm_mean as humidite_sol_7_28cm, 
        m.soil_moisture_28_to_100cm_mean as humidite_sol_28_100cm, 
        m.soil_temperature_0_to_7cm_mean as temperature_sol_0_7cm 
 
    from source_historique m 
 
    left join geo g 
 
        on regexp_replace( 
            lower( 
                regexp_replace( 
                    normalize(coalesce(m.nom_poi, m.Ville), NFD), 
                    r'\pM', 
                    '' 
                ) 
            ), 
            r"[' -]", 
            '' 
        ) 
        = 
        regexp_replace( 
            lower( 
                regexp_replace( 
                    normalize(g.Commune, NFD), 
                    r'\pM', 
                    '' 
                ) 
            ), 
            r"[' -]", 
            '' 
        ) 
 
        and coalesce( 
            m.numero_departement, 
            lpad( 
                cast(cast(m.Departement as int64) as string), 
                2, 
                '0' 
            ) 
        ) 
        = 
        lpad( 
            trim(cast(g.Numero_Departement as string)), 
            2, 
            '0' 
        ) 
 
    -- L'historique s'arrête au 31/07/2026 
    where safe_cast(m.time as date) < date '2026-08-01' 
 
), 
 
quotidienne_cleaned as ( 
 
    select 
 
        -- clé temporelle 
        safe_cast(m.date as date) as date, 
 
        -- géographie déjà enrichie par load_data.py 
        m.Commune as ville, 
 
        lpad( 
            trim(cast(m.code_INSEE as string)), 
            5, 
            '0' 
        ) as code_insee, 
 
        lpad(
            trim(cast(m.Numero_Departement as string)),
            2,
            '0'
        ) as departement,
 
        m.Region as region, 
 
        m.Latitude as latitude, 
        m.Longitude as longitude, 
 
        -- météo 
        safe_cast(m.weather_code as int64) as code_meteo, 
 
        -- températures 
        m.temperature_2m_mean as temperature_moyenne, 
        m.temperature_2m_min as temperature_minimale, 
        m.temperature_2m_max as temperature_maximale, 
 
        m.apparent_temperature_mean as temperature_ressentie_moyenne, 
        m.apparent_temperature_min as temperature_ressentie_minimale, 
        m.apparent_temperature_max as temperature_ressentie_maximale, 
 
        -- humidité 
        m.relative_humidity_2m_mean as humidite_moyenne, 
        m.relative_humidity_2m_min as humidite_minimale, 
        m.relative_humidity_2m_max as humidite_maximale, 
        m.dew_point_2m_mean as point_de_rosee_moyen, 
 
        -- précipitations 
        m.precipitation_sum as precipitations_totales, 
        m.rain_sum as pluie_totale, 
        m.snowfall_sum as neige_totale, 
        m.precipitation_hours as heures_de_precipitations, 
 
        -- vent 
        m.wind_speed_10m_mean as vitesse_vent_moyenne, 
        m.wind_speed_10m_max as vitesse_vent_maximale, 
        m.wind_gusts_10m_max as rafale_vent_maximale, 
        m.wind_direction_10m_dominant as direction_vent_dominante, 
 
        -- autres variables météo 
        m.cloud_cover_mean as couverture_nuageuse_moyenne, 
        m.pressure_msl_mean as pression_moyenne, 
        m.sunshine_duration as duree_ensoleillement, 
        m.shortwave_radiation_sum as rayonnement_solaire_total, 
        m.et0_fao_evapotranspiration as evapotranspiration, 
        m.vapour_pressure_deficit_max as deficit_pression_vapeur_maximal, 
 
        -- sol 
        m.soil_moisture_0_to_7cm_mean as humidite_sol_0_7cm, 
        m.soil_moisture_7_to_28cm_mean as humidite_sol_7_28cm, 
        m.soil_moisture_28_to_100cm_mean as humidite_sol_28_100cm, 
        m.soil_temperature_0_to_7cm_mean as temperature_sol_0_7cm 
 
    from source_quotidienne m 
 
    -- La nouvelle RAW prend le relais à partir du 01/08/2026 
    where safe_cast(m.date as date) >= date '2026-08-01' 
 
), 
 
combined as ( 
 
    select * 
    from historique_cleaned 
 
    union all 
 
    select * 
    from quotidienne_cleaned 
 
), 
 
final as ( 
 
    select 
 
        -- identifiant technique unique : 1 commune + 1 date 
        to_hex( 
            md5( 
                concat( 
                    cast(date as string), 
                    '|', 
                    coalesce(code_insee, '') 
                ) 
            ) 
        ) as row_hash, 
 
        -- timestamp de construction de la ligne Silver 
        current_timestamp() as insere_a, 
 
        -- géographie 
        ville, 
        code_insee, 
        departement, 
        region, 
        latitude, 
        longitude, 
 
        -- temps 
        date, 
 
        -- météo 
        code_meteo, 
 
        temperature_moyenne, 
        temperature_minimale, 
        temperature_maximale, 
 
        temperature_ressentie_moyenne, 
        temperature_ressentie_minimale, 
        temperature_ressentie_maximale, 
 
        humidite_moyenne, 
        humidite_minimale, 
        humidite_maximale, 
        point_de_rosee_moyen, 
 
        precipitations_totales, 
        pluie_totale, 
        neige_totale, 
        heures_de_precipitations, 
 
        vitesse_vent_moyenne, 
        vitesse_vent_maximale, 
        rafale_vent_maximale, 
        direction_vent_dominante, 
 
        couverture_nuageuse_moyenne, 
        pression_moyenne, 
        duree_ensoleillement, 
        rayonnement_solaire_total, 
        evapotranspiration, 
        deficit_pression_vapeur_maximal, 
 
        humidite_sol_0_7cm, 
        humidite_sol_7_28cm, 
        humidite_sol_28_100cm, 
        temperature_sol_0_7cm 
 
    from combined 
 
) 
 
select * 
from final
