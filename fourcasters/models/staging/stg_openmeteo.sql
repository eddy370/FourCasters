with source as (
    select *
    from {{ source('openmeteo_raw', 'open_meteo') }}
),

cleaned as (
    select
        -- clé temporelle
        safe_cast(time as date) as date,

        -- géographie harmonisée
        coalesce(nom_poi, Ville) as ville,

        coalesce(
            numero_departement,
            lpad(cast(cast(Departement as int64) as string), 2, '0')
        ) as departement,

        coalesce(latitude_poi, Latitude) as latitude,
        coalesce(longitude_poi, Longitude) as longitude,

        -- météo renommée selon la convention commune
        safe_cast(weather_code as int64) as code_meteo,

        -- températures
        temperature_2m_mean as temperature_moyenne,
        temperature_2m_min as temperature_minimale,
        temperature_2m_max as temperature_maximale,

        apparent_temperature_mean as temperature_ressentie_moyenne,
        apparent_temperature_min as temperature_ressentie_minimale,
        apparent_temperature_max as temperature_ressentie_maximale,

        -- humidité
        relative_humidity_2m_mean as humidite_moyenne,
        relative_humidity_2m_min as humidite_minimale,
        relative_humidity_2m_max as humidite_maximale,
        dew_point_2m_mean as point_de_rosee_moyen,

        -- précipitations
        precipitation_sum as precipitations_totales,
        rain_sum as pluie_totale,
        snowfall_sum as neige_totale,
        precipitation_hours as heures_de_precipitations,

        -- vent
        wind_speed_10m_mean as vitesse_vent_moyenne,
        wind_speed_10m_max as vitesse_vent_maximale,
        wind_gusts_10m_max as rafale_vent_maximale,
        wind_direction_10m_dominant as direction_vent_dominante,

        -- autres variables météo
        cloud_cover_mean as couverture_nuageuse_moyenne,
        pressure_msl_mean as pression_moyenne,
        sunshine_duration as duree_ensoleillement,
        shortwave_radiation_sum as rayonnement_solaire_total,
        et0_fao_evapotranspiration as evapotranspiration,
        vapour_pressure_deficit_max as deficit_pression_vapeur_maximal,

        -- sol
        soil_moisture_0_to_7cm_mean as humidite_sol_0_7cm,
        soil_moisture_7_to_28cm_mean as humidite_sol_7_28cm,
        soil_moisture_28_to_100cm_mean as humidite_sol_28_100cm,
        soil_temperature_0_to_7cm_mean as temperature_sol_0_7cm

    from source
)

select *
from cleaned
where temperature_moyenne is not null