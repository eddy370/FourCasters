with source as (
    select * from {{ source('openmeteo_raw', 'open_meteo') }}
),

cleaned as (
    select
        -- clé temporelle
        safe_cast(time as date) as date_jour,

        -- localisation harmonisée : un seul jeu de colonnes, quelle que soit
        -- la source d'origine (groupe POI ou groupe Ville)
        coalesce(nom_poi, Ville) as lieu_nom,
        coalesce(
            numero_departement,
            lpad(cast(cast(Departement as int64) as string), 2, '0')
        ) as departement,
        coalesce(latitude_poi, Latitude) as latitude,
        coalesce(longitude_poi, Longitude) as longitude,

        -- flag de provenance technique (POI vs Ville) — utile pour tracer
        -- l'origine, sans porter de sens métier au-delà de ça
        case
            when nom_poi is not null then 'poi'
            when Ville is not null then 'ville'
            else null
        end as source_lieu,

        -- code météo : catégoriel (codes WMO), pas une mesure continue
        safe_cast(weather_code as int64) as weather_code,

        -- températures
        temperature_2m_mean,
        temperature_2m_min,
        temperature_2m_max,
        apparent_temperature_mean,
        apparent_temperature_min,
        apparent_temperature_max,

        -- humidité
        relative_humidity_2m_mean,
        relative_humidity_2m_min,
        relative_humidity_2m_max,
        dew_point_2m_mean,

        -- précipitations -- colonnes clés pour le croisement avec Hub'Eau
        precipitation_sum,
        rain_sum,
        snowfall_sum,
        precipitation_hours,

        -- vent
        wind_speed_10m_mean,
        wind_speed_10m_max,
        wind_gusts_10m_max,
        wind_direction_10m_dominant,

        -- autres
        cloud_cover_mean,
        pressure_msl_mean,
        sunshine_duration,
        shortwave_radiation_sum,
        et0_fao_evapotranspiration,
        vapour_pressure_deficit_max,
        soil_moisture_0_to_7cm_mean,
        soil_moisture_7_to_28cm_mean,
        soil_moisture_28_to_100cm_mean,
        soil_temperature_0_to_7cm_mean

    from source
)

select * from cleaned