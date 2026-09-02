from pathlib import Path
import time

import pandas as pd
import requests


# -------------------------
# CONFIGURATION
# -------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

FICHIER_VILLES = BASE_DIR / "data" / "raw" / "Departement_V3_centroide_bonne.csv"
FICHIER_SORTIE = BASE_DIR / "data" / "processed" / "meteo_briey_lehavre.csv"

API_URL = "https://archive-api.open-meteo.com/v1/archive"

DATE_DEBUT = "2000-01-01"
DATE_FIN = "2026-08-01"

VILLE_DEBUT = "Briey"
VILLE_FIN = "Le Havre"


VARIABLES = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_min",
    "temperature_2m_max",
    "apparent_temperature_mean",
    "apparent_temperature_min",
    "apparent_temperature_max",
    "relative_humidity_2m_mean",
    "relative_humidity_2m_min",
    "relative_humidity_2m_max",
    "dew_point_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "wind_speed_10m_mean",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "cloud_cover_mean",
    "pressure_msl_mean",
    "sunshine_duration",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
    "vapour_pressure_deficit_max",
    "soil_moisture_0_to_7cm_mean",
    "soil_moisture_7_to_28cm_mean",
    "soil_moisture_28_to_100cm_mean",
    "soil_temperature_0_to_7cm_mean",
]


# -------------------------
# CHARGER LES VILLES
# -------------------------

villes = pd.read_csv(FICHIER_VILLES, sep=";")

index_debut = villes.index[villes["Commune"] == VILLE_DEBUT][0]
index_fin = villes.index[villes["Commune"] == VILLE_FIN][0]

villes = villes.loc[index_debut:index_fin]


# -------------------------
# COLLECTE
# -------------------------

for index, ville in villes.iterrows():

    print(f"Collecte de {ville['Commune']}")

    # On découpe en périodes de 5 ans
    for annee in range(2000, 2027, 5):

        debut = f"{annee}-01-01"
        fin = f"{min(annee + 4, 2026)}-12-31"

        # Pour la dernière période
        if annee == 2025:
            fin = DATE_FIN

        params = {
            "latitude": ville["Latitude"],
            "longitude": ville["Longitude"],
            "start_date": debut,
            "end_date": fin,
            "daily": ",".join(VARIABLES),
            "timezone": "Europe/Paris",
            "models": "era5_seamless",
        }

        response = requests.get(
            API_URL,
            params=params,
            timeout=120
        )

        response.raise_for_status()

        data = response.json()

        # JSON Open-Meteo → DataFrame Pandas
        df = pd.DataFrame(data["daily"])

        # Informations sur la commune
        df.insert(0, "Ville", ville["Commune"])
        df.insert(1, "Departement", ville["Numero_Departement"])
        df.insert(2, "Latitude", ville["Latitude"])
        df.insert(3, "Longitude", ville["Longitude"])

        # Le fichier est créé au premier passage,
        # puis les données suivantes sont ajoutées.
        fichier_existe = FICHIER_SORTIE.exists()

        df.to_csv(
            FICHIER_SORTIE,
            mode="a",
            header=not fichier_existe,
            index=False
        )

        print(f"  {debut} → {fin} : {len(df)} lignes")

        time.sleep(2)


print("Collecte terminée.")