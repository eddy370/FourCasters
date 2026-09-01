import time
import hashlib
import traceback
from datetime import datetime, timezone, timedelta, date

import requests
from google.cloud import bigquery


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ID = "projet-les-fourcasters"

SOURCE_TABLE_ID = "projet-les-fourcasters.dbt_dev.referentiel_geographique"
DEST_TABLE_ID = "projet-les-fourcasters.raw_openmeteo.meteo_journaliere_raw"

# L'historique avant cette date existe déjà.
DATE_DEBUT_PAR_DEFAUT = "2026-08-01"

API_URL = "https://archive-api.open-meteo.com/v1/archive"
MAX_RATE_LIMIT_RETRIES = 6

client = bigquery.Client(project=PROJECT_ID)


# ============================================================
# VARIABLES METEO
# ============================================================

VARIABLES_METEO = [
    "weather_code",
    "temperature_2m_mean", "temperature_2m_min", "temperature_2m_max",
    "apparent_temperature_mean", "apparent_temperature_min", "apparent_temperature_max",
    "relative_humidity_2m_mean", "relative_humidity_2m_min", "relative_humidity_2m_max",
    "dew_point_2m_mean",
    "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours",
    "wind_speed_10m_mean", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
    "cloud_cover_mean", "pressure_msl_mean", "sunshine_duration", "shortwave_radiation_sum",
    "et0_fao_evapotranspiration", "vapour_pressure_deficit_max",
    "soil_moisture_0_to_7cm_mean", "soil_moisture_7_to_28cm_mean",
    "soil_moisture_28_to_100cm_mean", "soil_temperature_0_to_7cm_mean",
]


# ============================================================
# HASH — clé naturelle (commune + date), pas le contenu météo.
# Une révision ERA5 d'une valeur existante ne crée pas de doublon.
# ============================================================

def calculer_hash(code_insee: str, date_observation: str) -> str:
    valeur = f"{code_insee}|{date_observation}"
    return hashlib.sha256(valeur.encode("utf-8")).hexdigest()


def enrichir_lignes(data: list) -> list:
    now = datetime.now(timezone.utc).isoformat()
    enriched = []

    for row in data:
        row_copy = dict(row)
        row_copy["row_hash"] = calculer_hash(
            row_copy["code_INSEE"],
            row_copy["date"]
        )
        row_copy["inserted_at"] = now
        enriched.append(row_copy)

    return enriched


# ============================================================
# CHARGEMENT BIGQUERY
# ============================================================

def charger_dans_bigquery(data: list):

    if not data:
        return

    data_enrichie = enrichir_lignes(data)

    hashes = [
        row["row_hash"]
        for row in data_enrichie
    ]

    dates = [
        row["date"]
        for row in data_enrichie
    ]

    # Le filtre de date exploite le PARTITION BY date de la table :
    # on ne scanne que les partitions concernées par ce lot.
    query = f"""
        SELECT row_hash
        FROM `{DEST_TABLE_ID}`
        WHERE date BETWEEN @date_min AND @date_max
          AND row_hash IN UNNEST(@hashes)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "date_min",
                "DATE",
                min(dates)
            ),
            bigquery.ScalarQueryParameter(
                "date_max",
                "DATE",
                max(dates)
            ),
            bigquery.ArrayQueryParameter(
                "hashes",
                "STRING",
                hashes
            ),
        ]
    )

    existants = {
        row["row_hash"]
        for row in client.query(
            query,
            job_config=job_config
        ).result()
    }

    nouvelles_lignes = [
        row
        for row in data_enrichie
        if row["row_hash"] not in existants
    ]

    if not nouvelles_lignes:
        print("→ Aucune nouvelle ligne à insérer.")
        return

    load_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    load_job = client.load_table_from_json(
        nouvelles_lignes,
        DEST_TABLE_ID,
        job_config=load_config
    )

    load_job.result()

    print(
        f"→ {len(nouvelles_lignes)} nouvelles lignes insérées."
    )


# ============================================================
# INGESTION
# ============================================================

def ingest_data():

    batch = []

    try:

        # ----------------------------------------------------
        # Création de la table RAW
        # ----------------------------------------------------

        client.query(f"""
        CREATE TABLE IF NOT EXISTS `{DEST_TABLE_ID}` (
            Commune STRING,
            Latitude FLOAT64,
            Longitude FLOAT64,
            Region STRING,
            Departement STRING,
            Numero_Departement STRING,
            code_INSEE STRING,
            date DATE,
            row_hash STRING,
            inserted_at TIMESTAMP,
            weather_code INT64,
            temperature_2m_mean FLOAT64,
            temperature_2m_min FLOAT64,
            temperature_2m_max FLOAT64,
            apparent_temperature_mean FLOAT64,
            apparent_temperature_min FLOAT64,
            apparent_temperature_max FLOAT64,
            relative_humidity_2m_mean FLOAT64,
            relative_humidity_2m_min FLOAT64,
            relative_humidity_2m_max FLOAT64,
            dew_point_2m_mean FLOAT64,
            precipitation_sum FLOAT64,
            rain_sum FLOAT64,
            snowfall_sum FLOAT64,
            precipitation_hours FLOAT64,
            wind_speed_10m_mean FLOAT64,
            wind_speed_10m_max FLOAT64,
            wind_gusts_10m_max FLOAT64,
            wind_direction_10m_dominant FLOAT64,
            cloud_cover_mean FLOAT64,
            pressure_msl_mean FLOAT64,
            sunshine_duration FLOAT64,
            shortwave_radiation_sum FLOAT64,
            et0_fao_evapotranspiration FLOAT64,
            vapour_pressure_deficit_max FLOAT64,
            soil_moisture_0_to_7cm_mean FLOAT64,
            soil_moisture_7_to_28cm_mean FLOAT64,
            soil_moisture_28_to_100cm_mean FLOAT64,
            soil_temperature_0_to_7cm_mean FLOAT64
        )
        PARTITION BY date
        CLUSTER BY code_INSEE
        """).result()


        # ----------------------------------------------------
        # Date de fin : J-7
        # ----------------------------------------------------
        # On laisse 7 jours de recul pour éviter de charger
        # des journées Open-Meteo encore incomplètes.

        end_date_str = (
            date.today() - timedelta(days=7)
        ).strftime("%Y-%m-%d")


        # ----------------------------------------------------
        # Communes à mettre à jour
        # ----------------------------------------------------

        villes = list(
            client.query(f"""
                SELECT
                    v.Commune,
                    v.Latitude,
                    v.Longitude,
                    v.Region,
                    v.Departement,
                    v.Numero_Departement,
                    v.code_INSEE,
                    d.max_date

                FROM `{SOURCE_TABLE_ID}` v

                LEFT JOIN (
                    SELECT
                        code_INSEE,
                        MAX(date) AS max_date
                    FROM `{DEST_TABLE_ID}`
                    GROUP BY code_INSEE
                ) d

                ON v.code_INSEE = d.code_INSEE

                WHERE
                    d.max_date IS NULL
                    OR d.max_date < DATE('{end_date_str}')
            """).result()
        )


        if not villes:
            print("Toutes les communes sont déjà à jour.")
            return


        print(
            f"Communes à traiter : {len(villes)}"
        )

        rate_limit_count = 0


        # ----------------------------------------------------
        # Boucle sur les communes
        # ----------------------------------------------------

        for index, ville in enumerate(
            villes,
            start=1
        ):

            commune = ville["Commune"]
            code_insee = ville["code_INSEE"]


            if code_insee is None:

                print(
                    f"⚠️ code_INSEE manquant pour "
                    f"{commune!r}, commune ignorée."
                )

                continue


            derniere_date = ville["max_date"]


            date_debut = (
                (
                    derniere_date
                    + timedelta(days=1)
                ).strftime("%Y-%m-%d")

                if derniere_date

                else DATE_DEBUT_PAR_DEFAUT
            )


            if date_debut > end_date_str:
                continue


            print(
                f"[{index}/{len(villes)}] "
                f"{commune} : "
                f"{date_debut} → {end_date_str}"
            )


            # ------------------------------------------------
            # Paramètres Open-Meteo
            # ------------------------------------------------

            params = {

                "latitude":
                    ville["Latitude"],

                "longitude":
                    ville["Longitude"],

                "start_date":
                    date_debut,

                "end_date":
                    end_date_str,

                "daily":
                    ",".join(VARIABLES_METEO),

                "timezone":
                    "Europe/Paris",

                "models":
                    "era5_seamless",
            }


            # ------------------------------------------------
            # Appel API
            # ------------------------------------------------

            while True:

                response = requests.get(
                    API_URL,
                    params=params,
                    timeout=60
                )


                try:
                    data = response.json()

                except ValueError:
                    data = {}


                rate_limited = (

                    response.status_code == 429

                    or "limit" in str(
                        data.get(
                            "reason",
                            ""
                        )
                    ).lower()
                )


                # --------------------------------------------
                # Rate limit
                # --------------------------------------------

                if rate_limited:

                    rate_limit_count += 1


                    if (
                        rate_limit_count
                        >= MAX_RATE_LIMIT_RETRIES
                    ):

                        charger_dans_bigquery(
                            batch
                        )

                        raise RuntimeError(
                            "Trop de rate limits "
                            "Open-Meteo consécutifs."
                        )


                    print(
                        f"🚦 Rate limit. "
                        f"Pause 61s "
                        f"({rate_limit_count}/"
                        f"{MAX_RATE_LIMIT_RETRIES})"
                    )

                    time.sleep(61)

                    continue


                # --------------------------------------------
                # Autre erreur API
                # --------------------------------------------

                if (
                    response.status_code != 200
                    or data.get("error")
                ):

                    charger_dans_bigquery(
                        batch
                    )

                    raise RuntimeError(
                        f"Erreur Open-Meteo pour "
                        f"{commune} : "
                        f"{data.get('reason', response.status_code)}"
                    )


                # --------------------------------------------
                # Succès
                # --------------------------------------------

                rate_limit_count = 0

                break


            # ------------------------------------------------
            # Lecture des données quotidiennes
            # ------------------------------------------------

            daily = data.get(
                "daily",
                {}
            )

            temperatures = (
                daily.get(
                    "temperature_2m_mean"
                )
                or []
            )


            for i, jour in enumerate(
                daily.get(
                    "time",
                    []
                )
            ):

                # On ignore les journées sans température :
                # elles seront retentées au prochain run.
                if (
                    i >= len(temperatures)
                    or temperatures[i] is None
                ):
                    continue


                row = {

                    "Commune":
                        str(commune)
                        if commune is not None
                        else None,

                    "Latitude":
                        float(ville["Latitude"])
                        if ville["Latitude"] is not None
                        else None,

                    "Longitude":
                        float(ville["Longitude"])
                        if ville["Longitude"] is not None
                        else None,

                    "Region":
                        str(ville["Region"])
                        if ville["Region"] is not None
                        else None,

                    "Departement":
                        str(ville["Departement"])
                        if ville["Departement"] is not None
                        else None,

                    "Numero_Departement":
                        str(
                            ville[
                                "Numero_Departement"
                            ]
                        )
                        if ville[
                            "Numero_Departement"
                        ] is not None
                        else None,

                    "code_INSEE":
                        str(code_insee),

                    "date":
                        jour,
                }


                # --------------------------------------------
                # Variables météo
                # --------------------------------------------

                for variable in VARIABLES_METEO:

                    valeurs = daily.get(
                        variable
                    )

                    row[variable] = (
                        valeurs[i]
                        if valeurs is not None
                        and i < len(valeurs)
                        else None
                    )


                batch.append(
                    row
                )


            # ------------------------------------------------
            # Chargement par lots
            # ------------------------------------------------

            if len(batch) >= 5000:

                charger_dans_bigquery(
                    batch
                )

                batch = []


            time.sleep(3)


        # ----------------------------------------------------
        # Dernier batch
        # ----------------------------------------------------

        charger_dans_bigquery(
            batch
        )

        batch = []

        print(
            "✅ Ingestion Open-Meteo terminée."
        )


    # ========================================================
    # GESTION DES ERREURS
    # ========================================================

    except Exception:

        if batch:

            print(
                "⚠️ Sauvegarde du batch "
                "avant arrêt..."
            )

            try:

                charger_dans_bigquery(
                    batch
                )

            except Exception:

                print(
                    "❌ Échec de la sauvegarde "
                    "du dernier batch."
                )


        print(
            "🔥 ERREUR pendant l'ingestion :"
        )

        print(
            traceback.format_exc()
        )

        # Pour que run_pipeline.py sache
        # que dbt ne doit pas tourner.
        raise


# ============================================================
# EXECUTION DIRECTE
# ============================================================

if __name__ == "__main__":
    ingest_data()