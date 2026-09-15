"""
Ingestion Open-Meteo : récupération incrémentale d'une fenêtre récente
(35 jours de révision pour les corrections ERA5), row_hash calculé sur le
contenu métier, chargement direct en WRITE_APPEND, rollback par batch_id
en cas d'erreur.
"""

import logging
import time
from datetime import date, datetime, timedelta

import requests


from src.bigquery_utils import (
    RECUL_JOURS,
    REFERENTIEL_TABLE_ID,
    TAILLE_BATCH,
    TIMEOUT_HTTP,
    annuler_le_lot,
    charger_dans_bigquery,
    client,
)

logger = logging.getLogger(__name__)

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"

METEO_TABLE_ID = "projet-les-fourcasters.raw_openmeteo.meteo_journaliere_raw"

METEO_DATE_DEBUT_PAR_DEFAUT = "2026-08-01"

# On redemande une fenêtre récente pour récupérer les corrections ERA5.
METEO_FENETRE_REVISION_JOURS = 35

MAX_RATE_LIMIT_RETRIES = 6
PAUSE_RATE_LIMIT_SECONDES = 61
PAUSE_ENTRE_COMMUNES_SECONDES = 3

# Retry par commune sur les erreurs transitoires (réseau, 5xx, erreurs API
# ponctuelles type "Something went wrong"). Le rate limit est géré à part
# (boucle dédiée ci-dessous) et n'entre pas dans ce budget de tentatives.
MAX_TENTATIVES_COMMUNE = 5
BACKOFF_COMMUNE_SECONDES = [5, 10, 20, 40]

# NOUVEAU : nombre de communes envoyées dans une requête Open-Meteo.
TAILLE_PAQUET_COMMUNES = 10

# NOUVEAU : timeout spécifique à Open-Meteo.
# 5 secondes pour établir la connexion / handshake,
# 60 secondes pour lire la réponse.
TIMEOUT_OPENMETEO = (5, 60)

# NOUVEAU : session persistante pour réutiliser les connexions HTTPS.
SESSION = requests.Session()
SESSION.mount(
    "https://",
    requests.adapters.HTTPAdapter(
        pool_connections=4,
        pool_maxsize=8,
    ),
)


class RateLimitEpuiseError(RuntimeError):
    """Rate limit Open-Meteo persistant malgré les pauses dédiées."""


METEO_VARIABLES = [
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


def creer_table_meteo() -> None:
    """Crée la table RAW météo et ajoute batch_id si nécessaire."""
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{METEO_TABLE_ID}` (
            Latitude FLOAT64,
            Longitude FLOAT64,
            code_INSEE STRING,
            date DATE,
            row_hash STRING,
            inserted_at TIMESTAMP,
            batch_id STRING,
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

    # La table existe déjà dans ton projet :
    # CREATE TABLE IF NOT EXISTS n'ajoute pas une colonne.
    client.query(
        f"ALTER TABLE `{METEO_TABLE_ID}` ADD COLUMN IF NOT EXISTS batch_id STRING"
    ).result()


def lire_communes() -> list:
    """Récupère les communes et leur dernière date chargée."""
    return list(client.query(f"""
        SELECT v.Commune, v.Latitude, v.Longitude, v.code_INSEE, d.max_date
        FROM `{REFERENTIEL_TABLE_ID}` v
        LEFT JOIN (
            SELECT code_INSEE, MAX(date) AS max_date
            FROM `{METEO_TABLE_ID}`
            GROUP BY code_INSEE
        ) d
            ON v.code_INSEE = d.code_INSEE
        """).result())


def calculer_date_debut_meteo(derniere_date, date_fin: date) -> str:
    """Détermine la date de début Open-Meteo."""
    date_debut_defaut = datetime.strptime(
        METEO_DATE_DEBUT_PAR_DEFAUT, "%Y-%m-%d"
    ).date()

    if derniere_date is None:
        return date_debut_defaut.strftime("%Y-%m-%d")

    date_revision = date_fin - timedelta(days=METEO_FENETRE_REVISION_JOURS)

    return max(date_revision, date_debut_defaut).strftime("%Y-%m-%d")


def _appel_http_openmeteo(params: dict, commune: str, rate_limit_count: int) -> tuple:
    """Un seul cycle d'appel Open-Meteo, avec gestion du rate limit (429)."""
    while True:
        try:
            # MODIFIÉ :
            # session persistante + timeout propre à Open-Meteo.
            response = SESSION.get(
                OPENMETEO_URL,
                params=params,
                timeout=TIMEOUT_OPENMETEO,
            )
        except requests.exceptions.RequestException as erreur:
            raise RuntimeError(
                f"Erreur réseau Open-Meteo pour {commune} : {erreur}"
            ) from erreur

        try:
            data = response.json()
        except ValueError:
            data = {}

        # MODIFIÉ :
        # en mode paquet, une réponse réussie peut être une liste.
        reason = (
            data.get("reason", "")
            if isinstance(data, dict)
            else ""
        )

        rate_limited = (
            response.status_code == 429
            or "limit" in str(reason).lower()
        )

        if rate_limited:
            rate_limit_count += 1

            if rate_limit_count >= MAX_RATE_LIMIT_RETRIES:
                raise RateLimitEpuiseError(
                    "Trop de rate limits Open-Meteo consécutifs."
                )

            logger.warning(
                f"🚦 Rate limit pour {commune}. Pause {PAUSE_RATE_LIMIT_SECONDES}s "
                f"({rate_limit_count}/{MAX_RATE_LIMIT_RETRIES})"
            )
            time.sleep(PAUSE_RATE_LIMIT_SECONDES)
            continue

        # MODIFIÉ :
        # data peut être une liste lorsqu'on demande plusieurs coordonnées.
        api_error = (
            isinstance(data, dict)
            and data.get("error")
        )

        if response.status_code != 200 or api_error:
            raison = (
                data.get("reason", response.status_code)
                if isinstance(data, dict)
                else response.status_code
            )

            raise RuntimeError(
                f"Erreur Open-Meteo pour {commune} : {raison}"
            )

        return data, 0


def appeler_openmeteo(params: dict, commune: str, rate_limit_count: int) -> tuple:
    """Appelle Open-Meteo pour une commune, avec retry et backoff progressif
    sur les erreurs transitoires (réseau, erreurs API ponctuelles type
    "Something went wrong").

    Seule cette commune est réessayée : Hub'Eau et les communes déjà
    traitées dans ce run ne sont pas affectés par ce retry.
    """
    for tentative in range(1, MAX_TENTATIVES_COMMUNE + 1):
        try:
            data, rate_limit_count = _appel_http_openmeteo(
                params, commune, rate_limit_count
            )

            if tentative > 1:
                logger.info(
                    f"✅ {commune} : requête réussie après {tentative} tentatives."
                )

            return data, rate_limit_count

        except RateLimitEpuiseError:
            # Rate limit déjà retenté longuement (pauses de 61s) : pas la
            # peine de reboucler ici, on laisse l'échec remonter.
            raise

        except RuntimeError as erreur:
            if tentative >= MAX_TENTATIVES_COMMUNE:
                logger.error(
                    f"❌ {commune} : échec définitif après {tentative} tentatives : {erreur}"
                )
                raise

            delai = BACKOFF_COMMUNE_SECONDES[tentative - 1]
            logger.warning(
                f"⏳ {commune} : tentative {tentative}/{MAX_TENTATIVES_COMMUNE} "
                f"échouée ({erreur}). Nouvel essai dans {delai}s."
            )
            time.sleep(delai)

    raise RuntimeError(
        f"Échec Open-Meteo pour {commune} "
        f"après {MAX_TENTATIVES_COMMUNE} tentatives."
    )


def verifier_longueurs_openmeteo(data: dict, commune: str) -> None:
    """Vérifie que toutes les séries daily ont la même longueur."""
    daily = data.get("daily", {})
    jours = daily.get("time", [])

    if not jours:
        raise RuntimeError(f"Open-Meteo {commune} : aucune date reçue.")

    taille_attendue = len(jours)

    for variable in METEO_VARIABLES:
        valeurs = daily.get(variable)

        if valeurs is None:
            raise RuntimeError(
                f"Open-Meteo {commune} : variable absente : {variable}."
            )

        if len(valeurs) != taille_attendue:
            raise RuntimeError(
                f"Open-Meteo {commune} : longueur incohérente pour {variable} "
                f"({len(valeurs)} au lieu de {taille_attendue})."
            )


def construire_lignes_meteo(data: dict, ville) -> list:
    """Transforme Open-Meteo en lignes journalières."""
    verifier_longueurs_openmeteo(data, ville["Commune"])

    daily = data["daily"]
    lignes = []

    for i, jour in enumerate(daily["time"]):
        # Journée incomplète : elle sera redemandée au prochain run.
        if daily["temperature_2m_mean"][i] is None:
            continue

        row = {
            "Latitude": (
                float(ville["Latitude"]) if ville["Latitude"] is not None else None
            ),
            "Longitude": (
                float(ville["Longitude"]) if ville["Longitude"] is not None else None
            ),
            "code_INSEE": str(ville["code_INSEE"]),
            "date": jour,
        }

        for variable in METEO_VARIABLES:
            row[variable] = daily[variable][i]

        lignes.append(row)

    return lignes


def executer_ingestion_openmeteo(batch_id: str) -> int:
    """Récupère et charge la fenêtre récente Open-Meteo."""
    creer_table_meteo()

    date_fin = date.today() - timedelta(days=RECUL_JOURS)
    date_fin_str = date_fin.strftime("%Y-%m-%d")

    villes = lire_communes()

    logger.info(f"Communes à traiter : {len(villes)} (batch {batch_id})")

    # NOUVEAU :
    # préparation des communes avec leur date de début individuelle.
    villes_par_date = {}

    for ville in villes:
        commune = ville["Commune"]

        if ville["code_INSEE"] is None:
            logger.warning(
                f"⚠️ code_INSEE manquant pour {commune!r}, commune ignorée."
            )
            continue

        date_debut = calculer_date_debut_meteo(
            ville["max_date"],
            date_fin,
        )

        if date_debut > date_fin_str:
            continue

        # Les communes ayant la même date_debut peuvent être
        # envoyées ensemble dans la même requête.
        if date_debut not in villes_par_date:
            villes_par_date[date_debut] = []

        villes_par_date[date_debut].append(ville)

    batch = []
    total_insere = 0
    rate_limit_count = 0

    numero_paquet = 0

    # NOUVEAU :
    # traitement par groupes ayant la même date_debut,
    # puis par paquets de 10 communes.
    for date_debut, groupe_villes in villes_par_date.items():

        for debut in range(
            0,
            len(groupe_villes),
            TAILLE_PAQUET_COMMUNES,
        ):

            paquet = groupe_villes[
                debut:debut + TAILLE_PAQUET_COMMUNES
            ]

            numero_paquet += 1

            logger.info(
                f"📦 Paquet {numero_paquet} : "
                f"{len(paquet)} communes "
                f"({date_debut} → {date_fin_str})"
            )

            latitudes = ",".join(
                str(ville["Latitude"])
                for ville in paquet
            )

            longitudes = ",".join(
                str(ville["Longitude"])
                for ville in paquet
            )

            params = {
                "latitude": latitudes,
                "longitude": longitudes,
                "start_date": date_debut,
                "end_date": date_fin_str,
                "daily": ",".join(METEO_VARIABLES),
                "timezone": "Europe/Paris",
                "models": "era5_seamless",
            }

            libelle_paquet = (
                f"paquet {numero_paquet} "
                f"({len(paquet)} communes)"
            )

            data, rate_limit_count = appeler_openmeteo(
                params,
                libelle_paquet,
                rate_limit_count,
            )

            # Open-Meteo renvoie :
            # - un dict pour une seule coordonnée
            # - une liste de dicts pour plusieurs coordonnées
            if isinstance(data, dict):
                resultats = [data]

            elif isinstance(data, list):
                resultats = data

            else:
                raise RuntimeError(
                    f"Open-Meteo {libelle_paquet} : "
                    f"format de réponse inattendu "
                    f"({type(data).__name__})."
                )

            # Vérifie qu'on a exactement une réponse par commune.
            if len(resultats) != len(paquet):
                raise RuntimeError(
                    f"Open-Meteo {libelle_paquet} : "
                    f"{len(paquet)} communes envoyées mais "
                    f"{len(resultats)} réponses reçues."
                )

            # On rattache chaque résultat à la commune correspondante.
            for ville, data_ville in zip(
                paquet,
                resultats,
            ):
                batch.extend(
                    construire_lignes_meteo(
                        data_ville,
                        ville,
                    )
                )

            if len(batch) >= TAILLE_BATCH:
                total_insere += charger_dans_bigquery(
                    data=batch,
                    table_id=METEO_TABLE_ID,
                    colonne_date="date",
                    batch_id=batch_id,
                )

                batch = []

            # Même pause que dans ton script original.
            time.sleep(PAUSE_ENTRE_COMMUNES_SECONDES)

    total_insere += charger_dans_bigquery(
        data=batch,
        table_id=METEO_TABLE_ID,
        colonne_date="date",
        batch_id=batch_id,
    )

    logger.info(
        f"📦 Nombre de paquets Open-Meteo envoyés : {numero_paquet}"
    )

    return total_insere


def ingest_openmeteo(batch_id: str) -> None:
    """Orchestre l'ingestion Open-Meteo et annule le batch en cas d'erreur."""
    try:
        total_insere = executer_ingestion_openmeteo(batch_id)

        logger.info(
            f"✅ Ingestion Open-Meteo terminée : {total_insere} nouvelles lignes."
        )

    except Exception:
        logger.exception(
            f"Erreur pendant l'ingestion Open-Meteo (batch {batch_id})."
        )
        annuler_le_lot(METEO_TABLE_ID, batch_id)
        raise