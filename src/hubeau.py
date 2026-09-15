"""
Ingestion Hub'Eau : référentiel des stations (remplacement complet en
WRITE_TRUNCATE) et observations élaborées HIXnJ sur une fenêtre récente
(35 jours de révision), limitées aux stations pertinentes pour FourCasters.
row_hash calculé sur le contenu métier, chargement direct en WRITE_APPEND,
rollback par batch_id en cas d'erreur.
"""

import logging
import time
from datetime import date, timedelta

import requests
from google.cloud import bigquery

from src.bigquery_utils import (
    RECUL_JOURS,
    REFERENTIEL_TABLE_ID,
    TAILLE_BATCH,
    TIMEOUT_HTTP,
    annuler_le_lot,
    charger_dans_bigquery,
    client,
    extraire_colonnes,
)

logger = logging.getLogger(__name__)

TAILLE_PAGE_API = 1000

# Retry réseau Hub'Eau.
MAX_TENTATIVES_RESEAU = 5
PAUSE_RESEAU_SECONDES = 10

HUBEAU_STATIONS_URL = (
    "https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations"
)
HUBEAU_OBS_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

HUBEAU_STATIONS_TABLE_ID = "projet-les-fourcasters.raw_hubeau.stations_hydrometriques"
HUBEAU_OBS_TABLE_ID = (
    "projet-les-fourcasters.raw_hubeau.observations_hydrometriques_elaborees"
)

HUBEAU_FENETRE_REVISION_JOURS = 35

# Hauteur instantanée maximale journalière.
HUBEAU_GRANDEUR = "HIXnJ"

HUBEAU_OBS_COLONNES = [
    "code_site",
    "code_station",
    "date_obs_elab",
    "resultat_obs_elab",
    "date_prod",
    "code_statut",
    "libelle_statut",
    "code_methode",
    "libelle_methode",
    "code_qualification",
    "libelle_qualification",
    "longitude",
    "latitude",
    "grandeur_hydro_elab",
]

HUBEAU_STATIONS_COLONNES = [
    "code_station",
    "code_site",
    "libelle_station",
    "code_commune_station",
    "libelle_commune",
    "code_departement",
    "libelle_departement",
    "code_region",
    "libelle_region",
    "code_cours_eau",
    "libelle_cours_eau",
    "latitude_station",
    "longitude_station",
    "date_ouverture_station",
    "date_fermeture_station",
    "en_service",
]

# Toutes les stations à <= 15 km d'une commune du projet,
# plus la station la plus proche pour une commune sans station <= 15 km.
STATIONS_DISTANCE_MAX_METRES = 15000
STATIONS_PERTINENTES_ATTENDU = 3011
STATIONS_PERTINENTES_MIN = 2700
STATIONS_PERTINENTES_MAX = 3300

# Découpage des stations dans les appels Hub'Eau.
TAILLE_LOT_CODE_ENTITE = 200


def requete_hubeau_avec_retry(
    url: str,
    params: dict | None,
    libelle: str,
) -> requests.Response:
    """Effectue un GET Hub'Eau avec retry/backoff."""
    tentative = 0

    while True:
        try:
            return requests.get(
                url,
                params=params,
                timeout=TIMEOUT_HTTP,
            )

        except requests.exceptions.RequestException as erreur:
            tentative += 1

            if tentative >= MAX_TENTATIVES_RESEAU:
                raise RuntimeError(
                    f"Hub'Eau {libelle} : échec réseau après "
                    f"{tentative} tentatives ({erreur})."
                ) from erreur

            pause = PAUSE_RESEAU_SECONDES * tentative

            logger.warning(
                f"Hub'Eau {libelle} : erreur réseau ({erreur}). "
                f"Nouvelle tentative dans {pause}s "
                f"({tentative}/{MAX_TENTATIVES_RESEAU})."
            )

            time.sleep(pause)


def iterer_pages_hubeau(
    url: str,
    params: dict,
    libelle: str,
):
    """Parcourt toutes les pages d'une API Hub'Eau."""
    params_premiere_requete = dict(params)
    params_premiere_requete["size"] = TAILLE_PAGE_API

    url_courante = url
    params_courants = params_premiere_requete

    total_attendu = None
    total_recu = 0
    numero_page = 1

    while url_courante:
        response = requete_hubeau_avec_retry(
            url_courante,
            params_courants,
            f"{libelle} — page {numero_page}",
        )

        try:
            data = response.json()

        except ValueError as erreur:
            logger.error(
                f"Hub'Eau {libelle} : réponse JSON invalide "
                f"(HTTP {response.status_code})."
            )

            raise RuntimeError(
                f"Hub'Eau {libelle} : réponse JSON invalide."
            ) from erreur

        if response.status_code not in (200, 206) or data.get("error"):
            raise RuntimeError(
                f"Erreur Hub'Eau {libelle} : "
                f"{data.get('message', response.status_code)}"
            )

        if total_attendu is None:
            total_attendu = data.get("count")

        lignes = data.get("data", [])
        total_recu += len(lignes)

        logger.debug(
            f"Hub'Eau {libelle} — page {numero_page} : "
            f"{len(lignes)} lignes "
            f"({total_recu}/{total_attendu or '?'})"
        )

        yield lignes, total_recu, total_attendu

        # L'URL next contient déjà les paramètres + curseur.
        url_courante = data.get("next")
        params_courants = None
        numero_page += 1

    if total_attendu is not None and total_recu != total_attendu:
        raise RuntimeError(
            f"Hub'Eau {libelle} incomplet : "
            f"{total_recu} lignes récupérées sur "
            f"{total_attendu} attendues."
        )

    logger.info(f"Hub'Eau {libelle} — récupération complète : " f"{total_recu} lignes.")


def ingest_hubeau_stations() -> None:
    """Recharge le référentiel complet des stations Hub'Eau."""
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{HUBEAU_STATIONS_TABLE_ID}` (
            code_station STRING,
            code_site STRING,
            libelle_station STRING,
            code_commune_station STRING,
            libelle_commune STRING,
            code_departement STRING,
            libelle_departement STRING,
            code_region STRING,
            libelle_region STRING,
            code_cours_eau STRING,
            libelle_cours_eau STRING,
            latitude_station FLOAT64,
            longitude_station FLOAT64,
            date_ouverture_station TIMESTAMP,
            date_fermeture_station TIMESTAMP,
            en_service BOOL
        )
        """).result()

    stations = []

    for lignes_page, _, _ in iterer_pages_hubeau(
        HUBEAU_STATIONS_URL,
        {},
        "stations",
    ):
        stations.extend(lignes_page)

    logger.info(f"Stations hydrométriques récupérées : {len(stations)}")

    lignes = [
        extraire_colonnes(station, HUBEAU_STATIONS_COLONNES) for station in stations
    ]

    if not lignes:
        raise RuntimeError("Aucune station Hub'Eau à charger.")

    load_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=client.get_table(HUBEAU_STATIONS_TABLE_ID).schema,
    )

    client.load_table_from_json(
        lignes,
        HUBEAU_STATIONS_TABLE_ID,
        job_config=load_config,
    ).result()

    logger.info(f"Hub'Eau stations chargé : {len(lignes)} stations.")


def creer_table_hubeau_obs() -> None:
    """Crée la RAW Hub'Eau observations et ajoute les colonnes techniques si nécessaire."""
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{HUBEAU_OBS_TABLE_ID}` (
            code_site STRING,
            code_station STRING,
            date_obs_elab DATE,
            resultat_obs_elab FLOAT64,
            date_prod TIMESTAMP,
            code_statut STRING,
            libelle_statut STRING,
            code_methode STRING,
            libelle_methode STRING,
            code_qualification STRING,
            libelle_qualification STRING,
            longitude FLOAT64,
            latitude FLOAT64,
            grandeur_hydro_elab STRING,
            row_hash STRING,
            inserted_at TIMESTAMP,
            batch_id STRING
        )
        PARTITION BY date_obs_elab
        CLUSTER BY code_station
        """).result()

    # CREATE TABLE IF NOT EXISTS n'ajoute pas les colonnes techniques
    # à une table déjà existante : on les ajoute donc explicitement.
    for colonne, type_sql in (
        ("row_hash", "STRING"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_id", "STRING"),
    ):
        client.query(f"""
            ALTER TABLE `{HUBEAU_OBS_TABLE_ID}`
            ADD COLUMN IF NOT EXISTS {colonne} {type_sql}
            """).result()


def recuperer_stations_pertinentes() -> list:
    """Calcule les stations Hub'Eau pertinentes pour FourCasters."""
    query = f"""
        WITH communes AS (
            SELECT
                code_INSEE,
                ST_GEOGPOINT(Longitude, Latitude) AS geo
            FROM `{REFERENTIEL_TABLE_ID}`
        ),

        stations AS (
            SELECT
                code_station,
                ST_GEOGPOINT(
                    longitude_station,
                    latitude_station
                ) AS geo
            FROM `{HUBEAU_STATIONS_TABLE_ID}`
            WHERE
                longitude_station IS NOT NULL
                AND latitude_station IS NOT NULL
        ),

        distances AS (
            SELECT
                c.code_INSEE,
                s.code_station,
                ST_DISTANCE(c.geo, s.geo) AS distance_m
            FROM communes c
            CROSS JOIN stations s
        ),

        stations_proches AS (
            SELECT DISTINCT code_station
            FROM distances
            WHERE distance_m <= {STATIONS_DISTANCE_MAX_METRES}
        ),

        communes_sans_station_proche AS (
            SELECT DISTINCT code_INSEE
            FROM communes
            WHERE code_INSEE NOT IN (
                SELECT code_INSEE
                FROM distances
                WHERE distance_m <= {STATIONS_DISTANCE_MAX_METRES}
            )
        ),

        station_plus_proche AS (
            SELECT
                d.code_INSEE,
                ARRAY_AGG(
                    d.code_station
                    ORDER BY d.distance_m
                    LIMIT 1
                )[OFFSET(0)] AS code_station
            FROM distances d
            INNER JOIN communes_sans_station_proche c
                ON d.code_INSEE = c.code_INSEE
            GROUP BY d.code_INSEE
        )

        SELECT code_station
        FROM stations_proches

        UNION DISTINCT

        SELECT code_station
        FROM station_plus_proche
    """

    lignes = client.query(query).result()

    return sorted(
        {ligne["code_station"] for ligne in lignes if ligne["code_station"] is not None}
    )


def verifier_nombre_stations(stations: list) -> None:
    """Vérifie que le nombre de stations reste dans une plage plausible."""
    nb_stations = len(stations)

    if not STATIONS_PERTINENTES_MIN <= nb_stations <= STATIONS_PERTINENTES_MAX:
        raise RuntimeError(
            f"Nombre de stations pertinentes hors plage : "
            f"{nb_stations} "
            f"(attendu entre {STATIONS_PERTINENTES_MIN} "
            f"et {STATIONS_PERTINENTES_MAX})."
        )

    if nb_stations != STATIONS_PERTINENTES_ATTENDU:
        logger.warning(
            f"⚠️ Référentiel Hub'Eau : {nb_stations} stations "
            f"(référence : {STATIONS_PERTINENTES_ATTENDU})."
        )


def executer_ingestion_hubeau_obs(
    batch_id: str,
) -> tuple:
    """Récupère et charge la fenêtre récente HIXnJ."""
    creer_table_hubeau_obs()

    stations = recuperer_stations_pertinentes()
    verifier_nombre_stations(stations)

    date_fin = date.today() - timedelta(days=RECUL_JOURS)

    date_debut = date_fin - timedelta(days=HUBEAU_FENETRE_REVISION_JOURS)

    logger.info(
        f"Hub'Eau obs_elab : " f"{date_debut} → {date_fin} " f"(batch {batch_id})"
    )

    nb_lots = (len(stations) + TAILLE_LOT_CODE_ENTITE - 1) // TAILLE_LOT_CODE_ENTITE

    batch = []
    total_insere = 0
    nb_sans_station = 0

    for index in range(
        0,
        len(stations),
        TAILLE_LOT_CODE_ENTITE,
    ):
        lot = stations[index : index + TAILLE_LOT_CODE_ENTITE]

        numero_lot = index // TAILLE_LOT_CODE_ENTITE + 1

        params = {
            "code_entite": ",".join(lot),
            "grandeur_hydro_elab": HUBEAU_GRANDEUR,
            "date_debut_obs_elab": date_debut.strftime("%Y-%m-%d"),
            "date_fin_obs_elab": date_fin.strftime("%Y-%m-%d"),
        }

        libelle = f"obs_elab — lot " f"{numero_lot}/{nb_lots}"

        for lignes_page, _, _ in iterer_pages_hubeau(
            HUBEAU_OBS_URL,
            params,
            libelle,
        ):
            for observation in lignes_page:
                ligne = extraire_colonnes(
                    observation,
                    HUBEAU_OBS_COLONNES,
                )

                if ligne["code_station"] is None:
                    nb_sans_station += 1
                    continue

                batch.append(ligne)

            if len(batch) >= TAILLE_BATCH:
                total_insere += charger_dans_bigquery(
                    data=batch,
                    table_id=HUBEAU_OBS_TABLE_ID,
                    colonne_date="date_obs_elab",
                    batch_id=batch_id,
                )

                batch = []

    total_insere += charger_dans_bigquery(
        data=batch,
        table_id=HUBEAU_OBS_TABLE_ID,
        colonne_date="date_obs_elab",
        batch_id=batch_id,
    )

    return total_insere, nb_sans_station


def ingest_hubeau_obs_elab(
    batch_id: str,
) -> None:
    """Orchestre l'ingestion Hub'Eau obs_elab et annule le batch en cas d'erreur."""
    try:
        total_insere, nb_sans_station = executer_ingestion_hubeau_obs(batch_id)

        logger.info(
            f"Hub'Eau obs_elab terminé : "
            f"{total_insere} nouvelles lignes, "
            f"{nb_sans_station} observations ignorées "
            f"(code_station manquant)."
        )

    except Exception:
        logger.exception(
            f"ERREUR pendant l'ingestion Hub'Eau " f"obs_elab (batch {batch_id})."
        )

        annuler_le_lot(
            HUBEAU_OBS_TABLE_ID,
            batch_id,
        )

        raise
