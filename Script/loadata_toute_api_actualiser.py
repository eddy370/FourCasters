"""
Ingestion des données Open-Meteo et Hub'Eau dans BigQuery.

Open-Meteo :
- historique météo quotidien depuis 2000 ;
- mises à jour incrémentales avec MERGE.

Hub'Eau :
- référentiel stations : chargement complet initial,
  puis actualisation périodique du référentiel ;
- observations élaborées : historique depuis 2000,
  puis mises à jour incrémentales avec MERGE.
"""

import hashlib
import json
import time
import traceback
from datetime import date, datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()

client = bigquery.Client()


PROJECT_ID = "projet-les-fourcasters"

# Les journées les plus récentes sont encore provisoires chez les deux sources.
RECUL_JOURS = 7

TAILLE_BATCH = 5000

TAILLE_PAGE_API = 1000

TIMEOUT_HTTP = 60

# Absorbent les erreurs réseau transitoires (ex. ReadTimeout) sur les
# appels Hub'Eau, sans interrompre toute l'ingestion pour un aléa réseau.
MAX_TENTATIVES_RESEAU = 5
PAUSE_RESEAU_SECONDES = 10


OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"

REFERENTIEL_TABLE_ID = "projet-les-fourcasters.dbt_dev.referentiel_geographique"
METEO_TABLE_ID = "projet-les-fourcasters.raw_openmeteo.meteo_journaliere_raw"

METEO_DATE_DEBUT_PAR_DEFAUT = "2026-08-01"

# Nombre de jours déjà chargés qu'on redemande à chaque exécution
# pour récupérer d'éventuelles corrections ERA5.
METEO_FENETRE_REVISION_JOURS = 35

MAX_RATE_LIMIT_RETRIES = 6

# La fenêtre de rate limit Open-Meteo est à la minute.
PAUSE_RATE_LIMIT_SECONDES = 61

PAUSE_ENTRE_COMMUNES_SECONDES = 3

# Si on ajoute une variable ici, on ajoute aussi la colonne à la table :
# ALTER TABLE `<METEO_TABLE_ID>` ADD COLUMN nouvelle_variable FLOAT64;
# CREATE TABLE IF NOT EXISTS ne modifie pas une table existante,
# et le MERGE échouerait sur T.nouvelle_variable.
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

METEO_CLES = ["code_INSEE", "date"]

# Colonnes hors mesures météo. Commune, Region, Departement et
# Numero_Departement n'y figurent pas : ce sont des attributs de
# code_INSEE, rejoints en staging dbt depuis le référentiel.
METEO_COLONNES_BASE = [
    "Latitude",
    "Longitude",
    "code_INSEE",
    "date",
    "row_hash",
    "inserted_at",
]


HUBEAU_STATIONS_URL = (
    "https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations"
)

HUBEAU_OBS_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

HUBEAU_STATIONS_TABLE_ID = (
    "projet-les-fourcasters.raw_hubeau.stations_hydrometriques"
)

HUBEAU_OBS_TABLE_ID = (
    "projet-les-fourcasters.raw_hubeau."
    "observations_hydrometriques_elaborees"
)

HUBEAU_DATE_DEBUT_PAR_DEFAUT = "2020-01-01"

HUBEAU_FENETRE_REVISION_JOURS = 35

# HIXnJ : hauteur instantanée maximale journalière.
HUBEAU_GRANDEUR = "HIXnJ"

# grandeur_hydro_elab fait partie de la clé :
# deux grandeurs différentes peuvent exister
# pour la même station le même jour.
HUBEAU_CLES = [
    "code_station",
    "date_obs_elab",
    "grandeur_hydro_elab",
]

HUBEAU_COLONNES_BASE = [
    "code_station",
    "date_obs_elab",
    "grandeur_hydro_elab",
    "row_hash",
    "inserted_at",
]

HUBEAU_COLONNES_VALEURS = [
    "code_site",
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


# --- Sélection des stations pertinentes + backfill historique HIXnJ ---
# Règle de sélection des stations validée séparément (cf. discussion) :
# toutes les stations à <= 15 km d'au moins une des 360 communes du
# référentiel, complétées par la station la plus proche pour toute
# commune sans aucune station à <= 15 km.
STATIONS_DISTANCE_MAX_METRES = 15000

STATIONS_PERTINENTES_ATTENDU = 3011

# Découpage prudent des codes station envoyés à `code_entite` : ce n'est
# pas une limite officielle documentée par Hub'Eau, juste une marge de
# sécurité sur la longueur de l'URL (comptage comme backfill).
TAILLE_LOT_CODE_ENTITE = 200

HUBEAU_BACKFILL_DATE_DEBUT = "2000-01-01"
HUBEAU_BACKFILL_DATE_FIN = "2026-09-01"

# Taille de flush pendant le backfill mensuel : plus grande que
# TAILLE_BATCH (réservée à l'incrémental et à Open-Meteo) pour réduire
# le nombre de MERGE sur tout l'historique, tout en restant bornée —
# jamais tout un mois n'est accumulé en mémoire.
TAILLE_BATCH_BACKFILL = 20_000

# Table de contrôle du backfill : un mois n'y est inséré qu'une fois
# tous ses lots de stations entièrement récupérés et chargés. Sert de
# mécanisme de reprise après crash (MAX(date_obs_elab) n'est pas fiable
# ici car la pagination Hub'Eau n'est pas chronologique).
HUBEAU_BACKFILL_CONTROLE_TABLE_ID = (
    "projet-les-fourcasters.raw_hubeau.controle_backfill_obs_elab"
)


def calculer_hash(row: dict) -> str:
    """Calcule l'empreinte SHA-256 du contenu d'une ligne.

    Args:
        row (dict): la ligne, sans row_hash ni inserted_at.

    Returns:
        str: empreinte hexadécimale de 64 caractères.
    """
    row_str = json.dumps(
        row,
        sort_keys=True,
        ensure_ascii=False,
    )

    return hashlib.sha256(
        row_str.encode()
    ).hexdigest()


def enrichir_lignes(data: list) -> list:
    """Ajoute row_hash et inserted_at à chaque ligne.

    Le hash est calculé avant inserted_at : sinon deux exécutions
    des mêmes données produiraient des empreintes différentes.

    Args:
        data (list): lignes issues de l'API.

    Returns:
        list: les mêmes lignes, enrichies.
    """
    now = datetime.now(
        timezone.utc
    ).isoformat()

    enriched = []

    for row in data:
        row_copy = dict(row)

        row_copy["row_hash"] = calculer_hash(row)

        row_copy["inserted_at"] = now

        enriched.append(row_copy)

    return enriched


def requete_hubeau_avec_retry(
    url: str,
    params: dict | None,
    libelle: str,
) -> requests.Response:
    """Effectue un GET Hub'Eau avec retry/backoff sur erreur réseau.

    Args:
        url (str): endpoint ou URL `next` Hub'Eau.
        params (dict | None): paramètres de la requête (None si déjà dans l'URL).
        libelle (str): description de l'appel pour les messages.

    Returns:
        requests.Response: la réponse HTTP obtenue.

    Raises:
        RuntimeError: après épuisement des tentatives réseau.
    """
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
                    f"Hub'Eau {libelle} : "
                    f"échec réseau après {tentative} tentatives "
                    f"({erreur})."
                ) from erreur

            pause = PAUSE_RESEAU_SECONDES * tentative

            print(
                f"⚠️ Hub'Eau {libelle} : "
                f"erreur réseau ({erreur}). "
                f"Nouvelle tentative dans {pause}s "
                f"({tentative}/{MAX_TENTATIVES_RESEAU})."
            )

            time.sleep(pause)


def iterer_pages_hubeau(url: str, params: dict, libelle: str):
    """Parcourt toutes les pages d'une API Hub'Eau.

    La première requête utilise les paramètres fournis. Les suivantes
    utilisent directement l'URL `next` renvoyée par Hub'Eau, qui contient
    notamment le curseur nécessaire à la pagination.

    Args:
        url (str): endpoint Hub'Eau.
        params (dict): paramètres de la première requête.
        libelle (str): nom de la source pour les messages.

    Yields:
        tuple: lignes de la page, nombre cumulé récupéré, total attendu.

    Raises:
        RuntimeError: si l'API renvoie une erreur ou si la récupération
            finale est incomplète.
    """
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
        except ValueError:
            data = {}

        if response.status_code not in (200, 206) or data.get("error"):
            raise RuntimeError(
                f"Erreur Hub'Eau {libelle} : "
                f"{data.get('message', response.status_code)}"
            )

        if total_attendu is None:
            total_attendu = data.get("count")

        lignes = data.get("data", [])
        total_recu += len(lignes)

        print(
            f"Hub'Eau {libelle} — page {numero_page} : "
            f"{len(lignes)} lignes "
            f"({total_recu}/{total_attendu or '?'})"
        )

        yield lignes, total_recu, total_attendu

        url_courante = data.get("next")

        # L'URL `next` contient déjà le cursor, size et les filtres.
        params_courants = None

        numero_page += 1

    if total_attendu is not None and total_recu != total_attendu:
        raise RuntimeError(
            f"Hub'Eau {libelle} incomplet : "
            f"{total_recu} lignes récupérées "
            f"sur {total_attendu} attendues."
        )

    print(
        f"✅ Hub'Eau {libelle} — récupération complète : "
        f"{total_recu} lignes."
    )


def extraire_colonnes(
    source: dict,
    colonnes: list,
) -> dict:
    """Ne garde que les colonnes attendues.

    Args:
        source (dict): enregistrement brut API.
        colonnes (list): colonnes à conserver.

    Returns:
        dict: ligne prête à charger.
    """
    return {
        colonne: source.get(colonne)
        for colonne in colonnes
    }


def charger_dans_bigquery(
    data: list,
    table_id: str,
    cles: list,
    colonnes_base: list,
    colonnes_valeurs: list,
    filtre_partition: str | None = None,
) -> None:
    """Charge un lot dans une table RAW via MERGE.

    Les lignes sont écrites dans une table temporaire,
    puis fusionnées avec la table cible.

    Args:
        data (list): lignes à charger.
        table_id (str): table RAW cible.
        cles (list): clé naturelle.
        colonnes_base (list): colonnes de base.
        colonnes_valeurs (list): colonnes métier.
        filtre_partition (str | None): condition SQL supplémentaire sur
            T (ex. bornes de partition), pour aider le pruning BigQuery
            sur de gros backfills périodiques. Sans effet sur les
            correspondances trouvées tant que S ne contient que des
            lignes respectant déjà cette condition.
    """
    if not data:
        return

    data_enrichie = enrichir_lignes(data)

    dataset = table_id.split(".")[1]

    table_temp = (
        f"{PROJECT_ID}.{dataset}."
        f"temp_{int(time.time() * 1000)}"
    )

    # Le schéma est copié de la table cible pour éviter
    # qu'une colonne entièrement NULL soit mal typée.
    load_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=client.get_table(table_id).schema,
    )

    try:
        client.load_table_from_json(
            data_enrichie,
            table_temp,
            job_config=load_config,
        ).result()

        condition_jointure = "\n        AND ".join(
            [
                f"T.{cle} = S.{cle}"
                for cle in cles
            ]
        )

        if filtre_partition:
            condition_jointure += f"\n        AND {filtre_partition}"

        colonnes_update = [
            colonne
            for colonne
            in colonnes_base + colonnes_valeurs
            if colonne not in cles
        ]

        set_update = ",\n            ".join(
            [
                f"T.{colonne} = S.{colonne}"
                for colonne in colonnes_update
            ]
        )

        colonnes_insert = (
            colonnes_base
            + colonnes_valeurs
        )

        noms_insert = ", ".join(
            colonnes_insert
        )

        valeurs_insert = ", ".join(
            [
                f"S.{colonne}"
                for colonne in colonnes_insert
            ]
        )

        # IS DISTINCT FROM fonctionne également si un ancien
        # row_hash est NULL.
        merge_query = f"""
            MERGE `{table_id}` AS T
            USING `{table_temp}` AS S

            ON {condition_jointure}

            WHEN MATCHED
                AND T.row_hash IS DISTINCT FROM S.row_hash
            THEN UPDATE SET
                {set_update}

            WHEN NOT MATCHED THEN
            INSERT (
                {noms_insert}
            )
            VALUES (
                {valeurs_insert}
            )
        """

        client.query(
            merge_query
        ).result()

    finally:
        # Toujours nettoyer la table temporaire, même si le LOAD ou le
        # MERGE a échoué. L'erreur de suppression est isolée pour ne
        # pas masquer l'erreur d'origine du LOAD/MERGE.
        try:
            client.delete_table(
                table_temp,
                not_found_ok=True,
            )
        except Exception:
            print(
                f"⚠️ Échec de la suppression de la table "
                f"temporaire {table_temp}."
            )

    print(
        f"→ Lot traité : "
        f"{len(data_enrichie)} lignes "
        f"comparées / insérées / mises à jour."
    )


def charger_meteo(
    data: list,
) -> None:
    """Charge un lot météo dans la RAW Open-Meteo."""
    charger_dans_bigquery(
        data=data,
        table_id=METEO_TABLE_ID,
        cles=METEO_CLES,
        colonnes_base=METEO_COLONNES_BASE,
        colonnes_valeurs=METEO_VARIABLES,
    )


def creer_table_meteo() -> None:
    """Crée la table RAW météo si nécessaire."""
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{METEO_TABLE_ID}` (
            Latitude FLOAT64,
            Longitude FLOAT64,
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
        """
    ).result()


def lire_communes() -> list:
    """Récupère les communes et leur dernière date chargée."""
    return list(
        client.query(
            f"""
            SELECT
                v.Commune,
                v.Latitude,
                v.Longitude,
                v.code_INSEE,
                d.max_date

            FROM `{REFERENTIEL_TABLE_ID}` v

            LEFT JOIN (
                SELECT
                    code_INSEE,
                    MAX(date) AS max_date
                FROM `{METEO_TABLE_ID}`
                GROUP BY code_INSEE
            ) d

            ON v.code_INSEE = d.code_INSEE
            """
        ).result()
    )


def calculer_date_debut_meteo(
    derniere_date,
    date_fin: date,
) -> str:
    """Détermine la date de début Open-Meteo."""
    date_debut_defaut = datetime.strptime(
        METEO_DATE_DEBUT_PAR_DEFAUT,
        "%Y-%m-%d",
    ).date()

    if derniere_date is None:
        return date_debut_defaut.strftime(
            "%Y-%m-%d"
        )

    date_revision = (
        date_fin
        - timedelta(
            days=METEO_FENETRE_REVISION_JOURS
        )
    )

    return max(
        date_revision,
        date_debut_defaut,
    ).strftime("%Y-%m-%d")


def appeler_openmeteo(
    params: dict,
    commune: str,
    rate_limit_count: int,
) -> tuple:
    """Appelle Open-Meteo en gérant le rate limit."""
    while True:

        response = requests.get(
            OPENMETEO_URL,
            params=params,
            timeout=TIMEOUT_HTTP,
        )

        try:
            data = response.json()

        except ValueError:
            data = {}

        rate_limited = (
            response.status_code == 429
            or "limit"
            in str(
                data.get("reason", "")
            ).lower()
        )

        if rate_limited:

            rate_limit_count += 1

            if (
                rate_limit_count
                >= MAX_RATE_LIMIT_RETRIES
            ):
                raise RuntimeError(
                    "Trop de rate limits "
                    "Open-Meteo consécutifs."
                )

            print(
                f"🚦 Rate limit. "
                f"Pause "
                f"{PAUSE_RATE_LIMIT_SECONDES}s "
                f"({rate_limit_count}/"
                f"{MAX_RATE_LIMIT_RETRIES})"
            )

            time.sleep(
                PAUSE_RATE_LIMIT_SECONDES
            )

            continue

        if (
            response.status_code != 200
            or data.get("error")
        ):
            raise RuntimeError(
                f"Erreur Open-Meteo "
                f"pour {commune} : "
                f"{data.get('reason', response.status_code)}"
            )

        return data, 0


def construire_lignes_meteo(
    data: dict,
    ville,
) -> list:
    """Transforme Open-Meteo en lignes journalières."""
    daily = data.get(
        "daily",
        {},
    )

    temperatures = (
        daily.get(
            "temperature_2m_mean"
        )
        or []
    )

    lignes = []

    for i, jour in enumerate(
        daily.get(
            "time",
            [],
        )
    ):

        # Journée incomplète : elle sera retentée
        # lors de la prochaine exécution.
        if (
            i >= len(temperatures)
            or temperatures[i] is None
        ):
            continue

        row = {
            "Latitude": (
                float(ville["Latitude"])
                if ville["Latitude"] is not None
                else None
            ),
            "Longitude": (
                float(ville["Longitude"])
                if ville["Longitude"] is not None
                else None
            ),
            "code_INSEE": str(
                ville["code_INSEE"]
            ),
            "date": jour,
        }

        for variable in METEO_VARIABLES:

            valeurs = daily.get(
                variable
            )

            row[variable] = (
                valeurs[i]
                if (
                    valeurs is not None
                    and i < len(valeurs)
                )
                else None
            )

        lignes.append(row)

    return lignes


def ingest_openmeteo() -> None:
    """Ingère les données météo quotidiennes."""
    batch = []

    try:
        creer_table_meteo()

        date_fin = (
            date.today()
            - timedelta(
                days=RECUL_JOURS
            )
        )

        date_fin_str = (
            date_fin.strftime(
                "%Y-%m-%d"
            )
        )

        villes = lire_communes()

        print(
            f"Communes à traiter : "
            f"{len(villes)}"
        )

        rate_limit_count = 0

        for index, ville in enumerate(
            villes,
            start=1,
        ):

            commune = ville["Commune"]

            if ville["code_INSEE"] is None:

                print(
                    f"⚠️ code_INSEE manquant "
                    f"pour {commune!r}, "
                    f"commune ignorée."
                )

                continue

            date_debut = (
                calculer_date_debut_meteo(
                    ville["max_date"],
                    date_fin,
                )
            )

            if date_debut > date_fin_str:
                continue

            print(
                f"[{index}/{len(villes)}] "
                f"{commune} : "
                f"{date_debut} → "
                f"{date_fin_str}"
            )

            params = {
                "latitude": ville["Latitude"],
                "longitude": ville["Longitude"],
                "start_date": date_debut,
                "end_date": date_fin_str,
                "daily": ",".join(
                    METEO_VARIABLES
                ),
                "timezone": "Europe/Paris",
                "models": "era5_seamless",
            }

            (
                data,
                rate_limit_count,
            ) = appeler_openmeteo(
                params,
                commune,
                rate_limit_count,
            )

            batch.extend(
                construire_lignes_meteo(
                    data,
                    ville,
                )
            )

            if (
                len(batch)
                >= TAILLE_BATCH
            ):
                charger_meteo(
                    batch
                )

                batch = []

            time.sleep(
                PAUSE_ENTRE_COMMUNES_SECONDES
            )

        charger_meteo(
            batch
        )

        batch = []

        print(
            "✅ Ingestion Open-Meteo terminée."
        )

    except Exception:

        if batch:

            print(
                "⚠️ Sauvegarde du batch "
                "avant arrêt..."
            )

            try:
                charger_meteo(
                    batch
                )

            except Exception:

                print(
                    "❌ Échec de la sauvegarde "
                    "du dernier batch."
                )

        print(
            "🔥 ERREUR pendant "
            "l'ingestion Open-Meteo :"
        )

        print(
            traceback.format_exc()
        )

        raise


def ingest_hubeau_stations() -> None:
    """Ingère le référentiel complet des stations hydrométriques.

    Le référentiel représente l'état courant des stations :
    la table RAW est donc remplacée intégralement à chaque exécution.
    """
    client.query(
        f"""
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
        """
    ).result()

    stations = []

    for lignes_page, _, _ in iterer_pages_hubeau(
        HUBEAU_STATIONS_URL,
        {},
        "stations",
    ):
        stations.extend(lignes_page)

    print(f"Stations hydrométriques récupérées : {len(stations)}")

    lignes = [
        extraire_colonnes(station, HUBEAU_STATIONS_COLONNES)
        for station in stations
    ]

    if not lignes:
        print("Aucune station Hub'Eau à charger.")
        return

    load_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=client.get_table(HUBEAU_STATIONS_TABLE_ID).schema,
    )

    client.load_table_from_json(
        lignes,
        HUBEAU_STATIONS_TABLE_ID,
        job_config=load_config,
    ).result()

    print(f"✅ Hub'Eau stations chargé : {len(lignes)} stations.")


def creer_table_hubeau_obs() -> None:
    """Crée et met à niveau la table RAW Hub'Eau."""
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS
        `{HUBEAU_OBS_TABLE_ID}` (
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
            inserted_at TIMESTAMP
        )
        PARTITION BY date_obs_elab
        CLUSTER BY code_station
        """
    ).result()

    client.query(
        f"""
        ALTER TABLE `{HUBEAU_OBS_TABLE_ID}`
        ADD COLUMN IF NOT EXISTS
        row_hash STRING
        """
    ).result()

    client.query(
        f"""
        ALTER TABLE `{HUBEAU_OBS_TABLE_ID}`
        ADD COLUMN IF NOT EXISTS
        inserted_at TIMESTAMP
        """
    ).result()


def calculer_date_debut_hubeau(
    derniere_date,
) -> date:
    """Détermine la date de début Hub'Eau."""
    date_debut_defaut = datetime.strptime(
        HUBEAU_DATE_DEBUT_PAR_DEFAUT,
        "%Y-%m-%d",
    ).date()

    if derniere_date is None:
        return date_debut_defaut

    date_revision = (
        derniere_date
        - timedelta(
            days=HUBEAU_FENETRE_REVISION_JOURS
        )
    )

    return max(
        date_revision,
        date_debut_defaut,
    )


def ingest_hubeau_obs_elab() -> None:
    """Ingère les observations hydrométriques élaborées.

    Les pages Hub'Eau sont parcourues avec leur curseur. Les observations
    sont chargées progressivement dans BigQuery afin de ne jamais conserver
    l'ensemble de l'historique en mémoire.

    Une fenêtre récente est redemandée à chaque exécution afin que le MERGE
    puisse intégrer les éventuelles corrections de la source.
    """
    creer_table_hubeau_obs()

    stations = recuperer_stations_pertinentes()

    verifier_nombre_stations(stations)

    date_fin = date.today() - timedelta(days=RECUL_JOURS)
    date_debut = date_fin - timedelta(days=HUBEAU_FENETRE_REVISION_JOURS)

    print(f"Hub'Eau obs_elab : {date_debut} → {date_fin}")

    filtre_partition = (
        f"T.date_obs_elab BETWEEN "
        f"DATE('{date_debut.isoformat()}') "
        f"AND DATE('{date_fin.isoformat()}')"
    )

    colonnes_api = [
        colonne
        for colonne in HUBEAU_COLONNES_BASE + HUBEAU_COLONNES_VALEURS
        if colonne not in ("row_hash", "inserted_at")
    ]

    nb_lots = (
        len(stations) + TAILLE_LOT_CODE_ENTITE - 1
    ) // TAILLE_LOT_CODE_ENTITE

    batch = []
    total_charge = 0
    nb_sans_station = 0

    try:
        for index in range(0, len(stations), TAILLE_LOT_CODE_ENTITE):
            lot = stations[index:index + TAILLE_LOT_CODE_ENTITE]
            numero_lot = index // TAILLE_LOT_CODE_ENTITE + 1

            params = {
                "code_entite": ",".join(lot),
                "grandeur_hydro_elab": HUBEAU_GRANDEUR,
                "date_debut_obs_elab": date_debut.strftime("%Y-%m-%d"),
                "date_fin_obs_elab": date_fin.strftime("%Y-%m-%d"),
            }

            libelle = f"obs_elab — lot {numero_lot}/{nb_lots}"

            for lignes_page, _, total_attendu in iterer_pages_hubeau(
                HUBEAU_OBS_URL,
                params,
                libelle,
            ):
                for observation in lignes_page:
                    ligne = extraire_colonnes(
                        observation,
                        colonnes_api,
                    )

                    # code_station NULL ne matcherait jamais la clé de
                    # jointure du MERGE (NULL <> NULL) : on l'exclut pour
                    # éviter des doublons insérés à chaque exécution.
                    if ligne["code_station"] is None:
                        nb_sans_station += 1
                        continue

                    batch.append(ligne)

                if len(batch) >= TAILLE_BATCH:
                    charger_dans_bigquery(
                        data=batch,
                        table_id=HUBEAU_OBS_TABLE_ID,
                        cles=HUBEAU_CLES,
                        colonnes_base=HUBEAU_COLONNES_BASE,
                        colonnes_valeurs=HUBEAU_COLONNES_VALEURS,
                        filtre_partition=filtre_partition,
                    )

                    total_charge += len(batch)

                    print(
                        f"Hub'Eau obs_elab — BigQuery : "
                        f"{total_charge}/{total_attendu or '?'} lignes chargées."
                    )

                    batch = []

        if batch:
            charger_dans_bigquery(
                data=batch,
                table_id=HUBEAU_OBS_TABLE_ID,
                cles=HUBEAU_CLES,
                colonnes_base=HUBEAU_COLONNES_BASE,
                colonnes_valeurs=HUBEAU_COLONNES_VALEURS,
                filtre_partition=filtre_partition,
            )

            total_charge += len(batch)

        print(
            f"✅ Hub'Eau obs_elab terminé : "
            f"{total_charge} lignes chargées, "
            f"{nb_sans_station} observations ignorées "
            f"(code_station manquant)."
        )

    except Exception:

        if batch:

            print(
                "⚠️ Sauvegarde du batch "
                "avant arrêt..."
            )

            try:
                charger_dans_bigquery(
                    data=batch,
                    table_id=HUBEAU_OBS_TABLE_ID,
                    cles=HUBEAU_CLES,
                    colonnes_base=HUBEAU_COLONNES_BASE,
                    colonnes_valeurs=HUBEAU_COLONNES_VALEURS,
                    filtre_partition=filtre_partition,
                )

            except Exception:

                print(
                    "❌ Échec de la sauvegarde "
                    "du dernier batch."
                )

        print(
            "🔥 ERREUR pendant "
            "l'ingestion Hub'Eau obs_elab :"
        )

        print(
            traceback.format_exc()
        )

        raise

def recuperer_stations_pertinentes() -> list:
    """Calcule les stations Hub'Eau pertinentes pour FourCasters.

    Règle validée : toutes les stations à <= 15 km d'au moins une des 360
    communes du référentiel, complétées, pour toute commune sans aucune
    station à <= 15 km, par sa station la plus proche.

    Lecture seule : aucune donnée BigQuery n'est modifiée.

    Returns:
        list: code_station triés, sans doublon.
    """
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
                ST_GEOGPOINT(longitude_station, latitude_station) AS geo
            FROM `{HUBEAU_STATIONS_TABLE_ID}`
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

        SELECT code_station FROM stations_proches
        UNION DISTINCT
        SELECT code_station FROM station_plus_proche
    """

    lignes = client.query(query).result()

    return sorted({ligne["code_station"] for ligne in lignes})


def verifier_nombre_stations(stations: list) -> None:
    """Vérifie que le nombre de stations pertinentes est celui attendu.

    Args:
        stations (list): code_station issus de recuperer_stations_pertinentes.

    Raises:
        RuntimeError: si le nombre diffère de STATIONS_PERTINENTES_ATTENDU.
    """
    if len(stations) != STATIONS_PERTINENTES_ATTENDU:
        raise RuntimeError(
            f"Nombre de stations pertinentes inattendu : "
            f"{len(stations)} au lieu de "
            f"{STATIONS_PERTINENTES_ATTENDU} attendues."
        )


def generer_periodes_mensuelles(
    date_debut: date,
    date_fin: date,
) -> list:
    """Découpe [date_debut, date_fin] en périodes calendaires mensuelles.

    Le dernier mois est tronqué à date_fin si celle-ci tombe avant la
    fin du mois calendaire.

    Args:
        date_debut (date): premier jour à couvrir.
        date_fin (date): dernier jour à couvrir.

    Returns:
        list: tuples (debut_mois, fin_mois), bornes incluses.
    """
    periodes = []

    annee, mois = date_debut.year, date_debut.month

    while date(annee, mois, 1) <= date_fin:

        debut_mois = date(annee, mois, 1)

        if mois == 12:
            fin_mois_calendaire = date(annee, 12, 31)
            annee, mois = annee + 1, 1
        else:
            fin_mois_calendaire = date(annee, mois + 1, 1) - timedelta(days=1)
            mois += 1

        periodes.append(
            (debut_mois, min(fin_mois_calendaire, date_fin))
        )

    return periodes


def creer_table_controle_backfill() -> None:
    """Crée la table de contrôle du backfill si nécessaire."""
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS
        `{HUBEAU_BACKFILL_CONTROLE_TABLE_ID}` (
            grandeur_hydro_elab STRING,
            mois DATE,
            nb_lignes INT64,
            termine_le TIMESTAMP
        )
        """
    ).result()


def lire_mois_termines(grandeur: str) -> set:
    """Récupère les mois déjà marqués terminés pour une grandeur.

    Args:
        grandeur (str): code de la grandeur (ex. HIXnJ).

    Returns:
        set: dates (1er du mois) déjà terminées.
    """
    lignes = client.query(
        f"""
        SELECT mois
        FROM `{HUBEAU_BACKFILL_CONTROLE_TABLE_ID}`
        WHERE grandeur_hydro_elab = '{grandeur}'
        """
    ).result()

    return {ligne["mois"] for ligne in lignes}


def marquer_mois_termine(
    grandeur: str,
    mois_debut: date,
    nb_lignes: int,
) -> None:
    """Enregistre un mois terminé de manière idempotente."""

    client.query(
        f"""
        MERGE `{HUBEAU_BACKFILL_CONTROLE_TABLE_ID}` AS T

        USING (
            SELECT
                '{grandeur}' AS grandeur_hydro_elab,
                DATE('{mois_debut.isoformat()}') AS mois,
                {nb_lignes} AS nb_lignes,
                CURRENT_TIMESTAMP() AS termine_le
        ) AS S

        ON T.grandeur_hydro_elab = S.grandeur_hydro_elab
        AND T.mois = S.mois

        WHEN MATCHED THEN
            UPDATE SET
                nb_lignes = S.nb_lignes,
                termine_le = S.termine_le

        WHEN NOT MATCHED THEN
            INSERT (
                grandeur_hydro_elab,
                mois,
                nb_lignes,
                termine_le
            )
            VALUES (
                S.grandeur_hydro_elab,
                S.mois,
                S.nb_lignes,
                S.termine_le
            )
        """
    ).result()


def traiter_mois_hubeau_obs(
    stations: list,
    mois_debut: date,
    mois_fin: date,
) -> int:
    """Récupère et charge un mois d'observations HIXnJ, progressivement.

    Parcourt les stations par lots de TAILLE_LOT_CODE_ENTITE, et chaque
    lot page par page via iterer_pages_hubeau (curseur + retry déjà en
    place), sans jamais accumuler tout le mois en mémoire : les lignes
    sont chargées dans BigQuery par batches de TAILLE_BATCH_BACKFILL au
    fil de la pagination.

    Ne retourne qu'une fois tous les lots et toutes leurs pages
    entièrement parcourus et chargés (y compris le dernier batch
    partiel) : c'est cette condition que l'appelant utilise pour savoir
    s'il peut marquer le mois terminé.

    Args:
        stations (list): les 3011 code_station pertinents.
        mois_debut (date): premier jour du mois.
        mois_fin (date): dernier jour du mois (borne incluse).

    Returns:
        int: nombre de lignes chargées pour ce mois.
    """
    colonnes_api = [
        colonne
        for colonne in HUBEAU_COLONNES_BASE + HUBEAU_COLONNES_VALEURS
        if colonne not in ("row_hash", "inserted_at")
    ]

    nb_lots = (
        len(stations) + TAILLE_LOT_CODE_ENTITE - 1
    ) // TAILLE_LOT_CODE_ENTITE

    filtre_partition = (
        f"T.date_obs_elab BETWEEN "
        f"DATE('{mois_debut.isoformat()}') "
        f"AND DATE('{mois_fin.isoformat()}')"
    )

    batch = []
    total_charge = 0
    nb_sans_station = 0

    for index in range(0, len(stations), TAILLE_LOT_CODE_ENTITE):
        lot = stations[index:index + TAILLE_LOT_CODE_ENTITE]
        numero_lot = index // TAILLE_LOT_CODE_ENTITE + 1

        params = {
            "code_entite": ",".join(lot),
            "grandeur_hydro_elab": HUBEAU_GRANDEUR,
            "date_debut_obs_elab": mois_debut.strftime("%Y-%m-%d"),
            "date_fin_obs_elab": mois_fin.strftime("%Y-%m-%d"),
        }

        libelle = (
            f"backfill {mois_debut:%Y-%m} — lot {numero_lot}/{nb_lots}"
        )

        for lignes_page, _, _ in iterer_pages_hubeau(
            HUBEAU_OBS_URL,
            params,
            libelle,
        ):
            for observation in lignes_page:
                ligne = extraire_colonnes(
                    observation,
                    colonnes_api,
                )

                # code_station NULL ne matcherait jamais la clé de
                # jointure du MERGE (NULL <> NULL) : on l'exclut pour
                # éviter des doublons insérés à chaque exécution.
                if ligne["code_station"] is None:
                    nb_sans_station += 1
                    continue

                batch.append(ligne)

            if len(batch) >= TAILLE_BATCH_BACKFILL:
                charger_dans_bigquery(
                    data=batch,
                    table_id=HUBEAU_OBS_TABLE_ID,
                    cles=HUBEAU_CLES,
                    colonnes_base=HUBEAU_COLONNES_BASE,
                    colonnes_valeurs=HUBEAU_COLONNES_VALEURS,
                    filtre_partition=filtre_partition,
                )

                total_charge += len(batch)

                batch = []

    if batch:
        charger_dans_bigquery(
            data=batch,
            table_id=HUBEAU_OBS_TABLE_ID,
            cles=HUBEAU_CLES,
            colonnes_base=HUBEAU_COLONNES_BASE,
            colonnes_valeurs=HUBEAU_COLONNES_VALEURS,
            filtre_partition=filtre_partition,
        )

        total_charge += len(batch)

    print(
        f"{mois_debut:%Y-%m} : {total_charge} lignes chargées, "
        f"{nb_sans_station} observations ignorées "
        f"(code_station manquant)."
    )

    return total_charge


def backfill_hubeau_obs_elab_hixnj() -> None:
    """Backfill historique HIXnJ pour les stations pertinentes.

    Traite le backfill mois par mois, uniquement sur les 3011 stations
    pertinentes (filtre code_entite). Un mois n'est marqué terminé dans
    la table de contrôle qu'après chargement complet et réussi de tous
    ses lots de stations : relancer cette fonction après un crash saute
    les mois déjà terminés et rejoue intégralement le mois interrompu,
    sans risque de doublon (MERGE idempotent sur row_hash).

    Fonction séparée de l'incrémental ingest_hubeau_obs_elab() : elle
    n'utilise ni sa fenêtre de révision de 35 jours, ni son découpage
    par nombre de lignes fixe.

    N'est pas appelée automatiquement par main().
    """
    creer_table_hubeau_obs()
    creer_table_controle_backfill()

    stations = recuperer_stations_pertinentes()

    verifier_nombre_stations(stations)

    print(f"Stations pertinentes récupérées : {len(stations)}")

    date_debut = datetime.strptime(
        HUBEAU_BACKFILL_DATE_DEBUT,
        "%Y-%m-%d",
    ).date()

    date_fin = datetime.strptime(
        HUBEAU_BACKFILL_DATE_FIN,
        "%Y-%m-%d",
    ).date()

    periodes = generer_periodes_mensuelles(date_debut, date_fin)
    mois_termines = lire_mois_termines(HUBEAU_GRANDEUR)

    print(
        f"Backfill {HUBEAU_GRANDEUR} : {len(periodes)} mois au total, "
        f"{len(mois_termines)} déjà terminés."
    )

    for mois_debut, mois_fin in periodes:

        if mois_debut in mois_termines:
            print(f"→ {mois_debut:%Y-%m} déjà terminé, ignoré.")
            continue

        print(f"→ {mois_debut:%Y-%m} : traitement...")

        nb_lignes = traiter_mois_hubeau_obs(
            stations,
            mois_debut,
            mois_fin,
        )

        marquer_mois_termine(
            HUBEAU_GRANDEUR,
            mois_debut,
            nb_lignes,
        )

        print(f"✅ {mois_debut:%Y-%m} terminé ({nb_lignes} lignes).")

    print(f"✅ Backfill {HUBEAU_GRANDEUR} terminé.")


def compter_observations_hubeau(stations: list) -> int:
    """Compte les observations HIXnJ Hub'Eau pour une liste de stations.

    Utilise `size=1` afin de ne lire que le champ `count` de chaque lot,
    sans télécharger ni charger la moindre observation.

    Args:
        stations (list): code_station à interroger.

    Returns:
        int: somme des `count` sur tous les lots.
    """
    nb_lots = (
        len(stations) + TAILLE_LOT_CODE_ENTITE - 1
    ) // TAILLE_LOT_CODE_ENTITE

    total = 0

    for index in range(0, len(stations), TAILLE_LOT_CODE_ENTITE):
        lot = stations[index:index + TAILLE_LOT_CODE_ENTITE]
        numero_lot = index // TAILLE_LOT_CODE_ENTITE + 1

        params = {
            "code_entite": ",".join(lot),
            "grandeur_hydro_elab": HUBEAU_GRANDEUR,
            "date_debut_obs_elab": HUBEAU_BACKFILL_DATE_DEBUT,
            "date_fin_obs_elab": HUBEAU_BACKFILL_DATE_FIN,
            "size": 1,
        }

        response = requete_hubeau_avec_retry(
            HUBEAU_OBS_URL,
            params,
            f"comptage obs_elab — lot {numero_lot}/{nb_lots}",
        )

        try:
            data = response.json()
        except ValueError:
            data = {}

        if response.status_code not in (200, 206) or data.get("error"):
            raise RuntimeError(
                f"Erreur Hub'Eau comptage — lot {numero_lot}/{nb_lots} : "
                f"{data.get('message', response.status_code)}"
            )

        count_lot = data.get("count", 0)

        print(
            f"Lot {numero_lot}/{nb_lots} "
            f"({len(lot)} stations) : count = {count_lot}"
        )

        total += count_lot

    print(
        f"✅ {nb_lots} lots interrogés — "
        f"total observations HIXnJ estimé : {total}"
    )

    return total


def tester_count_hubeau_pertinent() -> None:
    """Estime le volume d'observations HIXnJ pour les stations pertinentes.

    Mode comptage uniquement : aucune observation n'est téléchargée ni
    chargée dans BigQuery, et aucune donnée BigQuery n'est modifiée.
    """
    stations = recuperer_stations_pertinentes()

    print(f"Stations pertinentes récupérées : {len(stations)}")

    verifier_nombre_stations(stations)

    compter_observations_hubeau(stations)


def main():
    backfill_hubeau_obs_elab_hixnj()



if __name__ == "__main__":
    main()



