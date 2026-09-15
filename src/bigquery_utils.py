"""
Fonctions génériques BigQuery, partagées par les ingestions Open-Meteo et
Hub'Eau : hash de contenu, enrichissement technique (row_hash, inserted_at,
batch_id), chargement WRITE_APPEND avec déduplication, et rollback par
batch_id en cas d'erreur.

Contient aussi les constantes réellement partagées entre les deux sources
(fenêtre de recul, taille de batch, timeout HTTP, référentiel géographique) :
une seule source de vérité pour ces valeurs.

Les corrections d'une donnée ne remplacent plus la ligne RAW existante :
si le contenu change, le row_hash change et une nouvelle version est ajoutée.
Le staging dbt doit ensuite garder la version la plus récente par clé métier.
"""

import logging
import hashlib
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
client = bigquery.Client()

logger = logging.getLogger(__name__)

# Constantes partagées par Open-Meteo et Hub'Eau.
# Les journées les plus récentes sont encore provisoires chez les deux sources.
RECUL_JOURS = 7
TAILLE_BATCH = 5000
TIMEOUT_HTTP = (5, 60)
REFERENTIEL_TABLE_ID = "projet-les-fourcasters.dbt_dev.referentiel_geographique"


def calculer_hash(row: dict) -> str:
    """Calcule l'empreinte SHA-256 du contenu métier d'une ligne."""
    row_str = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(row_str.encode("utf-8")).hexdigest()


def enrichir_lignes(data: list, batch_id: str) -> list:
    """Ajoute row_hash, inserted_at et batch_id."""
    now = datetime.now(timezone.utc).isoformat()
    enriched = []

    for row in data:
        row_copy = dict(row)
        # Le hash est calculé AVANT les colonnes techniques.
        row_copy["row_hash"] = calculer_hash(row)
        row_copy["inserted_at"] = now
        row_copy["batch_id"] = batch_id
        enriched.append(row_copy)

    return enriched


def extraire_colonnes(source: dict, colonnes: list) -> dict:
    """Ne garde que les colonnes attendues."""
    return {colonne: source.get(colonne) for colonne in colonnes}


def recuperer_hashes_existants(table_id: str, colonne_date: str, data: list) -> set:
    """Récupère les row_hash déjà présents sur la période du batch."""
    dates = [
        ligne[colonne_date] for ligne in data if ligne.get(colonne_date) is not None
    ]

    if not dates:
        raise RuntimeError(f"Aucune date disponible dans {colonne_date}.")

    hashes = [ligne["row_hash"] for ligne in data]

    # On ne lit que la période correspondant au batch.
    query = f"""
        SELECT row_hash
        FROM `{table_id}`
        WHERE {colonne_date} BETWEEN @date_min AND @date_max
        AND row_hash IN UNNEST(@hashes)
    """

    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date_min", "DATE", min(dates)),
            bigquery.ScalarQueryParameter("date_max", "DATE", max(dates)),
            bigquery.ArrayQueryParameter("hashes", "STRING", hashes),
        ]
    )

    resultat = client.query(query, job_config=query_config).result()

    return {ligne["row_hash"] for ligne in resultat}


def inserer_lignes_bigquery(data: list, table_id: str) -> None:
    """Charge un lot de lignes dans BigQuery en WRITE_APPEND."""
    load_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=client.get_table(table_id).schema,
    )
    client.load_table_from_json(data, table_id, job_config=load_config).result()


def charger_dans_bigquery(
    data: list, table_id: str, colonne_date: str, batch_id: str
) -> int:
    """Ajoute uniquement les nouvelles lignes dans une table RAW.

    Le row_hash est un hash de contenu.

    - même contenu déjà présent : le row_hash existe, donc on ne réinsère pas ;
    - contenu corrigé : le row_hash change, donc une nouvelle version est ajoutée.
    """
    if not data:
        return 0

    data_enrichie = enrichir_lignes(data, batch_id)

    # Déduplication exacte à l'intérieur du batch Python.
    data_enrichie = list({ligne["row_hash"]: ligne for ligne in data_enrichie}.values())

    hashes_existants = recuperer_hashes_existants(table_id, colonne_date, data_enrichie)
    nouvelles_lignes = [
        ligne for ligne in data_enrichie if ligne["row_hash"] not in hashes_existants
    ]

    if not nouvelles_lignes:
        logger.info("→ Aucune nouvelle ligne à insérer.")
        return 0

    inserer_lignes_bigquery(nouvelles_lignes, table_id)

    logger.info(f"→ {len(nouvelles_lignes)} lignes ajoutées (batch {batch_id}).")

    return len(nouvelles_lignes)


def annuler_le_lot(table_id: str, batch_id: str) -> None:
    """Rollback : supprime les lignes ajoutées par cette exécution."""
    query = f"DELETE FROM `{table_id}` WHERE batch_id = @batch_id"

    query_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
    )

    client.query(query, job_config=query_config).result()

    logger.info(f"↩️ Rollback effectué : batch {batch_id} supprimé de {table_id}.")
