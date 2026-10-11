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
from datetime import date, datetime, timezone

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

# Mêmes clés et mêmes départages que dans les deux stagings dbt.
CLES_METIER_RAW = {
    "date": ("code_INSEE", "date"),
    "date_obs_elab": ("code_station", "date_obs_elab", "grandeur_hydro_elab"),
}


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


def cle_metier(ligne: dict, colonne_date: str) -> tuple:
    """Normalise la clé comme le staging ; refuse les clés manquantes."""
    if colonne_date not in CLES_METIER_RAW:
        raise ValueError(f"Source RAW non prise en charge : {colonne_date}.")

    valeurs = []
    for colonne in CLES_METIER_RAW[colonne_date]:
        valeur = ligne.get(colonne)
        if valeur is None or not str(valeur).strip():
            raise ValueError(f"Clé métier incomplète : {colonne} manquant.")
        valeur = str(valeur).strip()
        if colonne == colonne_date:
            valeur = date.fromisoformat(valeur[:10]).isoformat()
        elif colonne == "code_INSEE":
            if len(valeur) > 5:
                raise ValueError(f"Code INSEE trop long : {valeur}.")
            valeur = valeur.zfill(5)
        valeurs.append(valeur)

    return tuple(valeurs)


def recuperer_hashes_existants(table_id: str, colonne_date: str, data: list) -> dict:
    """Récupère uniquement le hash COURANT par clé sur la période du batch.

    L'historique reste en RAW. Un hash ancien ne doit pas empêcher un retour
    A → B → A. L'ordre de sélection est identique à celui du staging dbt.
    """
    cles = [cle_metier(ligne, colonne_date) for ligne in data]
    index_date = CLES_METIER_RAW[colonne_date].index(colonne_date)
    dates = [cle[index_date] for cle in cles]
    entites = sorted({cle[0] for cle in cles})
    parametres = [
        bigquery.ScalarQueryParameter("date_min", "DATE", min(dates)),
        bigquery.ScalarQueryParameter("date_max", "DATE", max(dates)),
        bigquery.ArrayQueryParameter("entites", "STRING", entites),
    ]

    if colonne_date == "date":
        colonnes = "code_INSEE, date"
        expressions = (
            "LPAD(TRIM(CAST(code_INSEE AS STRING)), 5, '0'), SAFE_CAST(date AS DATE)"
        )
        ordre = "inserted_at DESC, row_hash DESC"
        filtre_cles = "LPAD(TRIM(CAST(code_INSEE AS STRING)), 5, '0') IN UNNEST(@entites)"
    else:
        colonnes = "code_station, date_obs_elab, grandeur_hydro_elab"
        expressions = (
            "TRIM(code_station), SAFE_CAST(date_obs_elab AS DATE), TRIM(grandeur_hydro_elab)"
        )
        ordre = "inserted_at DESC, date_prod DESC, row_hash DESC"
        filtre_cles = (
            "TRIM(code_station) IN UNNEST(@entites) "
            "AND TRIM(grandeur_hydro_elab) IN UNNEST(@grandeurs)"
        )
        parametres.append(bigquery.ArrayQueryParameter(
            "grandeurs", "STRING", sorted({cle[2] for cle in cles})
        ))

    # Pas de filtre sur les hashes reçus AVANT la sélection : il masquerait B
    # lors d'un retour à A. Le filtre de dates limite la lecture des partitions.
    query = f"""
        WITH versions AS (
            SELECT {colonnes}, row_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY {expressions}
                    ORDER BY {ordre}
                ) AS rang
            FROM `{table_id}`
            WHERE {colonne_date} BETWEEN @date_min AND @date_max
                AND {filtre_cles}
        )
        SELECT {colonnes}, row_hash
        FROM versions
        WHERE rang = 1
    """

    query_config = bigquery.QueryJobConfig(query_parameters=parametres)

    resultat = client.query(query, job_config=query_config).result()

    return {cle_metier(ligne, colonne_date): ligne["row_hash"] for ligne in resultat}


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

    - même contenu que la version courante : aucune insertion ;
    - contenu différent de la version courante : une nouvelle version est ajoutée,
      même si ce contenu figurait dans une version ancienne.
    Deux contenus différents pour une même clé dans le batch sont ambigus :
    on arrête le chargement pour laisser l'ingestion annuler son lot.
    """
    if not data:
        return 0

    data_enrichie = enrichir_lignes(data, batch_id)

    # Une seule valeur reçue par clé ; aucun choix arbitraire entre deux versions.
    lignes_par_cle = {}
    for ligne in data_enrichie:
        cle = cle_metier(ligne, colonne_date)
        precedente = lignes_par_cle.get(cle)
        if precedente is not None and precedente["row_hash"] != ligne["row_hash"]:
            raise ValueError(f"Versions contradictoires dans le batch pour {cle}.")
        lignes_par_cle[cle] = ligne

    data_enrichie = list(lignes_par_cle.values())

    hashes_existants = recuperer_hashes_existants(table_id, colonne_date, data_enrichie)
    nouvelles_lignes = [
        ligne for cle, ligne in lignes_par_cle.items()
        if ligne["row_hash"] != hashes_existants.get(cle)
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
