"""
Inférence quotidienne : prédit le dépassement du P95 à J+1 pour chaque
station de la dernière date disponible dans marts.ml_features, puis écrit
le résultat dans ml.predictions par MERGE sur (date, code_station).

Rejouer le script pour une même date met à jour les lignes existantes au
lieu de les dupliquer ; les prédictions des dates précédentes sont conservées.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import joblib
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()

logger = logging.getLogger(__name__)

PROJECT = "projet-les-fourcasters"
FEATURES_TABLE = f"{PROJECT}.marts.ml_features"
PREDICTIONS_TABLE = f"{PROJECT}.ml.predictions"
STAGING_TABLE = f"{PROJECT}.ml.predictions_staging"

HUBEAU_RAW_TABLE = f"{PROJECT}.raw_hubeau.observations_hydrometriques_elaborees"
OPENMETEO_RAW_TABLE = f"{PROJECT}.raw_openmeteo.meteo_journaliere_raw"

MODELE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fourcasters_logistic_regression.pkl"
)

features = [
    "position_p95",
    "variation_position_1j",
    "pluie_j",
    "pluie_3j",
    "humidite_sol"
]

# Schéma de ml.predictions (inchangé, lu par Power BI en DirectQuery).
SCHEMA_PREDICTIONS = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("code_station", "STRING"),
    bigquery.SchemaField("code_insee", "STRING"),
    bigquery.SchemaField("code_departement", "STRING"),
    bigquery.SchemaField("position_p95", "FLOAT"),
    bigquery.SchemaField("variation_position_1j", "FLOAT"),
    bigquery.SchemaField("pluie_j", "FLOAT"),
    bigquery.SchemaField("pluie_3j", "FLOAT"),
    bigquery.SchemaField("humidite_sol", "FLOAT"),
    bigquery.SchemaField("prediction_j1", "INTEGER"),
    bigquery.SchemaField("probabilite_depassement_j1", "FLOAT"),
    bigquery.SchemaField("date_prediction", "TIMESTAMP"),
]


def dates_max_ingestion(client: bigquery.Client) -> dict:
    """Dernière date présente dans les tables RAW Hub'Eau et Open-Meteo."""

    query = f"""
    SELECT
        (SELECT MAX(date_obs_elab) FROM `{HUBEAU_RAW_TABLE}`) AS hubeau,
        (SELECT MAX(date) FROM `{OPENMETEO_RAW_TABLE}`) AS openmeteo
    """

    row = next(iter(client.query(query).result()))

    return {"hubeau": row.hubeau, "openmeteo": row.openmeteo}


def predire() -> int:
    """Prédit J+1 pour la dernière date de ml_features. Renvoie le nombre
    de lignes écrites (insérées + mises à jour) dans ml.predictions."""

    client = bigquery.Client(project=PROJECT)

    ingestion = dates_max_ingestion(client)
    logger.info(
        "Date max ingestion : Hub'Eau %s, Open-Meteo %s",
        ingestion["hubeau"],
        ingestion["openmeteo"]
    )

    # Lire les stations de la dernière date disponible
    query = f"""
    SELECT
        date,
        code_station,
        code_insee,
        code_departement,
        position_p95,
        variation_position_1j,
        pluie_j,
        pluie_3j,
        humidite_sol
    FROM `{FEATURES_TABLE}`
    WHERE date = (
        SELECT MAX(date)
        FROM `{FEATURES_TABLE}`
    )
    """

    df = client.query(query).to_dataframe(
        create_bqstorage_client=False
    )

    if df.empty:
        raise RuntimeError(f"Aucune ligne dans {FEATURES_TABLE} : rien à prédire.")

    date_features = df["date"].max()
    date_cible = date_features + timedelta(days=1)

    logger.info("Date max ml_features : %s", date_features)

    date_ingestion_commune = min(ingestion["hubeau"], ingestion["openmeteo"])
    if date_features < date_ingestion_commune:
        logger.warning(
            "ml_features (%s) est en retard sur l'ingestion (%s) : "
            "jours sans données exploitables (météo ou hydro manquante).",
            date_features,
            date_ingestion_commune
        )

    if df.duplicated(["date", "code_station"]).any():
        raise RuntimeError("Doublons (date, code_station) dans ml_features.")

    logger.info("Nombre de stations à prédire : %s", len(df))
    logger.info("Date J+1 ciblée : %s", date_cible)

    # Charger le pipeline entraîné (imputer + scaler + régression logistique)
    pipeline = joblib.load(MODELE)

    # Prédiction binaire
    df["prediction_j1"] = pipeline.predict(
        df[features]
    )

    # Probabilité de dépassement P95 à J+1
    df["probabilite_depassement_j1"] = pipeline.predict_proba(
        df[features]
    )[:, 1]

    # Date de calcul de la prédiction
    df["date_prediction"] = datetime.now(timezone.utc)

    # 1. Chargement dans une table de transit, écrasée à chaque exécution
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=SCHEMA_PREDICTIONS
    )

    client.load_table_from_dataframe(
        df,
        STAGING_TABLE,
        job_config=job_config
    ).result()

    # 2. MERGE idempotent : une ligne par (date, code_station)
    merge = f"""
    MERGE `{PREDICTIONS_TABLE}` p
    USING `{STAGING_TABLE}` s
        ON p.date = s.date
        AND p.code_station = s.code_station

    WHEN MATCHED THEN UPDATE SET
        code_insee = s.code_insee,
        code_departement = s.code_departement,
        position_p95 = s.position_p95,
        variation_position_1j = s.variation_position_1j,
        pluie_j = s.pluie_j,
        pluie_3j = s.pluie_3j,
        humidite_sol = s.humidite_sol,
        prediction_j1 = s.prediction_j1,
        probabilite_depassement_j1 = s.probabilite_depassement_j1,
        date_prediction = s.date_prediction

    WHEN NOT MATCHED THEN INSERT ROW
    """

    merge_job = client.query(merge)
    merge_job.result()

    stats = merge_job.dml_stats
    inserees = stats.inserted_row_count if stats else 0
    mises_a_jour = stats.updated_row_count if stats else 0

    client.delete_table(STAGING_TABLE, not_found_ok=True)

    logger.info(
        "Nombre de prédictions écrites dans ml.predictions : %s "
        "(%s insérées, %s mises à jour)",
        inserees + mises_a_jour,
        inserees,
        mises_a_jour
    )

    return inserees + mises_a_jour


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    predire()
