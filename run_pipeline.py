# run_pipeline.py — ingestion + transformation + prédiction, en une seule commande
#
# Chaque étape ne démarre que si la précédente a réussi : une exception
# (ou un code retour non nul de dbt) arrête le script avec un code d'erreur.

import os
import sys
import time
import subprocess
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load_data import main as ingest_data
from predict import predire, dates_max_ingestion
from google.cloud import bigquery


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def avec_retry(action, essais=3, delai=5):
    """Lance action(). En cas d'erreur, réessaie quelques fois avant d'abandonner."""

    for tentative in range(1, essais + 1):
        try:
            return action()

        except Exception as e:
            logger.warning(
                "Échec tentative %s/%s : %s",
                tentative,
                essais,
                e
            )

            if tentative < essais:
                logger.info("Nouvel essai dans %s secondes", delai)
                time.sleep(delai)

    raise RuntimeError(f"Abandon après {essais} tentatives.")


# 1. Ingestion : API → raw
logger.info("=== 1. Ingestion (API → raw) ===")

avec_retry(
    ingest_data,
    essais=3,
    delai=5
)

ingestion = dates_max_ingestion(bigquery.Client(project="projet-les-fourcasters"))
logger.info(
    "Date max ingestion : Hub'Eau %s, Open-Meteo %s",
    ingestion["hubeau"],
    ingestion["openmeteo"]
)


# 2. Transformation : dbt (dont marts.ml_features)
logger.info("=== 2. dbt run (raw → staging → marts) ===")

subprocess.run(
    ["dbt", "run"],
    check=True,
    cwd="fourcasters"
)


# 3. Contrôle de ml_features avant inférence : unicité (date, station)
#    et dernière date exploitable
logger.info("=== 3. dbt test (ml_features) ===")

subprocess.run(
    ["dbt", "test", "--select", "ml_features"],
    check=True,
    cwd="fourcasters"
)


# 4. Inférence : marts.ml_features → ml.predictions (MERGE)
logger.info("=== 4. Prédiction J+1 (ml_features → ml.predictions) ===")

predire()


logger.info("Pipeline terminé : données ingérées, transformées et prédites.")
