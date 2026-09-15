# run_pipeline.py — ingestion + transformation, en une seule commande

import os
import sys
import time
import subprocess
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load_data import main as ingest_data


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


# 2. Transformation : dbt
logger.info("=== 2. dbt run (raw → staging → marts) ===")

subprocess.run(
    ["uv", "run", "dbt", "run"],
    check=True,
    cwd="fourcasters"
)


logger.info("Pipeline terminé : données ingérées ET transformées.")
