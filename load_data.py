from datetime import datetime, timezone

from src.openmeteo import ingest_openmeteo
from src.hubeau import ingest_hubeau_stations, ingest_hubeau_obs_elab

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main() -> None:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info(f"Debut du pipeline, batch_id = {batch_id}")

    ingest_hubeau_stations()
    ingest_openmeteo(batch_id)
    ingest_hubeau_obs_elab(batch_id)

    logger.info(f"Fin du pipeline, batch_id = {batch_id}")


if __name__ == "__main__":
    main()
