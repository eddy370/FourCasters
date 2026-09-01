# run_pipeline.py — ingestion + transformation, en une seule commande
import time
import subprocess
from load_data import ingest_data  # réutilise le script d'ingestion


def avec_retry(action, essais=3, delai=5):
    """Lance action(). En cas d'erreur, réessaie quelques fois avant d'abandonner."""
    for tentative in range(1, essais + 1):
        try:
            return action()
        except Exception as e:
            print(f"⚠️ Échec (tentative {tentative}/{essais}) : {e}")
            if tentative < essais:
                print(f"↻ Nouvel essai dans {delai}s…")
                time.sleep(delai)
    raise RuntimeError(f"Abandon après {essais} tentatives.")


# 1. Ingestion : API → hash → raw_data
print("=== 1. Ingestion (API → raw) ===")
avec_retry(ingest_data, essais=3, delai=5)

# 2. Transformation : dbt run
print("\n=== 2. dbt run (raw → staging → marts) ===")
subprocess.run(["uv", "run", "dbt", "run"], check=True, cwd="../fourcasters")

print("\n✅ Pipeline terminé : données ingérées ET transformées.")
