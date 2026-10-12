# Script de travail historique, hors du pipeline quotidien run_pipeline.py.
# Les fichiers sources et étapes manuelles sont décrits dans docs/reproduction.md.

import pandas as pd

df = pd.read_parquet("data/raw/openmeteo_finale_vX.parquet")

print("Nombre total de lignes :", len(df))

controle = df[
    df["Ville"]
    .fillna("")
    .str.lower()
    .isin([
        "mende",
        "saint-chély-d'apcher"
    ])
][
    ["Ville", "Latitude", "Longitude"]
].drop_duplicates()

print(controle)