# Modèles SQL/dbt FourCasters

Ce dossier prépare les données BigQuery du projet collectif FourCasters réalisé à la Wild Code School. La présentation officielle est le [README principal](../README.md).

- `models/staging/` : typage, nettoyage et sélection de la dernière version RAW.
- `models/marts/` : dimensions, faits météo/hydro et les cinq variables ML avec leur cible J+1.
- `seeds/` : référentiel géographique.
- `tests/` : contrôles SQL complémentaires aux tests déclarés dans les fichiers YAML.

Avec le profil BigQuery configuré, depuis la racine du dépôt :

```bash
dbt seed --project-dir fourcasters
dbt run --project-dir fourcasters
dbt test --project-dir fourcasters
```

Le mart hydro est reconstruit pour appliquer des seuils station/mois cohérents à toutes les années. Le mart météo reste incrémental. Les détails d'installation et les imports attendus sont dans [reproduction.md](../docs/reproduction.md).
