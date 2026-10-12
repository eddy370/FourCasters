# Reproduire les traitements FourCasters

Le README principal présente le projet. Cette page détaille son environnement et les étapes nécessaires pour exécuter le code.

## Installation et contrôles locaux

Depuis la racine du dépôt, avec Python 3.12 :

```bash
python -m venv .venv
```

Activer l'environnement avec `source .venv/bin/activate` sous bash, ou `.\.venv\Scripts\Activate.ps1` sous PowerShell, puis :

```bash
pip install -r requirements.txt
pip check
python -m unittest discover -s tests -v
```

Les dépendances de `pyproject.toml` correspondent aux neuf versions de `requirements.txt`. `uv.lock` est conservé et actualisé pour les utilisateurs du gestionnaire uv déjà présent dans le projet. La commande `projet-fourcasters`, après installation du package, lance le même `run_pipeline.py` depuis la racine du dépôt.

Les tests locaux remplacent le client BigQuery et exécutent les expressions SQL contrôlées avec SQLite. Ils couvrent les versions RAW, la pluie sur trois jours, les reprises d'ingestion et la qualité hydro ; ils ne remplacent pas l'exécution BigQuery/dbt.

## Ressources BigQuery attendues

Le code utilise les identifiants du projet `projet-les-fourcasters`. Pour le reproduire dans un autre projet, adapter les identifiants présents dans les scripts, les sources dbt et le profil.

Préparer les datasets et ressources décrites dans [les sources dbt](../fourcasters/models/staging/) :

- `raw_openmeteo.open_meteo_corrigé` : météo historique importée ;
- les deux imports ONRN du dataset `raw_onrn` ;
- les observations hydrologiques historiques, préparées séparément du traitement quotidien ;
- la table de destination `ml.predictions`, avec le schéma `SCHEMA_PREDICTIONS` de [predict.py](../predict.py).

Le seed contient le référentiel des 360 communes ou points d'intérêt. Les ingestions créent leurs tables RAW quotidiennes. Elles ne reconstruisent pas automatiquement les imports historiques depuis 2000. Une station Hub'Eau encore sans données reçoit d'abord la fenêtre récente ; un chargement historique distinct reste nécessaire.

Le notebook EDA attend `data/raw/openmeteo_finale.parquet`. Ce fichier volumineux n'est pas fourni : les sorties du notebook restent consultables, mais une réexécution exige l'export correspondant. Les scripts historiques attendent également des fichiers locaux absents, notamment `Departement_V3_centroide_bonne.csv` et des fichiers météo CSV/parquet.

## Profil dbt et exécution cloud

Configurer une clé de service hors du dépôt et définir `GOOGLE_APPLICATION_CREDENTIALS` avec son chemin absolu. Exemple de `~/.dbt/profiles.yml` :

```yaml
fourcasters:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: projet-les-fourcasters
      dataset: dbt_dev
      location: US
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
```

Vérifier d'abord la connexion et charger le référentiel :

```bash
dbt debug --project-dir fourcasters
dbt seed --project-dir fourcasters
```

Puis lancer la chaîne depuis la racine :

```bash
python run_pipeline.py
```

Cette commande effectue des lectures et écritures BigQuery, avec les coûts associés. Elle collecte les données, construit les modèles, lance tous les tests dbt, puis écrit les prédictions. Le mart hydro est reconstruit pour harmoniser les seuils de toutes les années ; les versions RAW restent conservées. Le mart météo reste incrémental et accepte les dates anciennes absentes lors d'un rattrapage.

Les logs doivent indiquer les dates RAW, la date de `ml_features`, le nombre de stations, la date J+1 ciblée et les lignes de prédiction insérées ou mises à jour. Rejouer une date met à jour les prédictions existantes par station ; les dates précédentes sont conservées.

Sous GitHub Actions, le workflow attend le secret `GCP_SA_KEY`. Il prépare le profil dbt et sérialise les exécutions. Les changements de code sur `main` déclenchent une vérification cloud ; les modifications du README ou des captures seules ne la déclenchent pas. Éviter les ingestions manuelles concurrentes avec ce workflow.

## Notebooks et résultats ML

[ml_fourcasters.ipynb](../notebooks/ml_fourcasters.ipynb) est prévu pour Colab, avec authentification Google et accès BigQuery. Les bibliothèques de visualisation utilisées sont à vérifier dans l'environnement Colab avant une réexécution. La table `marts.ml_features` évolue ; le jeu historique du retest n'est pas fourni comme export figé.

Pour vérifier le modèle utilisé par `predict.py` : importer le `.pkl` du dépôt dans Colab, puis exécuter la cellule « Évaluation du modèle sauvegardé » avant la cellule d'export. Son empreinte est contrôlée. Cette évaluation est distincte du modèle entraîné dans le notebook. Les résultats enregistrés antérieurement ne lui sont pas attribués.

Les sorties nouvelles doivent être obtenues avant de modifier les chiffres du README. Une validation prospective nécessite aussi des seuils calculés uniquement sur le passé. Le fichier Power BI n'est pas fourni ; les captures permettent d'examiner la restitution, sans reconstruire les mesures DAX.

## Fichiers de travail conservés

- `src/chargement_openmeteo_historique.py` : collecte météo historique vers un fichier local.
- `Script/loadata_toute_api_actualiser.py` : script de travail comprenant les chargements historiques.
- `Script/corriger_parquet.py` : inspection d'un parquet local et de coordonnées ; il n'est pas appelé par le pipeline quotidien.
- Documents Word : documents initiaux de formation, avec des hypothèses et consignes de travail conservées. Le scénario SDIS est pédagogique.

Ces fichiers expliquent le travail initial. Les points d'entrée actuels sont `load_data.py`, `run_pipeline.py` et `predict.py`.
