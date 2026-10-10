# FourCasters

**Des données publiques dispersées à un tableau de bord automatisé sur les risques d'inondation en France.**

FourCasters est un projet de data engineering, d'analyse et de machine learning. Il collecte des observations hydrologiques et météorologiques, les centralise dans BigQuery, les transforme avec dbt et les restitue dans Power BI. Un modèle de classification estime, pour chaque station couverte, le risque de dépassement d'un repère hydrologique historique à J+1 **par rapport à la dernière journée de données disponible**.

> **Périmètre et limite importante :** FourCasters est un démonstrateur analytique, **pas un système d'alerte officiel ou en temps réel**. L'ingestion utilise volontairement un recul de 7 jours pour privilégier des données consolidées. La prévision J+1 ne signifie donc pas « demain par rapport à aujourd'hui ».

## Aperçu du résultat

Le tableau de bord Power BI comprend notamment une vue nationale, un historique hydrologique, une analyse départementale, une analyse du risque d'inondation et une page de prévision J+1.

**Captures du tableau de bord : à ajouter** (vue nationale et prévision J+1). Les visuels ne sont pas intégrés ici pour éviter d'afficher des images non vérifiées ou périmées.

## Ce que le projet démontre

- **Collecter et centraliser** des données issues de plusieurs sources publiques.
- **Automatiser** l'ingestion, les transformations et la génération de prédictions.
- **Fiabiliser** les traitements avec des contrôles, des reprises sur erreur et une gestion des doublons et des corrections de données.
- **Transformer** les données brutes en indicateurs exploitables dans un tableau de bord.
- **Documenter les limites** d'un modèle prédictif et la fraîcheur des données utilisées.

Ces compétences sont transférables à d'autres contextes (reporting opérationnel, suivi qualité, consolidation de données, pilotage logistique). **Ces cas d'usage ne sont pas des fonctionnalités déjà développées dans FourCasters.**

## Résultats et périmètre mesurés

- **247 stations hydrométriques** dans le jeu de données ML historique, selon l'état vérifié du projet au **07/10/2026**.
- **Environ 1,8 million de lignes** dans le jeu de variables ML reconstruit à cette date.
- **59 % de rappel sur le jeu de test** pour la détection des dépassements à J+1. Il s'agit d'une métrique d'évaluation du modèle, et non d'une garantie de performance future.
- Prédiction binaire du dépassement d'un seuil historique **P95**, calculée à partir des données hydrologiques et météorologiques disponibles.

Les chiffres ci-dessus correspondent à un état daté du projet et ne décrivent pas nécessairement le contenu actuel des tables BigQuery.

## Architecture

```text
Open-Meteo (API) ─┐
                  ├─> Python (ingestion) ─> BigQuery RAW
Hub'Eau (API) ────┘                              │
                                                v
                                         dbt (staging / marts)
                                                │
                                  ┌─────────────┴──────────────┐
                                  v                            v
                           Power BI (analyse)          ML (prédiction J+1)
                                                               │
                                                               v
                                                      BigQuery ml.predictions
                                                               │
                                                               v
                                                       Power BI (prévision)

GitHub Actions : exécution planifiée du pipeline
```

**Technologies :** Python, SQL, Google BigQuery, dbt, GitHub Actions, scikit-learn et Power BI.

Le script `run_pipeline.py` orchestre l'ingestion, les transformations dbt, les tests de `ml_features` et l'inférence ML. Le workflow GitHub Actions permet une exécution planifiée ou manuelle.

## Sources de données

- **Open-Meteo (API)** : observations météorologiques historiques.
- **Hub'Eau (API)** : stations et observations hydrométriques.
- **ONRN (données publiques importées)** : indicateurs historiques liés aux risques naturels et aux inondations.
- **Référentiels géographiques** : rattachement des stations et observations aux communes et départements.

**NASA FIRMS n'est pas une source utilisée dans le pipeline FourCasters.**

## Qualité et limites

- L'ingestion prévoit des tentatives supplémentaires sur les erreurs transitoires d'API et une gestion des limites de requêtes.
- Les chargements RAW utilisent des empreintes de contenu et des identifiants de lot pour faciliter la déduplication et la traçabilité.
- Le pipeline exécute des tests dbt avant l'inférence.
- Le modèle ML est une régression logistique avec imputation, standardisation et pondération des classes. La cible est le dépassement du P95 le lendemain **de la date des observations utilisées**.
- Les résultats ML sont indicatifs : ils ne prédisent ni les routes coupées, ni les dommages, ni les inondations à une adresse donnée.
- Le projet dépend de la disponibilité et de la qualité des sources publiques ; l'automatisation ne garantit pas une mise à jour réussie chaque jour.

## Installation et exécution

### Prérequis

- Python 3.11 ou version compatible avec les dépendances installées
- Un projet Google Cloud avec BigQuery activé
- Des identifiants GCP disposant des autorisations nécessaires
- dbt et son adaptateur BigQuery
- Git

### Installation

```bash
git clone https://github.com/eddy370/FourCasters.git
cd FourCasters
python -m venv .venv
# Activer l'environnement virtuel selon votre système
pip install -r requirements.txt
```

Configurer les identifiants Google Cloud et le fichier `~/.dbt/profiles.yml` pour votre environnement. **Le dépôt référence des tables et un projet GCP spécifiques : une exécution sur un autre projet nécessite d'adapter cette configuration et les identifiants de tables dans les scripts.**

### Lancement

```bash
python run_pipeline.py
```

Cette commande lance l'ingestion, les transformations dbt, les tests ML configurés et les prédictions. Elle nécessite l'accès aux ressources BigQuery attendues et au modèle sérialisé utilisé par `predict.py`.

## Structure du dépôt

```text
FourCasters/
├── .github/workflows/        # Exécution planifiée
├── docs/                     # Schéma et documentation
├── fourcasters/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── seeds/
│   └── dbt_project.yml
├── src/                      # Connecteurs API et utilitaires BigQuery
├── load_data.py              # Ingestion
├── run_pipeline.py           # Orchestration
├── predict.py                # Inférence ML
├── requirements.txt
└── README.md
```

### Modèle de données

![Schéma du modèle de données](docs/schema_bdd.svg)

## Contexte

Projet collectif réalisé dans le cadre de la formation **Data Analyst de la Wild Code School**. Ce dépôt illustre une chaîne de traitement de bout en bout ; il ne constitue ni un service commercial déployé chez un client, ni une solution officielle de prévention des crues.
