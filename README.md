# FourCasters — Du traitement de données à l'aide à la décision

**Un pipeline de données automatisé, de la collecte aux tableaux de bord et à la prédiction.**

FourCasters est une **preuve de concept (PoC) en Data Engineering, Business Intelligence et Machine Learning**, appliquée aux risques naturels en France.

Le principe : réunir des données publiques dispersées, automatiser leur collecte et leur transformation, contrôler leur qualité, puis produire des indicateurs consultables dans Power BI. Une brique de machine learning complète l'analyse avec une estimation de dépassement de seuil hydrologique à J+1.

**Ce que démontre ce projet :** la capacité à concevoir une chaîne de données de bout en bout, plutôt qu'une analyse ponctuelle réalisée dans un notebook.

## Le résultat : un tableau de bord Power BI

Le tableau de bord rassemble cinq angles d'analyse :

- **Historique hydrologique :** évolution des niveaux mesurés et comparaison avec des repères historiques.
- **Vue nationale :** synthèse des observations et comparaison géographique.
- **Analyse départementale :** exploration des différences entre territoires.
- **Risque d'inondation :** exploitation de données historiques publiques de l'ONRN.
- **Prévision J+1 :** estimation, station par station, de dépassements de niveaux élevés à partir des dernières données consolidées.

L'objectif est de passer de plusieurs jeux de données et traitements techniques à une **lecture opérationnelle plus simple** : indicateurs, cartes, graphiques et filtres interactifs.

> **Important :** FourCasters n'est ni un dispositif d'alerte officiel ni un système de prévision en temps réel. Le « J+1 » est calculé à partir de la **dernière date de données exploitables**, pas nécessairement à partir de la date du jour.

## Chiffres clés du projet

| Indicateur | Résultat |
| --- | --- |
| Données de variables ML | Environ **1,8 million de lignes** |
| Couverture du jeu ML historique | **247 stations hydrométriques** |
| Objectif du modèle | Détecter un dépassement du seuil historique **P95 à J+1** |
| Rappel sur le jeu de test | **59 %** des dépassements identifiés |

*Volumes issus de la reconstruction vérifiée le **07/10/2026** ; ce sont des valeurs datées, pas des compteurs en temps réel. Le rappel de 59 % est une métrique sur un jeu de test, et non une garantie de performance en exploitation.*

## Compétences et valeur démontrées

| Besoin | Réalisation dans FourCasters |
| --- | --- |
| **Regrouper des données hétérogènes** | API Open-Meteo et Hub'Eau, fichiers publics ONRN et référentiels géographiques |
| **Réduire les opérations manuelles récurrentes** | Scripts Python et exécution planifiée par GitHub Actions |
| **Améliorer la fiabilité des traitements** | Contrôles des réponses API, gestion d'erreurs, déduplication et tests dbt |
| **Rendre les données exploitables** | Modélisation SQL/dbt dans BigQuery et restitution Power BI |
| **Explorer un usage prédictif** | Pipeline de classification avec scikit-learn et publication des résultats dans BigQuery |

**Applications possibles de cette méthode :** consolidation d'indicateurs de production, suivi qualité/HSE, tableaux de bord logistiques ou rapprochement de fichiers métier. Il s'agit de **compétences transférables**, et non de fonctionnalités déjà développées ou de prestations déjà réalisées chez des clients.

## Architecture technique

```text
Open-Meteo (API) ──┐
                   ├──> Python / ingestion ──> BigQuery (RAW)
Hub'Eau (API) ─────┘                                │
                                                   v
ONRN (fichiers) ────────────────> Données importées │
                                                   v
                                       dbt / SQL (transformations)
                                       staging → intermediate → marts
                                                   │
                              ┌────────────────────┴─────────────────┐
                              v                                      v
                        Power BI / BI                        Features ML
                                                                     │
                                                                     v
                                                      scikit-learn / prédiction
                                                                     │
                                                                     v
                                                       BigQuery ml.predictions
                                                                     │
                                                                     v
                                                            Power BI / J+1

GitHub Actions : exécution planifiée ou manuelle du pipeline
```

**Stack :** Python · SQL · Google BigQuery · dbt · GitHub Actions · scikit-learn · Power BI.

Le script `run_pipeline.py` enchaîne l'ingestion, les transformations dbt, les tests associés aux variables ML et l'inférence. Une étape en échec interrompt l'exécution.

### Modèle de données

Le schéma présente les principales tables et leurs relations :

![Schéma des tables FourCasters](docs/schema_bdd.svg)

## Sources réellement utilisées

- **Open-Meteo (API)** : variables météorologiques historiques.
- **Hub'Eau (API)** : référentiel des stations et observations hydrométriques.
- **ONRN (fichiers publics importés)** : données historiques relatives aux risques naturels, dont les inondations.
- **Référentiels géographiques** : correspondances communes, départements et stations.

Les données ne suivent pas toutes le même mode d'ingestion : les deux API sont intégrées aux scripts de collecte, tandis que les fichiers ONRN sont importés séparément.

## Fiabilité, automatisation et choix ML

- **Collecte résiliente :** pagination Hub'Eau, délais d'attente, tentatives supplémentaires sur les erreurs temporaires et gestion des limitations d'appels Open-Meteo.
- **Traçabilité des chargements :** identifiants de lot, empreintes de contenu SHA-256 et prise en compte des corrections de données.
- **Transformation structurée :** modèles dbt organisés en couches, avec tests déclenchés avant la prédiction.
- **Réexécution des prédictions :** mise à jour BigQuery par `MERGE` pour éviter de dupliquer les résultats d'une même station à une même date.
- **Modèle interprétable :** régression logistique avec imputation des valeurs manquantes, standardisation et pondération des classes pour traiter la rareté des dépassements.

### Limites à connaître

L'ingestion conserve volontairement un **recul de 7 jours** pour éviter de traiter comme définitives les données les plus récentes. Le modèle estime un dépassement de seuil hydrologique à J+1 **relativement à cette dernière date**, et non une inondation imminente.

Le modèle ne prédit ni les routes coupées, ni les dommages matériels, ni le risque à une adresse précise. La disponibilité des API, la qualité des données et les performances du modèle restent des contraintes à surveiller.

## Installation et reproduction

### Prérequis

- Python et les dépendances définies dans `requirements.txt` (le workflow GitHub Actions utilise Python 3.12)
- Un projet Google Cloud avec BigQuery et les autorisations nécessaires
- La configuration d'authentification Google Cloud et le profil dbt `~/.dbt/profiles.yml`
- L'accès aux tables et référentiels attendus par le pipeline
- Le modèle entraîné utilisé par `predict.py`

### Installation

```bash
git clone https://github.com/eddy370/FourCasters.git
cd FourCasters
python -m venv .venv
# Activer .venv selon votre système (Windows, macOS ou Linux)
pip install -r requirements.txt
```

### Exécution

```bash
python run_pipeline.py
```

**Attention :** plusieurs noms de projet et de tables BigQuery sont actuellement spécifiques à l'environnement FourCasters. Cloner le dépôt ne suffit pas pour exécuter le pipeline dans un autre compte GCP : il faut adapter les identifiants, préparer les ressources et configurer les accès. Ne jamais publier de clé de service dans Git.

### Organisation du dépôt

```text
FourCasters/
├── .github/workflows/     # Planification et exécution du pipeline
├── docs/                  # Documentation et schéma des tables
├── fourcasters/
│   ├── models/            # Transformations dbt
│   ├── seeds/             # Données de référence
│   └── dbt_project.yml
├── src/                   # Connecteurs API et utilitaires BigQuery
├── load_data.py           # Orchestration de la collecte
├── run_pipeline.py        # Pipeline de bout en bout
├── predict.py             # Inférence et écriture des prédictions
├── requirements.txt
└── README.md
```

## Contexte du projet

FourCasters est un **projet collectif réalisé dans le cadre de la formation Data Analyst de la Wild Code School**. Il sert à démontrer une méthode de travail et des compétences techniques, et non à présenter une solution commerciale déjà déployée.

Ce dépôt documente le code et l'architecture. Les résultats et métriques sont présentés avec leur date ou leur périmètre d'évaluation pour éviter de les confondre avec des garanties opérationnelles.
