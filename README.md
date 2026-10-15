# FourCasters — Explorer les données hydrologiques en France

[![Pipeline quotidien](https://github.com/eddy370/FourCasters/actions/workflows/pipeline.yml/badge.svg)](https://github.com/eddy370/FourCasters/actions/workflows/pipeline.yml)

**Projet réalisé pendant la formation Data Analyst de la Wild Code School, au sein d'un groupe de quatre.** Le cadrage initial, dont le choix d'Open-Meteo, a été fait collectivement. Ensuite, chacun a avancé de manière autonome sur sa partie du projet.

Comment suivre l'évolution des cours d'eau à partir de données publiques dispersées entre plusieurs organismes ? FourCasters rapproche des observations hydrologiques, des données météo et des statistiques historiques de sinistres, puis les rend consultables dans Power BI.

Mon travail a porté principalement sur le volet hydrologique, sa préparation dans BigQuery/dbt, les indicateurs et tableaux de bord, ainsi que sur l'expérimentation d'un modèle de machine learning pour repérer les stations susceptibles de franchir un seuil de niveau d'eau élevé à J+1.

**Technologies :** Python · SQL · BigQuery · dbt · Power BI · scikit-learn · GitHub Actions

[Tableaux de bord](#tableaux-de-bord) · [Architecture](#de-la-donnée-brute-aux-indicateurs) · [Machine learning](#une-première-prédiction-à-j1) · [Reproduire les traitements](docs/reproduction.md)

## Tableaux de bord

Les pages Power BI permettent de passer d'une vue nationale à une analyse par département ou par station, puis d'explorer les résultats de la prédiction à J+1.

### Vue nationale

![Vue nationale FourCasters : indicateurs hydrologiques, carte et comparaison départementale](docs/images/vue-nationale-2026-10-12.png)

*Exemple sur l'année 2010 : les écarts sont calculés par rapport au repère historique propre à chaque station.*

### Prévision à J+1

![Prévision J+1 dans Power BI : probabilités de dépassement, départements concernés et carte](docs/images/prevision-j1-2026-10-15.png)

*Capture présentant les prévisions pour le 8 octobre 2026. Le taux de détection de 59 % provient de l'évaluation historique détaillée plus bas.*

<details>
<summary>Voir également l'historique hydrologique et l'analyse départementale</summary>

### Historique hydrologique

![Historique FourCasters : niveaux hydrologiques, pluie et saisonnalité](docs/images/historique-hydrologique-2026-10-12.png)

*Évolution des niveaux et des précipitations, avec des indicateurs sélectionnés sur 2012 et des séries historiques depuis 2000.*

### Analyse départementale

![Analyse départementale FourCasters : Var, sinistres ONRN et historique hydrologique](docs/images/analyse-departementale-2026-10-12.png)

*Exemple dans le Var. Les données de sinistres ONRN portent sur 1995–2021 ; les autres visuels suivent les périodes indiquées dans leurs filtres.*

</details>

## De la donnée brute aux indicateurs

Trois sources alimentent le projet :

- **[Hub'Eau](https://hubeau.eaufrance.fr/)** fournit les observations de hauteur d'eau des stations hydrométriques.
- **[Open-Meteo](https://open-meteo.com/)** apporte les précipitations et l'humidité du sol.
- **L'ONRN** fournit des données historiques de sinistres, utilisées pour compléter l'analyse territoriale.

```mermaid
flowchart LR
    A["Hub'Eau et Open-Meteo<br/>API"] --> P["Python<br/>collecte"]
    O["ONRN<br/>fichiers historiques"] --> R["BigQuery<br/>données brutes"]
    P --> R
    R --> D["SQL / dbt<br/>nettoyage et modèles"]
    D --> B["Power BI<br/>tableaux de bord"]
    D --> F["Variables ML"]
    F --> M["Régression logistique<br/>predict.py"]
    M --> Q["BigQuery<br/>prédictions"]
    Q --> B
```

Le travail a surtout consisté à **rendre ces données comparables et fiables**. Les sources n'ont ni la même fréquence ni les mêmes identifiants : il a fallu gérer les dates manquantes, les correspondances géographiques, les doublons et les corrections publiées après coup.

Pour éviter de perdre la trace des modifications, les données brutes sont historisées dans BigQuery. Les modèles dbt construisent ensuite les tables d'analyse et vérifient notamment l'unicité des clés, la cohérence des références et la qualité des observations. Les seuils hydrologiques sont calculés par station et par mois.

Les règles, incidents rencontrés et corrections apportées sont documentés dans [les contrôles de qualité](docs/corrections_qualite.md). Le code est accessible dans [`src/`](src/), [`fourcasters/models/`](fourcasters/models/) et [`fourcasters/tests/`](fourcasters/tests/).

## Une première prédiction à J+1

L'objectif de cette partie était de prédire si le niveau maximal d'une station atteindrait ou dépasserait le **95e percentile (P95)** de son historique le jour suivant. Ce seuil correspond, pour une station et un mois donnés, à un niveau situé parmi les 5 % d'observations historiques les plus élevées.

Le modèle utilise cinq variables : le niveau actuel par rapport au seuil, son évolution sur un jour, la pluie du jour, la pluie cumulée sur trois jours et l'humidité du sol. J'ai retenu une **régression logistique**, avec imputation des valeurs manquantes, standardisation et pondération des classes pour tenir compte de la rareté des dépassements.

L'évaluation enregistrée dans le [notebook ML](notebooks/ml_fourcasters.ipynb) porte sur environ **1,8 million de lignes et 247 stations** (2000–2026), avec une séparation chronologique : entraînement avant 2023 et test à partir de 2023.

| Ce qui est mesuré | Précision | Rappel | F1 |
| --- | ---: | ---: | ---: |
| Modèle, tous les dépassements | 32,6 % | 87,7 % | 47,6 % |
| Règle de persistance (« demain = aujourd'hui »), tous les dépassements | 69,9 % | 69,9 % | 69,9 % |
| Modèle, **nouveaux franchissements** du P95 | **10,6 %** | **59,19 %** | **17,9 %** |

Pour les nouveaux franchissements (une station sous le seuil à J et au-dessus à J+1), le modèle en a détecté **3 007 sur 5 080**. La règle de persistance ne repère aucun de ces passages, mais elle obtient un meilleur F1 global. La faible précision du modèle signifie aussi **beaucoup de faux positifs**.

**Précaution importante :** ces métriques proviennent d'une évaluation antérieure aux corrections de qualité des 11–12 octobre 2026. Elles n'ont pas été recalculées sur le jeu corrigé et ne constituent pas une évaluation vérifiée du fichier `.pkl` actuellement utilisé par [`predict.py`](predict.py). Les seuils ont par ailleurs été calculés sur l'ensemble de l'historique : une validation véritablement prospective devra les recalculer en n'utilisant que les données connues à chaque date.

## Une chaîne automatisée et testée

[GitHub Actions](.github/workflows/pipeline.yml) orchestre chaque jour la collecte, les transformations dbt, les contrôles qualité et le calcul des prédictions. Une étape en échec interrompt le pipeline. Des exécutions manuelles et des déclenchements après certaines modifications du code sont également possibles.

Les exécutions vérifiées du **12 octobre 2026** ont réussi **24 tests locaux**, construit **12 modèles dbt** et validé **49 tests dbt** : [contrôles locaux](https://github.com/eddy370/FourCasters/actions/runs/38227785621) · [pipeline cloud](https://github.com/eddy370/FourCasters/actions/runs/38227785704).

La couverture varie selon les jours et les contrôles qualité : les 247 stations du jeu historique ne sont pas nécessairement exploitables au quotidien. L'ingestion conserve aussi un recul de sept jours. **J+1 désigne donc le lendemain de la dernière date exploitable**, pas forcément demain à la date de consultation.

## Difficultés rencontrées et limites

Le projet a mis en évidence trois difficultés concrètes : maintenir une collecte après une interruption, conserver des données cohérentes lorsque les sources sont corrigées et évaluer correctement un modèle sur des événements rares.

Les prochaines améliorations les plus utiles seraient de réévaluer le modèle sur les données corrigées, de mesurer plus précisément la couverture quotidienne et de réaliser une validation sans information issue du futur. Un dépassement statistique du P95 n'indique pas à lui seul une inondation ou des dommages : **FourCasters est un projet pédagogique et ne constitue pas un système d'alerte officiel**.

## Explorer le code et reproduire le projet

| Besoin | Fichiers |
| --- | --- |
| Collecte et suivi des données | [`src/`](src/) · [`load_data.py`](load_data.py) |
| Transformations et tests | [`fourcasters/models/`](fourcasters/models/) · [`fourcasters/tests/`](fourcasters/tests/) |
| Entraînement et évaluation ML | [`notebooks/ml_fourcasters.ipynb`](notebooks/ml_fourcasters.ipynb) |
| Calcul quotidien des scores | [`predict.py`](predict.py) |
| Orchestration | [`run_pipeline.py`](run_pipeline.py) · [workflow GitHub Actions](.github/workflows/pipeline.yml) |
| Installation et environnement BigQuery | [`docs/reproduction.md`](docs/reproduction.md) |

Pour exécuter les tests locaux, après installation des dépendances :

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Ces tests ne nécessitent pas d'accès à Google Cloud. La reproduction complète demande une configuration BigQuery et des données historiques supplémentaires, détaillées dans le [guide de reproduction](docs/reproduction.md). Le fichier source Power BI n'est pas inclus dans le dépôt ; les captures ci-dessus présentent les principales vues.

FourCasters a été réalisé dans un cadre de formation. Les éventuelles références au SDIS présentes dans les documents initiaux de [`docs/`](docs/) correspondent à un scénario pédagogique, sans mission commandée par un client.
