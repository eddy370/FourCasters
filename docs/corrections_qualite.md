# Corrections ciblées du 11/10/2026

Audit du code de `main`, commit de départ `947660c70efea6fab6ee721ec470d52ecfac2bff`.
Projet collectif Wild Code School ; architecture et classifieur conservés.

## Constats confirmés et corrections

| Point | Comportement avant correction | Cas concret | Correction minimale | Vérification attendue |
| --- | --- | --- | --- | --- |
| Pluie sur trois jours | `COUNT(pluie) = COUNT(*)` acceptait une fenêtre incomplète | J−2 = 1 mm, J−1 absent, J = 3 mm : cumul de 4 mm accepté | Une ligne par commune/date, trois dates dans la fenêtre calendaire et trois pluies renseignées | Ce cas produit `NULL` ; trois jours à 1, 2, 3 mm produisent 6 mm |
| Doublon météo | Aucun contrôle direct du grain commune/date du mart ; le cumul pouvait compter deux fois une date | J−2 = 1, J−1 = 2 deux fois, J = 3 : cumul de 8 mm | Date dupliquée rendue inexploitable dans le CTE météo ; test dbt du grain ajouté et sélectionné avant inférence | Le test retourne la clé en doublon ; les fenêtres concernées produisent `NULL` |
| Versions RAW | Recherche du hash dans tout l'historique | A → B → A : le dernier A était ignoré, B restait courant | Comparaison au hash de la dernière version de la clé métier, avec le même ordre que le staging | A → A : 1 ligne RAW ; A → B : 2 lignes, B courant ; A → B → A : 3 lignes, dernier A courant |
| Ambiguïté dans un chargement | Deux hashes différents pour une clé pouvaient être insérés avec le même timestamp | Deux valeurs météo différentes pour la même commune/date | Arrêt explicite avant insertion ; les doublons strictement identiques sont réduits à une ligne | `ValueError` sans insertion pour ce chargement ; l'ingestion conserve son rollback par `batch_id` |
| Lisibilité des dépendances | `requirements.txt` était encodé en UTF-16 avec BOM | Octets `FF FE` et caractères NUL dans une lecture UTF-8 | Conversion en UTF-8 ; toutes les versions existantes conservées | Installation complète et `pip check` réussis |

## Versions RAW et historique

- Open-Meteo : clé normalisée **code INSEE × date** ; ordre `inserted_at DESC, row_hash DESC`.
- Hub'Eau observations : clé **station × date × grandeur** ; ordre `inserted_at DESC, date_prod DESC, row_hash DESC`.
- Le SHA-256 porte toujours sur le contenu reçu avant l'ajout des colonnes techniques.
- `inserted_at`, `batch_id`, chargement `WRITE_APPEND` et annulation du lot sont conservés.
- Les anciennes versions ne sont ni effacées ni rechargées. Un ancien hash manquant provoque une nouvelle version technique lors de la prochaine réception de la clé.
- Le référentiel des stations Hub'Eau conserve son remplacement existant ; la correction concerne les RAW d'observations et de météo.
- La lecture reste limitée à la période et aux communes/stations/grandeurs reçues dans le chargement. Aucun filtrage sur les hashes reçus ne précède le choix de la dernière version.
- Le workflow du pipeline sérialise les exécutions. Des ingestions lancées manuellement en parallèle hors de ce workflow restent à éviter : lecture puis insertion ne constitue pas une transaction atomique.

## Conséquences sur le ML

Les premières journées d'une série, les trous calendaires, les pluies nulles et les dates dupliquées rendent les fenêtres concernées inexploitées. Une pluie de **0 mm** reste valide.

La jointure finale exclut déjà `pluie_3j IS NULL`. Le jeu peut donc perdre des lignes et des stations passant sous le minimum existant de 100 jours exploitables. La dernière date ou la couverture des prédictions peuvent également changer. L'ampleur réelle doit être mesurée dans BigQuery.

Les cinq variables, `target_j1`, la variation hydrologique, les seuils et le fichier du modèle sont conservés. Le calcul de la cible exige toujours une observation exactement à J+1 et garde les lignes sans cible pour l'inférence.

**Aucun entraînement, aucune métrique et aucun volume cloud n'ont été recalculés lors de cette intervention.** Les chiffres du README datés du 07/10/2026 décrivent l'état antérieur à la correction.

## Vérifications reproductibles

Vérifications réalisées le 11/10/2026 : **16 tests locaux réussis**, installation de `requirements.txt` et `pip check` réussis, analyse dbt réussie (**12 modèles, 47 tests définis**). La sélection du pipeline comprend **7 tests météo/ML**, dont le nouveau contrôle d'unicité. Ces tests dbt ont été identifiés et analysés, sans être exécutés dans BigQuery.

Depuis la racine, après installation des dépendances :

```bash
python -m unittest discover -s tests -v
pip check
```

Les 16 tests locaux couvrent les cas demandés de pluie, les séquences de versions pour les deux sources, la normalisation des clés, leur indépendance, les doublons, les clés invalides, les hashes anciens manquants et l'annulation d'un lot. Ils exécutent les expressions SQL de production avec SQLite ; seules les conversions de dialecte nécessaires sont adaptées. Ils ne prouvent pas l'exécution dans BigQuery.

Avec un profil dbt configuré, la structure peut être analysée sans lancer de modèle :

```bash
dbt parse --project-dir fourcasters --no-partial-parse
```

Après la prochaine construction des modèles dans l'environnement GCP :

```bash
dbt test --project-dir fourcasters --select ml_features fct_meteo_journaliere
```

Vérifier alors : aucun doublon commune/date ou station/date, présence de lignes à la dernière date, volumes/stations/dates de `marts.ml_features`, puis nombre et dates des prédictions dans les logs du pipeline. Une nouvelle évaluation chronologique est nécessaire avant de publier des métriques liées au jeu corrigé.

Les tests locaux sont également exécutés par `.github/workflows/validation.yml` sur les changements de code de `main`, sans secret GCP. Le traitement quotidien conserve son workflow existant.


# Corrections ciblées du 12/10/2026

Commit de départ : `bc87ba6effc011f483ede5520f18048b8dd245d7` sur `main`.

## Cas vérifiés et changements

| Point | Comportement constaté | Cas concret | Correction | Vérification |
| --- | --- | --- | --- | --- |
| Reprise météo | Une dernière date existante était ignorée au profit de la fenêtre fixe | Dernière date 01/08, fin 04/10 : reprise au 30/08, laissant 28 journées absentes | Début au plus ancien de la fenêtre de révision et du lendemain de la dernière date, avec la borne initiale conservée | Reprise au 02/08 ; une ingestion normale conserve les 35 jours de révision |
| Reprise Hub'Eau | La fenêtre de 35 jours était identique même après une longue interruption | Dernière date de la source au 01/08 après une interruption | Lire la dernière date HIXnJ de la source et rattraper depuis son lendemain | Paramètres API vérifiés localement, sans réseau |
| Mart météo | L'incrémental écartait aussi une date ancienne nouvellement récupérée | 02/08 absent du mart alors que sa dernière date est le 04/10 | Ajouter les clés absentes du mart en plus de la fenêtre récente | Le filtre SQL de production accepte le 02/08 absent et écarte une ancienne clé déjà présente |
| Qualité hydro du ML | Une observation pouvait entrer dans le ML tout en étant non classifiable dans le mart | Qualification différente de 20, statut non accepté ou quatre années de référence | Exiger `niveau_hydrologique IS NOT NULL` dans le CTE de position | Tests de qualification/statut, d'historique court et de dénominateur nul ; contrôle dbt ajouté |
| Seuils hydro | Seules les journées récentes recevaient les seuils recalculés | Une même station/mois pouvait conserver des seuils différents selon l'année | Matérialiser ce mart en table reconstruite ; calculs et colonnes conservés | Deux dates anciennes/récentes reçoivent le même seuil actualisé dans le test SQL local ; contrôle dbt station/mois ajouté |
| Dépendances | `pyproject.toml` ne déclarait que dbt | Installation du package sans déclaration des bibliothèques API et ML | Reprendre les neuf versions de `requirements.txt`, puis actualiser `uv.lock` | `pip check`, `uv lock --check` et `uv sync --locked` |
| Point d'entrée du package | La commande affichait seulement un message d'initialisation | `projet-fourcasters` affichait « Hello from… » | Appeler le `run_pipeline.py` existant depuis la racine | Package construit ; entrée conservée et chemin d'import du pipeline corrigé |

Le mart hydro reconstruit lit et écrit davantage de lignes qu'un incrémental limité à 35 jours. Ce choix simple assure des seuils cohérents dans tout l'historique ; son temps d'exécution et son coût BigQuery sont à suivre dans les exécutions. Les versions RAW, le hash de contenu, les lots et leur annulation sont conservés. Le mart météo reste incrémental.

Une station Hub'Eau sans observations RAW reçoit la fenêtre récente. Le chargement initial de son historique reste distinct. Hub’Eau utilise la dernière date disponible de la source, car une station arrêtée peut conserver une ancienne date sans anomalie. Cette reprise ne vérifie pas les trous propres à une station ou tous les trous internes d’un historique ancien : ceux-ci demandent un contrôle de couverture spécifique.

## Présentation et preuves

- README principal raccourci ; installation détaillée dans `reproduction.md`.
- Trois nouvelles captures du 12/10/2026 ajoutées sans retouche ; anciennes captures conservées.
- Commentaires de dialogue remplacés par des commentaires techniques ; modèle dbt initial documenté.
- Documents Word et scripts historiques conservés avec leur rôle expliqué. Les fichiers Word restent les documents initiaux de formation ; ils ne remplacent pas la présentation actuelle.
- Sorties, métadonnées et code antérieurs des deux notebooks conservés. Une cellule distincte d'évaluation du modèle sauvegardé est ajoutée sans exécution ni résultat annoncé.

Empreinte SHA-256 du modèle conservé :

`53dc114275320d7e16da5c105cb460599442f3d43e0d0d5376a179f4548fc205`

Les cinq variables, la cible J+1, le classifieur et ses paramètres restent les mêmes. Le filtrage hydro et la correction de pluie peuvent modifier les lignes, les stations, la date exploitable et les résultats d'inférence. **Aucune nouvelle métrique d'entraînement/test n'est publiée.**

## Vérifications

Le 12/10/2026 : **24 tests locaux réussis**, dépendances installées et vérifiées avec pip et uv, conservation des anciennes cellules et sorties des notebooks vérifiée ; analyse dbt réussie (12 modèles, 49 tests définis). Le pipeline lance désormais tous les tests dbt avant l'inférence. Sa vérification cloud est déclenchée sur les changements de code de `main`.

L'exécution cloud du [11/10/2026](https://github.com/eddy370/FourCasters/actions/runs/38133076421) avait validé les corrections précédentes : 12 modèles, 7 tests et 193 prédictions. Elle ne valide pas les changements du 12/10.

Restent à effectuer après reconstruction : réévaluer le fichier du modèle sur le jeu corrigé et, pour une évaluation prospective, reconstruire les seuils avec les seules observations passées. Dans Power BI, le sous-titre du KPI de 59 % et la lecture des scores de la capture J+1 restent à corriger dans le rapport natif.


## Résultat de la vérification cloud du 12/10/2026

Code exécuté : commit `a83f15ab12b1ad078305a9d6ff7811a67376f9ac`.

- [Tests locaux GitHub Actions](https://github.com/eddy370/FourCasters/actions/runs/38227785621) : 24 tests réussis.
- [Pipeline BigQuery/dbt](https://github.com/eddy370/FourCasters/actions/runs/38227785704) : 12 modèles construits, **49 tests réussis**, aucun test en erreur ou ignoré.
- Dernières dates RAW et `ml_features` : **05/10/2026** ; cible J+1 : **06/10/2026**.
- **12 stations et 12 prédictions insérées** pour cette dernière date. Cette faible couverture quotidienne reste une limite à analyser. Les 193 prédictions du 11/10 portaient sur le 04/10 ; les deux exécutions ne constituent pas une comparaison sur une même journée.
- Le mart hydro a été reconstruit en **22,05 secondes** dans cette exécution (volume lu annoncé par dbt : 1,9 GiB). Ce temps ne représente pas celui de l'ensemble du pipeline et ne garantit pas les exécutions futures.
- Les logs signalent deux pauses Open-Meteo pour limitation de débit et une reprise Hub'Eau après timeout ; ces mécanismes ont permis la poursuite du traitement.

Les tests de cohérence des seuils et de qualité hydro ont ainsi été exécutés dans BigQuery. Les sorties d'entraînement/test du notebook restent historiques : aucun nouveau rappel, précision ou F1 n'a été calculé.
