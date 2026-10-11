"""Exécute le CTE météo réellement versionné avec SQLite, sans cloud.

UNIX_DATE est fourni comme fonction SQLite ; le SQL de fenêtre est conservé.
Cela vérifie les données et les cas limites, pas la compilation BigQuery/dbt.
"""

import re
import sqlite3
import unittest
from datetime import date
from pathlib import Path

RACINE = Path(__file__).resolve().parents[1]


def requete_meteo():
    sql = (RACINE / "fourcasters/models/marts/ml_features.sql").read_text()
    # Extrait les CTE météo de production, sans recopier leur calcul.
    debut = re.search(r"meteo(?:_jours)? as \(", sql).start()
    fin = sql.index("\nposition as (", debut)
    ctes = sql[debut:fin].rstrip().rstrip(",")
    ctes = ctes.replace("{{ ref('fct_meteo_journaliere') }}", "meteo_source")
    return "with " + ctes + " select code_insee, date, pluie_3j from meteo"


class PluieTroisJoursTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.create_function(
            "unix_date", 1,
            lambda jour: (date.fromisoformat(jour) - date(1970, 1, 1)).days,
        )
        self.db.execute("""
            create table meteo_source (
                code_insee text, date text, pluie_totale real,
                humidite_sol_7_28cm real
            )
        """)

    def calculer(self, lignes):
        self.db.executemany("insert into meteo_source values (?, ?, ?, ?)", lignes)
        return {(commune, jour): pluie for commune, jour, pluie
                in self.db.execute(requete_meteo())}

    def test_trois_jours_et_premieres_dates(self):
        resultats = self.calculer([
            ("37001", "2026-10-01", 1, .2),
            ("37001", "2026-10-02", 2, .2),
            ("37001", "2026-10-03", 3, .2),
        ])
        self.assertIsNone(resultats["37001", "2026-10-01"])
        self.assertIsNone(resultats["37001", "2026-10-02"])
        self.assertEqual(resultats["37001", "2026-10-03"], 6)

    def test_j_moins_un_absent(self):
        resultats = self.calculer([
            ("37001", "2026-10-01", 1, .2),
            ("37001", "2026-10-03", 3, .2),
        ])
        self.assertIsNone(resultats["37001", "2026-10-03"])

    def test_j_moins_deux_absent(self):
        resultats = self.calculer([
            ("37001", "2026-09-30", 10, .2),
            ("37001", "2026-10-02", 2, .2),
            ("37001", "2026-10-03", 3, .2),
        ])
        self.assertIsNone(resultats["37001", "2026-10-03"])

    def test_pluie_nulle_a_chacune_des_dates(self):
        for index in range(3):
            with self.subTest(index=index):
                self.db.execute("delete from meteo_source")
                lignes = [("37001", f"2026-10-0{i + 1}",
                           None if i == index else i + 1, .2) for i in range(3)]
                self.assertIsNone(self.calculer(lignes)["37001", "2026-10-03"])

    def test_pluie_zero_et_partition_par_commune(self):
        resultats = self.calculer([
            ("37001", "2026-10-01", 0, .2),
            ("37001", "2026-10-02", 0, .2),
            ("37001", "2026-10-03", 0, .2),
            ("37002", "2026-10-03", 100, .2),
        ])
        self.assertEqual(resultats["37001", "2026-10-03"], 0)
        self.assertIsNone(resultats["37002", "2026-10-03"])

    def test_doublon_detecte_et_fenetres_ecartees(self):
        # Deux copies identiques de J-1 ; le calcul ne choisit pas une copie.
        resultats = self.calculer([
            ("37001", "2026-10-01", 1, .2),
            ("37001", "2026-10-02", 2, .2),
            ("37001", "2026-10-02", 2, .2),
            ("37001", "2026-10-03", 3, .2),
            ("37001", "2026-10-04", 4, .2),
            ("37001", "2026-10-05", 5, .2),
        ])
        self.assertIsNone(resultats["37001", "2026-10-03"])
        self.assertIsNone(resultats["37001", "2026-10-04"])
        self.assertEqual(resultats["37001", "2026-10-05"], 12)
        controle = (RACINE / "fourcasters/tests/assert_fct_meteo_unique_commune_date.sql").read_text()
        controle = controle.replace("{{ ref('fct_meteo_journaliere') }}", "meteo_source")
        self.assertEqual(list(self.db.execute(controle)), [("37001", "2026-10-02", 2)])

    def test_doublon_ne_remplace_pas_un_jour_absent(self):
        resultats = self.calculer([
            ("37001", "2026-10-01", 1, .2),
            ("37001", "2026-10-01", 1, .2),
            ("37001", "2026-10-03", 3, .2),
        ])
        self.assertIsNone(resultats["37001", "2026-10-03"])


if __name__ == "__main__":
    unittest.main()
