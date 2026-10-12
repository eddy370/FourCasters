"""Exécute les règles hydro et le filtre ML de production avec SQLite."""

from pathlib import Path
import sqlite3
import unittest

RACINE = Path(__file__).resolve().parents[1]


class QualiteHydroTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.create_function("safe_divide", 2, lambda a, b: a / b if a is not None and b else None)
        self.db.executescript("""
            create table obs_source (
                row_hash text, code_station text, date_obs_elab text,
                grandeur_hydro_elab text, resultat_obs_elab real, date_prod text,
                code_statut text, libelle_statut text, code_methode text,
                libelle_methode text, code_qualification text, libelle_qualification text
            );
            create table seuils_source (
                code_station text, mois integer, p90 real, p95 real, p99 real,
                n_reference integer, n_annees_reference integer
            );
            insert into seuils_source values ('S1', 10, 5, 10, 15, 1200, 5);
        """)
        fact = (RACINE / "fourcasters/models/marts/fct_observation_hydro.sql").read_text()
        final = fact[fact.index("final as ("):fact.index("\nselect *\nfrom final")].strip()
        final = final.replace("extract(month from obs.date_obs_elab)", "cast(strftime('%m', obs.date_obs_elab) as integer)")
        ml = (RACINE / "fourcasters/models/marts/ml_features.sql").read_text()
        position = ml[ml.index("position as ("):ml.index("\n-- Variation et cible")].strip().rstrip(",")
        position = position.replace("{{ ref('fct_observation_hydro') }}", "final")
        self.requete = (
            "with obs as (select * from obs_source), "
            "seuils as (select * from seuils_source), " + final + ", " + position +
            " select date, position_p95, depasse_p95_j from position where position_p95 is not null"
        )

    def observation(self, qualification="20", statut="12", jour="2026-10-01"):
        self.db.execute("""
            insert into obs_source (row_hash, code_station, date_obs_elab,
                grandeur_hydro_elab, resultat_obs_elab, code_statut, code_qualification)
            values (?, 'S1', ?, 'HIXnJ', 12, ?, ?)
        """, (jour, jour, statut, qualification))

    def test_observations_valides_et_non_qualifiees(self):
        for qualification, statut, accepte in [
            ("20", "12", True), ("20", "16", True),
            ("10", "12", False), (None, "12", False),
            ("20", "0", False), ("20", None, False),
        ]:
            with self.subTest(qualification=qualification, statut=statut):
                self.db.execute("delete from obs_source")
                self.observation(qualification, statut)
                lignes = list(self.db.execute(self.requete))
                self.assertEqual(len(lignes), int(accepte))
                if accepte:
                    self.assertEqual(lignes[0], ("2026-10-01", .2, 1))

    def test_historique_court_et_seuils_non_exploitables(self):
        self.observation()
        self.db.execute("update seuils_source set n_annees_reference = 4")
        self.assertEqual(list(self.db.execute(self.requete)), [])
        self.db.execute("update seuils_source set n_annees_reference = 5, p99 = p90")
        self.assertEqual(list(self.db.execute(self.requete)), [])

    def test_seuil_actualise_applique_aussi_a_une_date_ancienne(self):
        self.observation(jour="2001-10-01")
        self.observation(jour="2026-10-01")
        self.assertEqual({v for _, v, _ in self.db.execute(self.requete)}, {.2})
        self.db.execute("update seuils_source set p95 = 11")
        self.assertEqual({v for _, v, _ in self.db.execute(self.requete)}, {.1})
