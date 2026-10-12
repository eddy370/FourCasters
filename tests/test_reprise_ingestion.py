"""Dates de reprise : contrôles sans accès aux API ni à BigQuery."""

from datetime import date, timedelta
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from google.cloud import bigquery

with patch.object(bigquery, "Client"):
    from src import bigquery_utils as utils, hubeau, openmeteo


class RepriseIngestionTests(unittest.TestCase):
    def test_mart_meteo_accepte_une_journee_ancienne_absente(self):
        sql = (Path(__file__).resolve().parents[1] /
               "fourcasters/models/marts/fct_meteo_journaliere.sql").read_text()
        filtre = sql[sql.index("where date >="):sql.index("{% endif %}")]
        filtre = filtre.replace("{{ this }}", "mart_source").replace("interval 35 day", "35")
        with sqlite3.connect(":memory:") as db:
            db.create_function("date_sub", 2, lambda jour, n:
                               (date.fromisoformat(jour) - timedelta(days=n)).isoformat() if jour else None)
            db.executescript("""
                create table mart_source (row_hash text, date text);
                create table meteo_source (row_hash text, date text);
                insert into mart_source values ('ancien', '2026-08-01'), ('recent', '2026-10-04');
                insert into meteo_source values ('ancien', '2026-08-01'),
                    ('manquant', '2026-08-02'), ('recent', '2026-10-04');
            """)
            requete = "select m.row_hash from meteo_source m " + filtre
            self.assertEqual({r[0] for r in db.execute(requete)}, {"manquant", "recent"})

    def test_fenetre_recente_et_reprise_longue(self):
        fin = date(2026, 10, 4)
        for derniere, attendu in [
            (date(2026, 10, 3), date(2026, 8, 30)),
            (date(2026, 8, 1), date(2026, 8, 2)),
            (date(2026, 8, 29), date(2026, 8, 30)),
            (None, date(2026, 8, 30)),
        ]:
            with self.subTest(derniere=derniere):
                self.assertEqual(utils.calculer_date_debut_revision(derniere, fin, 35), attendu)

    def test_premier_chargement_meteo_et_borne_initiale(self):
        self.assertEqual(openmeteo.calculer_date_debut_meteo(None, date(2026, 10, 4)), "2026-08-01")
        self.assertEqual(openmeteo.calculer_date_debut_meteo(date(2026, 8, 1), date(2026, 10, 4)), "2026-08-02")
        self.assertEqual(openmeteo.calculer_date_debut_meteo(date(2026, 8, 2), date(2026, 8, 4)), "2026-08-01")

    def test_hubeau_reprise_de_la_source_sans_appel_reseau(self):
        jour = date(2026, 10, 11)
        derniere = date(2026, 8, 1)
        params_envoyes = []

        def pages(url, params, libelle):
            params_envoyes.append(params)
            return iter([])

        with patch.object(hubeau, "date") as calendrier, \
                patch.object(hubeau, "creer_table_hubeau_obs"), \
                patch.object(hubeau, "recuperer_stations_pertinentes", return_value=["S1", "S2"]), \
                patch.object(hubeau, "verifier_nombre_stations"), \
                patch.object(hubeau, "recuperer_derniere_date_hubeau", return_value=derniere), \
                patch.object(hubeau, "iterer_pages_hubeau", side_effect=pages), \
                patch.object(hubeau, "charger_dans_bigquery", return_value=0):
            calendrier.today.return_value = jour
            hubeau.executer_ingestion_hubeau_obs("test")

        self.assertEqual(params_envoyes[0]["date_debut_obs_elab"], "2026-08-02")
        self.assertEqual(params_envoyes[0]["date_fin_obs_elab"], "2026-10-04")

    def test_derniere_date_hubeau_de_la_grandeur_attendue(self):
        ligne = SimpleNamespace(derniere_date=date(2026, 10, 3))
        with patch.object(hubeau, "client") as client:
            client.query.return_value.result.return_value = [ligne]
            self.assertEqual(hubeau.recuperer_derniere_date_hubeau(), ligne.derniere_date)
            sql, = client.query.call_args.args
            params = client.query.call_args.kwargs["job_config"].query_parameters
        self.assertIn("MAX(date_obs_elab)", sql)
        self.assertEqual(params[0].value, "HIXnJ")
