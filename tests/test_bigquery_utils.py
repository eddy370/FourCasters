"""Tests des versions RAW : SDK installé, client remplacé, SQL exécuté localement.

SQLite conserve le ROW_NUMBER et ses critères. Les conversions de dialecte
STRING/DATE sont adaptées explicitement. Aucun appel réseau ni clé GCP.
"""

import re
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from google.cloud import bigquery

# Le client de production est initialisé à l'import : neutralise l'authentification.
with patch.object(bigquery, "Client"):
    from src import bigquery_utils as utils


class ClientLocal:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.create_function("lpad", 3, lambda s, n, c: None if s is None else s.rjust(n, c)[:n])
        self.db.execute("""
            create table raw (
                code_INSEE text, date text, rain_sum real,
                code_station text, date_obs_elab text, grandeur_hydro_elab text,
                resultat_obs_elab real, date_prod text,
                row_hash text, inserted_at text, batch_id text
            )
        """)
        self.requetes = []
        self.chargements = []

    def query(self, sql, job_config):
        self.requetes.append(sql)
        sql = sql.replace("`test.raw`", "raw")
        sql = re.sub(r"SAFE_CAST\((\w+) AS DATE\)", r"substr(\1, 1, 10)", sql)
        sql = sql.replace("AS STRING", "AS TEXT")
        parametres = {}
        for p in job_config.query_parameters:
            if isinstance(p, bigquery.ArrayQueryParameter):
                noms = [f"{p.name}_{i}" for i in range(len(p.values))]
                sql = sql.replace(f"UNNEST(@{p.name})", "(" + ", ".join(":" + n for n in noms) + ")")
                parametres.update(zip(noms, p.values))
            else:
                parametres[p.name] = str(p.value)
        lignes = [bigquery.Row(tuple(r), {nom: i for i, nom in enumerate(r.keys())})
                  for r in self.db.execute(sql, parametres)]
        return SimpleNamespace(result=lambda: lignes)

    def get_table(self, table_id):
        return SimpleNamespace(schema=[])

    def load_table_from_json(self, lignes, table_id, job_config):
        assert job_config.write_disposition == "WRITE_APPEND"
        self.chargements.append(lignes)
        for ligne in lignes:
            colonnes = ", ".join(ligne)
            valeurs = ", ".join("?" for _ in ligne)
            self.db.execute(f"insert into raw ({colonnes}) values ({valeurs})", tuple(ligne.values()))
        return SimpleNamespace(result=lambda: None)


def meteo(valeur, commune="37001", jour="2026-10-01"):
    return {"code_INSEE": commune, "date": jour, "rain_sum": valeur}


def hydro(valeur, station="S1", grandeur="HIXnJ", jour="2026-10-01", production="2026-10-02T12:00:00Z"):
    return {"code_station": station, "date_obs_elab": jour,
            "grandeur_hydro_elab": grandeur, "resultat_obs_elab": valeur,
            "date_prod": production}


class VersionsRawTests(unittest.TestCase):
    def setUp(self):
        self.client = ClientLocal()
        self.addCleanup(self.client.db.close)
        remplacement = patch.object(utils, "client", self.client)
        remplacement.start()
        self.addCleanup(remplacement.stop)

    def charger(self, lignes, colonne_date="date", batch_id="lot"):
        return utils.charger_dans_bigquery(lignes, "test.raw", colonne_date, batch_id)

    def test_a_a_a_b_a_et_rejeu_pour_les_deux_sources(self):
        for construire, colonne_date in [(meteo, "date"), (hydro, "date_obs_elab")]:
            with self.subTest(source=colonne_date):
                self.client.db.execute("delete from raw")
                a, b = construire(1), construire(2)
                self.assertEqual(self.charger([a], colonne_date, "a1"), 1)
                self.assertEqual(self.charger([a], colonne_date, "a2"), 0)
                self.assertEqual(self.charger([b], colonne_date, "b"), 1)
                courants = utils.recuperer_hashes_existants("test.raw", colonne_date, [b])
                self.assertEqual(courants[utils.cle_metier(b, colonne_date)], utils.calculer_hash(b))
                self.assertEqual(self.charger([a], colonne_date, "a3"), 1)
                self.assertEqual(self.charger([a], colonne_date, "a4"), 0)
                courants = utils.recuperer_hashes_existants("test.raw", colonne_date, [a])
                self.assertEqual(courants[utils.cle_metier(a, colonne_date)], utils.calculer_hash(a))
                historique = list(self.client.db.execute("select row_hash, batch_id, inserted_at from raw order by inserted_at"))
                self.assertEqual([r["batch_id"] for r in historique], ["a1", "b", "a3"])
                self.assertEqual([r["row_hash"] for r in historique], [utils.calculer_hash(a), utils.calculer_hash(b), utils.calculer_hash(a)])
                self.assertTrue(all(r["inserted_at"] for r in historique))

    def test_cles_independantes(self):
        # Une ancienne clé invalide, hors des entités reçues, ne bloque pas le lot.
        self.client.db.execute("insert into raw (date, date_obs_elab) values ('2026-10-01', '2026-10-01')")
        self.assertEqual(self.charger([meteo(1), meteo(1, commune="37002"), meteo(1, jour="2026-10-02")]), 3)
        self.assertEqual(self.charger([meteo(2)]), 1)
        self.assertEqual(self.charger([meteo(1, commune="37002"), meteo(1, jour="2026-10-02")]), 0)
        self.assertEqual(self.charger([hydro(1), hydro(1, station="S2"), hydro(1, grandeur="QmnJ")], "date_obs_elab"), 3)
        self.assertEqual(self.charger([hydro(2)], "date_obs_elab"), 1)
        self.assertEqual(self.charger([hydro(1, station="S2"), hydro(1, grandeur="QmnJ")], "date_obs_elab"), 0)

    def test_doublons_identiques_et_contradictions(self):
        for construire, colonne_date in [(meteo, "date"), (hydro, "date_obs_elab")]:
            with self.subTest(source=colonne_date):
                self.assertEqual(self.charger([construire(1), construire(1)], colonne_date), 1)
                nb_avant = len(self.client.chargements)
                with self.assertRaisesRegex(ValueError, "Versions contradictoires"):
                    self.charger([construire(2), construire(3)], colonne_date)
                self.assertEqual(len(self.client.chargements), nb_avant)

    def test_cles_manquantes_date_invalide_et_source_inconnue(self):
        for ligne in [meteo(1, commune=None), meteo(1, jour=None),
                      meteo(1, jour="invalide"), meteo(1, commune="123456")]:
            with self.subTest(ligne=ligne), self.assertRaises(ValueError):
                self.charger([ligne])
        with self.assertRaises(ValueError):
            self.charger([{"autre_date": "2026-10-01"}], "autre_date")
        self.assertEqual(self.client.chargements, [])

    def test_lot_vide_sans_requete(self):
        self.assertEqual(self.charger([]), 0)
        self.assertEqual(self.client.requetes, [])

    def test_normalisation_comme_staging(self):
        self.charger([meteo(1, commune=" 1234 ")])
        self.assertEqual(self.charger([meteo(1, commune="01234")]), 1)
        self.assertEqual(self.charger([meteo(1, commune="01234")]), 0)
        self.assertEqual(self.client.db.execute("select count(*) from raw").fetchone()[0], 2)
        self.assertEqual(utils.cle_metier(hydro(1, station=" S1 ", grandeur=" HIXnJ "), "date_obs_elab"), ("S1", "2026-10-01", "HIXnJ"))

    def test_hash_manquant_sur_ancienne_ligne(self):
        self.client.db.execute("insert into raw (code_INSEE, date, rain_sum) values ('37001', '2026-10-01', 1)")
        self.assertEqual(self.charger([meteo(1)]), 1)
        self.assertEqual(self.charger([meteo(1)]), 0)

    def test_annulation_retablit_la_version_precedente(self):
        for construire, colonne_date in [(meteo, "date"), (hydro, "date_obs_elab")]:
            with self.subTest(source=colonne_date):
                self.client.db.execute("delete from raw")
                a, b = construire(1), construire(2)
                self.charger([a], colonne_date, "ancien")
                self.charger([b], colonne_date, "a_annuler")
                utils.annuler_le_lot("test.raw", "a_annuler")
                self.assertEqual(self.charger([a], colonne_date), 0)
                self.assertEqual(self.client.db.execute("select count(*) from raw").fetchone()[0], 1)

    def test_departage_hubeau_identique_au_staging(self):
        # Même inserted_at : date_prod, puis row_hash, départagent les versions.
        a, b = hydro(1, production="2026-10-02T10:00:00Z"), hydro(2)
        lignes = utils.enrichir_lignes([a, b], "lot")
        self.client.load_table_from_json(lignes, "test.raw", bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
        courants = utils.recuperer_hashes_existants("test.raw", "date_obs_elab", [a])
        self.assertEqual(courants[utils.cle_metier(a, "date_obs_elab")], utils.calculer_hash(b))


if __name__ == "__main__":
    unittest.main()
