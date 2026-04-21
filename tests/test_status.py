"""Tests para detect_status, detect_asset, get_current_status — mvp2.

Cambios respecto a mvp1:
  - Assets ahora usan nombre_tecnico (G-25, G-345, G-455, G-465, G-21, G-19, G-251)
  - Libertadores y Pehuenche fuera de scope (solo caminos RM)
  - Volcán como asset independiente eliminado → ahora alias de G-25
  - Baños Morales / Lo Valdés → alias de G-465
"""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from cordillera.db import Database
from cordillera.models import AssetType, EventType, Source
from cordillera.status import (
    detect_asset,
    detect_status,
    get_all_statuses,
    get_current_status,
    post_process_all,
    post_process_event,
)


class TestDetectStatus(unittest.TestCase):

    def test_closed_cierre(self):
        status, conf = detect_status("cierre de ruta")
        self.assertEqual(status, "closed")
        self.assertGreater(conf, 0.5)

    def test_closed_cerrado(self):
        status, conf = detect_status("el camino está cerrado")
        self.assertEqual(status, "closed")

    def test_closed_prohibe_transito(self):
        status, conf = detect_status("se prohíbe el tránsito vehicular")
        self.assertEqual(status, "closed")
        self.assertGreaterEqual(conf, 0.7)

    def test_closed_prohibe_ingreso(self):
        status, conf = detect_status("se prohíbe ingreso al sector")
        self.assertEqual(status, "closed")

    def test_closed_prohibicion_ingreso(self):
        status, conf = detect_status("prohibición de ingreso al camino")
        self.assertEqual(status, "closed")

    def test_closed_acceso_suspendido(self):
        status, conf = detect_status("acceso suspendido por condiciones climáticas")
        self.assertEqual(status, "closed")

    def test_open_habilitado(self):
        status, conf = detect_status("camino habilitado para el tránsito")
        self.assertEqual(status, "open")
        self.assertGreater(conf, 0.5)

    def test_open_abierto(self):
        status, conf = detect_status("el acceso está abierto")
        self.assertEqual(status, "open")

    def test_open_permite_transito(self):
        status, conf = detect_status("se permite el tránsito vehicular")
        self.assertEqual(status, "open")
        self.assertGreaterEqual(conf, 0.7)

    def test_open_apertura(self):
        status, conf = detect_status("apertura del camino a partir de hoy")
        self.assertEqual(status, "open")

    def test_unknown_irrelevant(self):
        status, conf = detect_status("informe meteorológico general del día")
        self.assertEqual(status, "unknown")
        self.assertEqual(conf, 0.0)

    def test_unknown_no_keywords(self):
        status, conf = detect_status("reunión de coordinación institucional")
        self.assertEqual(status, "unknown")

    def test_ambiguous_defaults_closed(self):
        status, conf = detect_status("cierre temporal, pero se habilita paso peatonal")
        self.assertEqual(status, "closed")
        self.assertLess(conf, 0.8)

    def test_confidence_increases_with_hits(self):
        single = detect_status("cerrado")[1]
        multiple = detect_status("cerrado, intransitable, acceso suspendido")[1]
        self.assertGreater(multiple, single)


class TestDetectAsset(unittest.TestCase):
    """detect_asset ahora retorna nombre_tecnico (G-XX)."""

    # --- dpp_cordillera ---
    def test_g25_directo(self):
        self.assertEqual(detect_asset("Ruta G-25 cerrada"), "G-25")

    def test_g25_alias_cajon(self):
        self.assertEqual(detect_asset("Cierre del Cajón del Maipo"), "G-25")

    def test_g25_alias_volcan(self):
        # "camino al volcán" es alias de G-25
        self.assertEqual(detect_asset("Cierre del camino al Volcán"), "G-25")

    def test_g345_directo(self):
        self.assertEqual(detect_asset("Ruta G-345 cerrada"), "G-345")

    def test_g345_alias_alfalfal(self):
        self.assertEqual(detect_asset("Camino al Alfalfal cerrado"), "G-345")

    def test_g455_directo(self):
        self.assertEqual(detect_asset("Ruta G-455 habilitada"), "G-455")

    def test_g455_alias_yeso(self):
        self.assertEqual(detect_asset("Acceso al Embalse El Yeso cerrado"), "G-455")

    def test_g455_alias_yeso_short(self):
        self.assertEqual(detect_asset("Camino al Yeso habilitado"), "G-455")

    def test_g465_directo(self):
        self.assertEqual(detect_asset("Ruta G-465 cerrada"), "G-465")

    def test_g465_alias_morales(self):
        self.assertEqual(detect_asset("Cierre sector Baños Morales"), "G-465")

    def test_g465_alias_melosas(self):
        self.assertEqual(detect_asset("Acceso a Las Melosas suspendido"), "G-465")

    # --- dpr_metropolitana ---
    def test_g21_directo(self):
        self.assertEqual(detect_asset("Habilitada ruta G-21"), "G-21")

    def test_g21_alias_farellones(self):
        self.assertEqual(detect_asset("Camino a Farellones abierto"), "G-21")

    def test_g19_directo(self):
        self.assertEqual(detect_asset("Cierre ruta G-19"), "G-19")

    def test_g19_alias_parva(self):
        self.assertEqual(detect_asset("Restricción en camino a La Parva"), "G-19")

    def test_g251_directo(self):
        self.assertEqual(detect_asset("G-251 habilitado"), "G-251")

    def test_g251_alias_valle_nevado(self):
        self.assertEqual(detect_asset("Apertura camino a Valle Nevado"), "G-251")

    # --- G-251 no debe matchear como G-25 ---
    def test_g251_no_confunde_con_g25(self):
        self.assertEqual(detect_asset("Ruta G-251 cerrada"), "G-251")

    # --- Fuera de scope ---
    def test_libertadores_fuera_de_scope(self):
        # Libertadores ya no está en el diccionario de activos RM
        self.assertIsNone(detect_asset("Paso Los Libertadores abierto"))

    def test_no_match_returns_none(self):
        self.assertIsNone(detect_asset("Informe meteorológico general"))

    def test_no_match_unrelated(self):
        self.assertIsNone(detect_asset("Reunión administrativa"))


class TestGetCurrentStatus(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db = Database(Path(self.tmp.name))
        self.db.connect()

    def tearDown(self):
        self.db.close()

    def _insert(self, asset, event_type, status, date="2025-06-15T08:00:00"):
        self.db.conn.execute(
            """INSERT INTO events
               (asset, asset_type, source, event_type, date_event, source_ref, raw_text, status, confidence)
               VALUES (?, 'camino', 'resolucion', ?, ?, ?, ?, ?, ?)""",
            (asset, event_type, date, f"ref-{asset}", f"texto {asset}", status, 0.8),
        )
        self.db.conn.commit()

    def test_returns_most_recent_open(self):
        self._insert("G-25", "APERTURA", "open", "2025-06-15T08:00:00")
        self._insert("G-25", "CIERRE", "closed", "2025-06-14T08:00:00")
        result = get_current_status(self.db, "G-25")
        self.assertEqual(result["status"], "OPEN")

    def test_returns_most_recent_closed(self):
        self._insert("G-455", "APERTURA", "open", "2025-06-14T08:00:00")
        self._insert("G-455", "CIERRE", "closed", "2025-06-15T10:00:00")
        result = get_current_status(self.db, "G-455")
        self.assertEqual(result["status"], "CLOSED")

    def test_no_events_returns_unknown(self):
        result = get_current_status(self.db, "G-251")
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertIsNone(result["source"])

    def test_ignores_unknown_status_rows(self):
        self._insert("G-19", "CIERRE", "unknown")
        result = get_current_status(self.db, "G-19")
        self.assertEqual(result["status"], "UNKNOWN")

    def test_get_all_statuses(self):
        self._insert("G-21", "APERTURA", "open")
        self._insert("G-19", "CIERRE", "closed")
        results = get_all_statuses(self.db)
        self.assertEqual(len(results), 2)
        by_asset = {r["asset"]: r["status"] for r in results}
        self.assertEqual(by_asset["G-21"], "OPEN")
        self.assertEqual(by_asset["G-19"], "CLOSED")


class TestPostProcess(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db = Database(Path(self.tmp.name))
        self.db.connect()

    def tearDown(self):
        self.db.close()

    def _insert_raw(self, asset, raw_text, ref="REX-test"):
        self.db.conn.execute(
            """INSERT INTO events
               (asset, asset_type, source, event_type, date_event, source_ref, raw_text)
               VALUES (?, 'camino', 'resolucion', 'CIERRE', '2025-06-15T08:00:00', ?, ?)""",
            (asset, ref, raw_text),
        )
        self.db.conn.commit()
        return self.db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def test_post_process_event_closed(self):
        eid = self._insert_raw("G-25", "Se prohíbe el tránsito por G-25", "REX-A")
        post_process_event(self.db, eid)
        row = self.db.conn.execute(
            "SELECT status, confidence FROM events WHERE id = ?", (eid,)
        ).fetchone()
        self.assertEqual(row["status"], "closed")
        self.assertGreater(row["confidence"], 0.0)

    def test_post_process_event_open(self):
        eid = self._insert_raw("G-21", "Se habilita tránsito por G-21 Farellones", "REX-B")
        post_process_event(self.db, eid)
        row = self.db.conn.execute(
            "SELECT status FROM events WHERE id = ?", (eid,)
        ).fetchone()
        self.assertEqual(row["status"], "open")

    def test_post_process_all_idempotent(self):
        eid = self._insert_raw("G-455", "Cierre ruta G-455, El Yeso", "REX-C")
        updated1 = post_process_all(self.db)
        updated2 = post_process_all(self.db)
        self.assertGreaterEqual(updated1, 1)
        self.assertEqual(updated2, 0)  # ya procesado

    def test_post_process_all_skips_already_resolved(self):
        self.db.conn.execute(
            """INSERT INTO events
               (asset, asset_type, source, event_type, date_event, source_ref, raw_text, status, confidence)
               VALUES ('G-19', 'camino', 'resolucion', 'CIERRE', '2025-06-15T08:00:00', 'REX-D', 'texto', 'closed', 0.9)"""
        )
        self.db.conn.commit()
        updated = post_process_all(self.db)
        self.assertEqual(updated, 0)
