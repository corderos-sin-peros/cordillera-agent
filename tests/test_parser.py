"""Tests para parser y assets — mvp2.

Cambios respecto a mvp1:
  - Assets usan nombre_tecnico (G-25, G-345, etc.)
  - Volcán es alias de G-25; Baños Morales es alias de G-465
  - Libertadores removido del scope (pasos fronterizos fuera de RM caminos)
  - G-19 y G-251 agregados
"""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from cordillera.assets import (
    ASSETS,
    match_all_assets,
    match_asset,
    match_assets_for_delegacion,
)
from cordillera.db import Database
from cordillera.models import AssetType, EventType, RawMessage, Source
from cordillera.parser import classify_event_type, parse
from cordillera.pipeline import run_pipeline


class TestClassifyEventType(unittest.TestCase):

    def test_cierre_prohibe_transito(self):
        self.assertEqual(
            classify_event_type("Se prohíbe el tránsito vehicular"),
            EventType.CIERRE,
        )

    def test_cierre_cierre(self):
        self.assertEqual(classify_event_type("cierre total del camino"), EventType.CIERRE)

    def test_cierre_cerrado(self):
        self.assertEqual(classify_event_type("el camino está cerrado"), EventType.CIERRE)

    def test_cierre_intransitable(self):
        self.assertEqual(classify_event_type("ruta intransitable"), EventType.CIERRE)

    def test_apertura_habilita(self):
        self.assertEqual(classify_event_type("se habilita el tránsito"), EventType.APERTURA)

    def test_apertura_habilitase(self):
        self.assertEqual(classify_event_type("Habilítase el paso"), EventType.APERTURA)

    def test_apertura_apertura(self):
        self.assertEqual(classify_event_type("apertura del camino"), EventType.APERTURA)

    def test_apertura_restablece(self):
        self.assertEqual(classify_event_type("se restablece el tránsito"), EventType.APERTURA)

    def test_ambiguo_prioriza_cierre(self):
        result = classify_event_type("cierre parcial pero se habilita paso peatonal")
        self.assertEqual(result, EventType.CIERRE)

    def test_sin_evento(self):
        self.assertIsNone(classify_event_type("informe meteorológico del día"))

    def test_sin_evento_irrelevante(self):
        self.assertIsNone(classify_event_type("reunión de coordinación"))


class TestAssets(unittest.TestCase):
    """Tests para el diccionario unificado de assets."""

    def test_total_assets(self):
        self.assertEqual(len(ASSETS), 7)

    def test_all_have_delegacion(self):
        for a in ASSETS:
            self.assertIn(a.delegacion, ["dpp_cordillera", "dpr_metropolitana"])

    def test_dpp_cordillera_count(self):
        from cordillera.assets import get_assets_by_delegacion
        self.assertEqual(len(get_assets_by_delegacion("dpp_cordillera")), 4)

    def test_dpr_metropolitana_count(self):
        from cordillera.assets import get_assets_by_delegacion
        self.assertEqual(len(get_assets_by_delegacion("dpr_metropolitana")), 3)

    def test_g251_no_confunde_con_g25(self):
        result = match_asset("Ruta G-251 cerrada")
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "G-251")

    def test_g25_no_matchea_g251(self):
        matches = match_all_assets("Ruta G-251 cerrada")
        names = [m[0] for m in matches]
        self.assertNotIn("G-25", names)

    def test_banos_morales_mapea_g465(self):
        result = match_asset("Cierre sector Baños Morales")
        self.assertEqual(result[0], "G-465")

    def test_volcan_alias_mapea_g25(self):
        result = match_asset("Cierre camino al Volcán")
        self.assertEqual(result[0], "G-25")

    def test_matching_conservador_por_delegacion(self):
        # G-19 solo debe matchear en dpr_metropolitana, no en dpp_cordillera
        en_dpr = match_assets_for_delegacion("Cierre G-19 La Parva", "dpr_metropolitana")
        en_dpp = match_assets_for_delegacion("Cierre G-19 La Parva", "dpp_cordillera")
        self.assertTrue(len(en_dpr) > 0)
        self.assertEqual(len(en_dpp), 0)


class TestParse(unittest.TestCase):

    def _msg(self, text, source=Source.RESOLUCION, ref="REX-TEST"):
        return RawMessage(
            text=text,
            source=source,
            source_ref=ref,
            captured_at=datetime(2025, 6, 15, 8, 0),
        )

    def test_cierre_con_activo(self):
        events = parse(self._msg("Se prohíbe el tránsito por la Ruta G-25, sector Cajón del Maipo"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].asset, "G-25")
        self.assertEqual(events[0].event_type, EventType.CIERRE)

    def test_apertura_con_activo(self):
        events = parse(self._msg("Se habilita el tránsito por ruta G-21, camino a Farellones"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].asset, "G-21")
        self.assertEqual(events[0].event_type, EventType.APERTURA)

    def test_multiples_activos(self):
        text = "Cierre de ruta G-455 al Embalse El Yeso y ruta G-345 al Alfalfal"
        events = parse(self._msg(text))
        assets = {e.asset for e in events}
        self.assertIn("G-455", assets)
        self.assertIn("G-345", assets)
        self.assertTrue(all(e.event_type == EventType.CIERRE for e in events))

    def test_nuevos_activos_g19(self):
        events = parse(self._msg("Se prohíbe el tránsito por G-19, camino a La Parva"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].asset, "G-19")

    def test_nuevos_activos_g251(self):
        events = parse(self._msg("Apertura ruta G-251, camino a Valle Nevado"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].asset, "G-251")

    def test_fuera_de_scope_retorna_vacio(self):
        # Libertadores ya no está en scope
        events = parse(self._msg("Se habilita paso Los Libertadores"))
        self.assertEqual(len(events), 0)

    def test_sin_activo_retorna_vacio(self):
        events = parse(self._msg("Se prohíbe el tránsito en zona cordillerana"))
        self.assertEqual(len(events), 0)

    def test_sin_evento_retorna_vacio(self):
        events = parse(self._msg("Informe del estado de la Ruta G-25"))
        self.assertEqual(len(events), 0)

    def test_alias_cajon_del_maipo(self):
        events = parse(self._msg("Cierre del Cajón del Maipo por temporal"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].asset, "G-25")

    def test_trazabilidad(self):
        msg = self._msg("Se cierra ruta G-455 al Embalse El Yeso", ref="REX-2025-099")
        events = parse(msg)
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e.source_ref, "REX-2025-099")
        self.assertEqual(e.source, Source.RESOLUCION)
        self.assertIn("G-455", e.raw_text)


class TestIdempotencia(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db = Database(Path(self.tmp.name))
        self.db.connect()

    def tearDown(self):
        self.db.close()

    def _event(self, asset="G-25", ref="REX-001"):
        from cordillera.models import Event
        return Event(
            asset=asset,
            asset_type=AssetType.CAMINO,
            source=Source.RESOLUCION,
            event_type=EventType.CIERRE,
            date_event=datetime(2025, 6, 15, 8, 0),
            source_ref=ref,
            raw_text=f"Cierre de {asset}",
        )

    def test_insert_returns_id(self):
        eid = self.db.insert_event(self._event())
        self.assertIsNotNone(eid)
        self.assertIsInstance(eid, int)

    def test_duplicate_returns_none(self):
        self.db.insert_event(self._event())
        eid2 = self.db.insert_event(self._event())
        self.assertIsNone(eid2)

    def test_same_ref_different_asset_not_duplicate(self):
        eid1 = self.db.insert_event(self._event(asset="G-25", ref="REX-001"))
        eid2 = self.db.insert_event(self._event(asset="G-455", ref="REX-001"))
        self.assertIsNotNone(eid1)
        self.assertIsNotNone(eid2)

    def test_same_ref_different_event_type_not_duplicate(self):
        from cordillera.models import Event
        e1 = self._event(ref="REX-002")
        e2 = Event(
            asset="G-25", asset_type=AssetType.CAMINO, source=Source.RESOLUCION,
            event_type=EventType.APERTURA, date_event=datetime(2025, 6, 16, 8, 0),
            source_ref="REX-002", raw_text="Apertura G-25",
        )
        eid1 = self.db.insert_event(e1)
        eid2 = self.db.insert_event(e2)
        self.assertIsNotNone(eid1)
        self.assertIsNotNone(eid2)

    def test_run_twice_idempotent(self):
        from cordillera.ingest.resoluciones import ResolucionesIngest
        sources = [ResolucionesIngest(use_samples=True)]
        r1 = run_pipeline(sources, self.db)
        r2 = run_pipeline(sources, self.db)
        self.assertGreater(r1.events_new, 0)
        self.assertEqual(r2.events_new, 0)
        self.assertGreater(r2.events_duplicate, 0)
