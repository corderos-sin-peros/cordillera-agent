"""Pipeline end-to-end: ingesta → parser → persistencia.

Aísla errores por fuente — una fuente que falle no rompe las demás.
Filtra eventos según gobernanza de fuentes (ASSET_SOURCE_MAP).
Mantiene asset_status actualizado: baseline → eventos ordenados por fecha.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .models import EventType
from .parser import parse
from .status import detect_status, post_process_event

if TYPE_CHECKING:
    from .db import Database
    from .ingest.base import IngestBase
    from .models import Event

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Gobernanza de fuentes
# ---------------------------------------------------------------------------

ASSET_SOURCE_MAP: dict[str, list[str]] = {
    "G-21":  ["lobarnechea", "dpr_metropolitana"],
    "G-19":  ["lobarnechea", "dpr_metropolitana"],
    "G-251": ["lobarnechea", "dpr_metropolitana"],
    "G-25":  ["dpp_cordillera"],
    "G-345": ["dpp_cordillera"],
    "G-455": ["dpp_cordillera"],
    "G-465": ["dpp_cordillera"],
}

# ---------------------------------------------------------------------------
# Baseline de estado — representa realidad ANTES del primer evento conocido
# ---------------------------------------------------------------------------

BASELINE_STATUS: dict[str, dict] = {
    "G-21":  {"status": "open", "date": "2026-03-01"},
    "G-19":  {"status": "open", "date": "2026-03-01"},
    "G-251": {"status": "open", "date": "2026-03-01"},
    "G-25":  {"status": "open", "date": "2026-03-01"},
    "G-345": {"status": "open", "date": "2026-03-01"},
    "G-455": {"status": "open", "date": "2026-03-01"},
    "G-465": {"status": "open", "date": "2026-03-01"},
}


def _source_allowed(asset: str, source_id: str) -> bool:
    allowed = ASSET_SOURCE_MAP.get(asset)
    if allowed is None:
        return False
    return source_id in allowed


def apply_event_to_status(db: Database, event: Event) -> None:
    """Actualiza asset_status si el evento es más reciente que el estado actual."""
    _, confidence = detect_status(event.raw_text)
    new_status = "closed" if event.event_type == EventType.CIERRE else "open"
    updated = db.update_asset_status(
        asset=event.asset,
        status=new_status,
        updated_at=event.date_event,
        source=event.source.value,
        confidence=confidence,
        source_ref=event.source_ref,
    )
    if updated:
        logger.info(
            f"asset_status {event.asset} → {new_status} "
            f"({event.date_event.date()}, conf={confidence})"
        )


@dataclass
class PipelineResult:
    events_new: int = 0
    events_duplicate: int = 0
    events_error: int = 0
    events_filtered: int = 0
    source_errors: list[str] = None  # type: ignore

    def __post_init__(self):
        if self.source_errors is None:
            self.source_errors = []


def run_pipeline(sources: list[IngestBase], db: Database) -> PipelineResult:
    """Ejecuta el pipeline completo: baseline → ingesta → persistencia."""
    # 1. Inicializar baseline (idempotente — solo actúa si asset_status está vacío)
    db.init_baseline(BASELINE_STATUS)

    result = PipelineResult()
    for source in sources:
        source_name = type(source).__name__
        try:
            _process_source(source, source_name, db, result)
        except Exception as e:
            logger.error(f"[{source_name}] Fuente falló completamente: {e}", exc_info=True)
            result.source_errors.append(f"{source_name}: {e}")

    logger.info(
        f"Pipeline completado — nuevos={result.events_new} "
        f"duplicados={result.events_duplicate} "
        f"filtrados={result.events_filtered} "
        f"errores={result.events_error}"
    )
    return result


def _process_source(
    source: IngestBase, name: str, db: Database, result: PipelineResult
) -> None:
    logger.info(f"[{name}] Iniciando ingesta")
    messages = source.fetch()
    logger.info(f"[{name}] {len(messages)} mensajes capturados")

    for msg in messages:
        try:
            events = parse(msg)
            if not events:
                logger.debug(f"[{name}] Sin eventos en: {msg.source_ref}")
                continue

            for event in events:
                if not _source_allowed(event.asset, msg.source_id):
                    logger.info(
                        f"[{name}] DESCARTADO {event.asset} — "
                        f"source_id='{msg.source_id}' no permitido"
                    )
                    result.events_filtered += 1
                    continue

                ids = db.insert_events([event])
                eid = ids[0]
                if eid is not None:
                    event.id = eid
                    result.events_new += 1
                    logger.info(
                        f"[{name}] NUEVO {event.event_type.value} "
                        f"{event.asset} | {event.source_ref}"
                    )
                    post_process_event(db, eid)
                    apply_event_to_status(db, event)
                else:
                    result.events_duplicate += 1

        except Exception as e:
            result.events_error += 1
            logger.error(f"[{name}] Error procesando {msg.source_ref}: {e}")
