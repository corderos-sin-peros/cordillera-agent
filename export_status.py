#!/usr/bin/env python3
"""export_status.py — Genera data/status.json desde la DB del Cordillera Agent.

Ejecutado por GitHub Actions después del pipeline. El front lee este archivo
como fetch estático desde GitHub Pages o raw.githubusercontent.com.

Output: data/status.json
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Asegura que el módulo cordillera sea importable desde la raíz del repo
sys.path.insert(0, str(Path(__file__).parent))

from cordillera import config
from cordillera.db import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Mapeo DB → etiquetas del front
_STATUS_LABEL = {"open": "abierto", "closed": "cerrado", "unknown": "sin datos"}
_HISTORIAL_LABEL = {"open": "abierto", "closed": "cerrado", "unknown": "sin datos"}

# Número de eventos por activo en el historial
HISTORIAL_LIMIT = 5


def _fmt_date(iso: str | None) -> str:
    """Retorna solo YYYY-MM-DD desde un string ISO o vacío."""
    if not iso:
        return ""
    return iso[:10]


def build_status_json(db: Database) -> dict:
    statuses = db.get_all_asset_statuses()   # list[dict] desde asset_status
    assets_out: dict[str, dict] = {}

    for row in statuses:
        asset_id = row["asset_id"]
        status_raw = row.get("status", "unknown")
        confidence = row.get("confidence", 0.0) or 0.0
        updated_at = row.get("updated_at", "")
        source_ref = row.get("source_ref", "") or ""

        # Últimos N eventos para historial
        events = db.get_events(asset=asset_id, limit=HISTORIAL_LIMIT)
        historial = []
        for ev in events:
            ev_status = "unknown"
            if ev.get("event_type") == "APERTURA":
                ev_status = "open"
            elif ev.get("event_type") == "CIERRE":
                ev_status = "closed"

            raw = ev.get("raw_text", "")
            nota = raw[:120] + "…" if len(raw) > 120 else raw

            historial.append({
                "fecha":  _fmt_date(ev.get("date_event")),
                "status": _HISTORIAL_LABEL.get(ev_status, ev_status),
                "nota":   nota,
                "fuente": ev.get("source", ""),
                "ref":    ev.get("source_ref", ""),
            })

        # Respaldo: texto del evento más reciente (truncado)
        if events:
            raw_last = events[0].get("raw_text", "")
            respaldo = raw_last[:160] + "…" if len(raw_last) > 160 else raw_last
        else:
            respaldo = "Sin eventos registrados"

        assets_out[asset_id] = {
            "status":      _STATUS_LABEL.get(status_raw, "sin datos"),
            "statusSince": _fmt_date(updated_at),
            "confianza":   round(confidence * 100),
            "respaldo":    respaldo,
            "sourceUrl":   source_ref,
            "historial":   historial,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
    }


def main() -> None:
    db = Database(config.DB_PATH)
    db.connect()
    logger.info(f"DB: {config.DB_PATH} — {db.count()} eventos")

    payload = build_status_json(db)
    db.close()

    out_path = Path(__file__).parent / "data" / "status.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(f"Exportado → {out_path} ({len(payload['assets'])} activos)")


if __name__ == "__main__":
    main()
