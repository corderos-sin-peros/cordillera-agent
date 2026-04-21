"""Post-procesamiento y resolución de estado desde asset_status.

detect_status / post_process_event → operan sobre la tabla events (sin cambios).
get_current_status / get_all_statuses → leen desde asset_status (fuente de verdad).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional, Tuple

from .assets import match_asset

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Detección de estado desde texto crudo (para tabla events)
# ---------------------------------------------------------------------------

_CLOSED_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"se\s+proh[ií]be\s+(el\s+)?tr[áa]nsito",
        r"proh[ií]b[ea]se\s+(el\s+)?tr[áa]nsito",
        r"proh[ií]be\s+ingreso",
        r"prohibici[óo]n\s+de\s+ingreso",
        r"acceso\s+suspend",
        r"cierre",
        r"cerrado",
        r"se\s+cierra",
        r"intransitable",
        r"no\s+transitable",
        r"suspend[ea]",
        r"restricci[óo]n\s+total",
    ]
]

_OPEN_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"se\s+permite\s+(el\s+)?tr[áa]nsito",
        r"habil[ií]t[ae]se",
        r"se\s+habilita",
        r"habilitado",
        r"abierto",
        r"apertura",
        r"se\s+abre",
        r"reanuda",
        r"restablec",
        r"normaliza",
    ]
]


def detect_status(text: str) -> Tuple[str, float]:
    """Clasifica texto como open/closed/unknown con nivel de confianza."""
    closed_hits = sum(1 for p in _CLOSED_PATTERNS if p.search(text))
    open_hits   = sum(1 for p in _OPEN_PATTERNS   if p.search(text))

    if closed_hits == 0 and open_hits == 0:
        return ("unknown", 0.0)
    if closed_hits > 0 and open_hits == 0:
        return ("closed", round(min(1.0, 0.6 + closed_hits * 0.15), 2))
    if open_hits > 0 and closed_hits == 0:
        return ("open", round(min(1.0, 0.6 + open_hits * 0.15), 2))
    total = closed_hits + open_hits
    return ("closed", round(max(0.3, (closed_hits - open_hits) / total * 0.5 + 0.5), 2))


def detect_asset(text: str) -> Optional[str]:
    result = match_asset(text)
    return result[0] if result else None


# ---------------------------------------------------------------------------
# Post-processing sobre tabla events (sin cambios respecto a versión anterior)
# ---------------------------------------------------------------------------

def post_process_event(db, event_id: int) -> None:
    """Actualiza status + confidence en la tabla events para un evento dado."""
    assert db.conn is not None
    row = db.conn.execute(
        "SELECT id, raw_text, asset FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    if row is None:
        return

    status, confidence = detect_status(row["raw_text"])
    db.conn.execute(
        "UPDATE events SET status = ?, confidence = ? WHERE id = ?",
        (status, confidence, event_id),
    )
    db.conn.commit()
    logger.info(f"events id={event_id} asset={row['asset']} status={status} conf={confidence}")


def post_process_all(db) -> int:
    """Re-procesa todos los eventos con status=unknown en la tabla events."""
    assert db.conn is not None
    rows = db.conn.execute(
        "SELECT id, raw_text, asset FROM events WHERE status IS NULL OR status = 'unknown'"
    ).fetchall()
    updated = 0
    for row in rows:
        status, confidence = detect_status(row["raw_text"])
        if status != "unknown":
            db.conn.execute(
                "UPDATE events SET status = ?, confidence = ? WHERE id = ?",
                (status, confidence, row["id"]),
            )
            updated += 1
    if updated:
        db.conn.commit()
    logger.info(f"Post-procesamiento: {updated}/{len(rows)} eventos actualizados")
    return updated


# ---------------------------------------------------------------------------
# Estado actual — lee desde asset_status (fuente de verdad)
# ---------------------------------------------------------------------------

def get_current_status(db, asset: str, stale_days: int = 30) -> Optional[dict]:
    """Estado actual de un activo desde asset_status."""
    assert db.conn is not None
    row = db.conn.execute(
        """SELECT asset_id, status, updated_at, source, confidence, source_ref
           FROM asset_status WHERE asset_id = ?""",
        (asset,),
    ).fetchone()

    if row is None:
        return {
            "asset": asset, "status": "UNKNOWN", "source": None,
            "timestamp": None, "confidence": 0.0,
            "days_ago": None, "is_stale": None, "source_ref": None,
        }

    try:
        event_dt = datetime.fromisoformat(row["updated_at"])
        days_ago = (datetime.now() - event_dt).days
        is_stale = days_ago > stale_days
    except Exception:
        days_ago = None
        is_stale = None

    result = {
        "asset":      row["asset_id"],
        "status":     row["status"].upper(),
        "source":     row["source"],
        "timestamp":  row["updated_at"],
        "confidence": row["confidence"],
        "days_ago":   days_ago,
        "is_stale":   is_stale,
        "source_ref": row["source_ref"] or None,
    }
    stale_tag = " ⚠ VIEJO" if is_stale else ""
    logger.info(
        f"status_resolution asset={asset} status={result['status']} "
        f"updated={result['timestamp']} conf={result['confidence']}{stale_tag}"
    )
    return result


def get_all_statuses(db) -> list[dict]:
    """Estado actual de todos los activos en asset_status."""
    assert db.conn is not None
    rows = db.conn.execute(
        "SELECT asset_id FROM asset_status ORDER BY asset_id"
    ).fetchall()
    return [get_current_status(db, row["asset_id"]) for row in rows]
