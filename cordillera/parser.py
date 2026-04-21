"""Parser determinístico para clasificar eventos de apertura/cierre.

Flujo:
  1. classify_category()  → EventCategory (closure / opening / restriction / operational / informational)
  2. Solo closure y opening producen eventos (los demás no cambian estado)
  3. parse()              → list[Event]
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from .assets import match_all_assets
from .models import Event, EventCategory, EventType, RawMessage

# ---------------------------------------------------------------------------
# Patterns por categoría — orden de precedencia: closure primero
# ---------------------------------------------------------------------------

_CLOSURE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"se\s+proh[ií]be\s+(el\s+)?tr[áa]nsito",
        r"proh[ií]b[ea]se\s+(el\s+)?tr[áa]nsito",
        r"proh[ií]be\s+ingreso",
        r"prohibici[óo]n\s+de\s+ingreso",
        r"acceso\s+cerrado",
        r"se\s+suspende\s+(el\s+)?tr[áa]nsito",
        r"cierre\s*(total|parcial|temporal)?",
        r"se\s+cierra",
        r"cerrado",
        r"no\s+transitable",
        r"intransitable",
        r"restricci[óo]n\s+total",
        r"acceso\s+suspend",
    ]
]

_OPENING_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"habil[ií]t[ae]se",
        r"se\s+habilita",
        r"habilitad[ao]",
        r"apertura",
        r"se\s+abre",
        r"abierto",
        r"reanudar",
        r"reanuda",
        r"restablec",
        r"normaliza",
        r"permite\s+(el\s+)?tr[áa]nsito",
    ]
]

_RESTRICTION_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"restricci[óo]n\s+de\s+acceso",
        r"solo\s+residentes",
        r"horario\s+limitado",
        r"horario\s+restringido",
        r"acceso\s+restringido",
        r"solo\s+veh[íi]culos\s+(livianos|autorizados)",
    ]
]

_OPERATIONAL_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"trabajos?\s+(de\s+)?(mantenci[óo]n|vialidad|emergencia)",
        r"operativo\s+(de\s+)?(seguridad|vialidad)",
        r"medidas?\s+preventivas?",
        r"fiscalizaci[óo]n",
        r"control\s+de\s+acceso",
    ]
]

# Solo estas categorías generan cambio de estado
_STATE_CHANGING = {EventCategory.CLOSURE, EventCategory.OPENING}

_CATEGORY_TO_EVENT_TYPE: dict[EventCategory, EventType] = {
    EventCategory.CLOSURE: EventType.CIERRE,
    EventCategory.OPENING: EventType.APERTURA,
}


def classify_category(text: str) -> EventCategory:
    """Clasifica el texto en una categoría semántica.

    Precedencia: closure > opening > restriction > operational > informational.
    Ambiguo (ambos presentes) → closure (conservador).
    """
    if any(p.search(text) for p in _CLOSURE_PATTERNS):
        return EventCategory.CLOSURE
    if any(p.search(text) for p in _OPENING_PATTERNS):
        return EventCategory.OPENING
    if any(p.search(text) for p in _RESTRICTION_PATTERNS):
        return EventCategory.RESTRICTION
    if any(p.search(text) for p in _OPERATIONAL_PATTERNS):
        return EventCategory.OPERATIONAL
    return EventCategory.INFORMATIONAL


def parse(message: RawMessage) -> list[Event]:
    """Parsea un mensaje crudo y retorna 0..N eventos normalizados.

    Solo genera eventos para categorías que cambian estado (closure, opening).
    Restriction / operational / informational retornan [].
    """
    category = classify_category(message.text)
    if category not in _STATE_CHANGING:
        return []

    assets = match_all_assets(message.text)
    if not assets:
        return []

    event_type = _CATEGORY_TO_EVENT_TYPE[category]
    return [
        Event(
            asset=canonical,
            asset_type=asset_type,
            source=message.source,
            event_type=event_type,
            date_event=message.captured_at,
            source_ref=message.source_ref,
            raw_text=message.text,
        )
        for canonical, asset_type in assets
    ]
