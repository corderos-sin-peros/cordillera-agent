"""Modelos de dominio para Cordillera Agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class AssetType(str, Enum):
    PASO = "paso"
    CAMINO = "camino"


class Source(str, Enum):
    WHATSAPP = "whatsapp"
    RESOLUCION = "resolucion"


class EventType(str, Enum):
    APERTURA = "APERTURA"
    CIERRE = "CIERRE"


class EventCategory(str, Enum):
    """Clasificación semántica previa al mapeo de estado."""
    CLOSURE      = "closure"
    OPENING      = "opening"
    RESTRICTION  = "restriction"
    OPERATIONAL  = "operational"
    INFORMATIONAL = "informational"


@dataclass
class RawMessage:
    """Mensaje crudo capturado por cualquier módulo de ingesta."""

    text: str
    source: Source
    source_ref: str                                  # URL, ID de mensaje, etc.
    captured_at: datetime = field(default_factory=datetime.utcnow)
    source_id: str = ""                              # "dpp_cordillera" | "lobarnechea" | …


@dataclass
class Event:
    """Evento normalizado listo para persistir."""

    asset: str
    asset_type: AssetType
    source: Source
    event_type: EventType
    date_event: datetime
    source_ref: str
    raw_text: str
    id: Optional[int] = None
