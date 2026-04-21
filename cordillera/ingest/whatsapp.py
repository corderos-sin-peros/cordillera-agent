"""Ingesta WhatsApp — stub para MVP.

Contrato listo para integrar con Playwright/Puppeteer sobre WhatsApp Web.
Por ahora retorna datos de ejemplo para pasos fronterizos.
"""

from __future__ import annotations

import logging
from datetime import datetime

from ..models import RawMessage, Source
from .base import IngestBase

logger = logging.getLogger(__name__)

SAMPLE_WHATSAPP = [
    {
        "ref": "wa-msg-001",
        "date": "2025-06-15T07:15:00",
        "text": "🔴 Paso Los Libertadores CERRADO por temporal de nieve. Se prohíbe el tránsito hasta nuevo aviso.",
    },
    {
        "ref": "wa-msg-002",
        "date": "2025-06-17T13:45:00",
        "text": "🟢 Paso Los Libertadores ABIERTO. Se habilita tránsito para vehículos livianos con cadenas.",
    },
]


class WhatsAppIngest(IngestBase):
    """Ingesta desde canal de WhatsApp.

    MVP: retorna datos de ejemplo.
    Producción: Playwright/Puppeteer polling WhatsApp Web.
    """

    def __init__(self, use_samples: bool = True):
        self.use_samples = use_samples

    def fetch(self) -> list[RawMessage]:
        if self.use_samples:
            logger.info("WhatsApp: usando datos de ejemplo")
            return [
                RawMessage(
                    text=m["text"],
                    source=Source.WHATSAPP,
                    source_ref=m["ref"],
                    captured_at=datetime.fromisoformat(m["date"]),
                )
                for m in SAMPLE_WHATSAPP
            ]

        # TODO: implementar polling real
        # 1. Conectar a WhatsApp Web via Playwright
        # 2. Navegar al canal específico
        # 3. Leer mensajes nuevos desde último checkpoint
        # 4. Filtrar solo mensajes sobre pasos fronterizos
        logger.warning("WhatsApp polling real no implementado")
        return []
