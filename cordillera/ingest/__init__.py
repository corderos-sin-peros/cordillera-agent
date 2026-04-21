from .base import IngestBase
from .delegacion import DelegacionIngest
from .lobarnechea import LoBarnecheaIngest
from .resoluciones import ResolucionesIngest
from .whatsapp import WhatsAppIngest

__all__ = [
    "IngestBase",
    "DelegacionIngest",
    "LoBarnecheaIngest",
    "ResolucionesIngest",
    "WhatsAppIngest",
]
