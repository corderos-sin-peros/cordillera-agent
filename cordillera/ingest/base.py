"""Contrato base para módulos de ingesta."""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import RawMessage


class IngestBase(ABC):
    """Interfaz que todo módulo de ingesta debe implementar."""

    @abstractmethod
    def fetch(self) -> list[RawMessage]:
        """Obtiene mensajes crudos desde la fuente.

        Returns lista de RawMessage listos para ser parseados.
        """
        ...
