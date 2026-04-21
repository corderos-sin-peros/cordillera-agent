"""Ingesta de resoluciones exentas — evidencia de Nivel 2.

Intenta recuperar y validar resoluciones exentas referenciadas
en publicaciones de las Delegaciones.

Flujo:
  1. Recibe referencia a resolución (ej: "REX N° 234-2025")
  2. Busca en Transparencia Activa de la Delegación correspondiente
  3. Valida que la resolución aplique al activo en cuestión
  4. Retorna detalles explícitos: fechas, condiciones, vigencia

Si la resolución no es pública, el evento de la noticia sigue siendo válido.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

from ..models import RawMessage, Source
from .base import IngestBase

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URLs de Transparencia Activa por Delegación
# ---------------------------------------------------------------------------

TRANSPARENCIA_URLS: dict[str, str] = {
    "dpp_cordillera": (
        "https://www.delegacioncordillera.gob.cl"
        "/transparencia-activa/resoluciones-exentas/"
    ),
    "dpr_metropolitana": (
        "https://delegacionmetropolitana.gob.cl"
        "/transparencia-activa/resoluciones-exentas/"
    ),
}

# ---------------------------------------------------------------------------
# Resultado de retrieval de resolución
# ---------------------------------------------------------------------------

@dataclass
class ResolucionResult:
    ref: str
    found: bool
    validated: bool
    url: Optional[str] = None
    raw_text: Optional[str] = None
    effective_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    condition: Optional[str] = None
    notes: str = ""


# ---------------------------------------------------------------------------
# Patterns de extracción en texto de resolución
# ---------------------------------------------------------------------------

_DATE_PATTERN = re.compile(
    r"(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto"
    r"|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})",
    re.IGNORECASE,
)
_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _parse_date_from_text(text: str) -> Optional[datetime]:
    m = _DATE_PATTERN.search(text)
    if not m:
        return None
    mes = _MONTHS.get(m.group(2).lower())
    if not mes:
        return None
    try:
        return datetime(int(m.group(3)), mes, int(m.group(1)))
    except ValueError:
        return None


def _extract_condition(text: str) -> Optional[str]:
    """Extrae condición de reapertura si está explícita en el texto."""
    patterns = [
        r"hasta\s+que\s+(.{10,80}?)[\.\;]",
        r"cuando\s+(.{10,80}?)[\.\;]",
        r"en\s+cuanto\s+(.{10,80}?)[\.\;]",
        r"mientras\s+(.{10,80}?)[\.\;]",
        r"hasta\s+nuevo\s+aviso",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return None


# ---------------------------------------------------------------------------
# ResolucionFetcher
# ---------------------------------------------------------------------------

def _try_import_deps():
    try:
        import httpx
        from bs4 import BeautifulSoup
        return httpx, BeautifulSoup
    except ImportError as e:
        raise ImportError(
            "Dependencias de scraping no instaladas. "
            "Ejecuta: pip install httpx beautifulsoup4"
        ) from e


class ResolucionFetcher:
    """Intenta recuperar y validar una resolución exenta desde Transparencia Activa."""

    TIMEOUT = 15
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; CordilleraAgent/1.0; "
            "+https://github.com/cordillera-agent)"
        ),
    }

    def __init__(self, delegacion_id: str):
        self.delegacion_id = delegacion_id
        self.base_url = TRANSPARENCIA_URLS.get(delegacion_id, "")
        self._httpx, self._bs4 = _try_import_deps()

    def _get(self, url: str) -> Optional[str]:
        try:
            r = self._httpx.get(url, timeout=self.TIMEOUT, headers=self.HEADERS, follow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.warning(f"[resoluciones/{self.delegacion_id}] GET {url} → {e}")
            return None

    def fetch(self, ref: str, asset_aliases: list[str]) -> ResolucionResult:
        """Intenta encontrar y validar la resolución referenciada.

        Args:
            ref:           Referencia textual (ej: "Resolución Exenta N° 234-2025")
            asset_aliases: Aliases del activo para validar que la resolución aplica

        Returns:
            ResolucionResult con found=True/False y validated=True/False
        """
        result = ResolucionResult(ref=ref, found=False, validated=False)

        if not self.base_url:
            result.notes = f"Delegación desconocida: {self.delegacion_id}"
            return result

        # Extraer número de resolución para buscar en la página
        num_match = re.search(r"(\d+)", ref)
        if not num_match:
            result.notes = "No se pudo extraer número de resolución"
            return result
        numero = num_match.group(1)

        html = self._get(self.base_url)
        if not html:
            result.notes = "Transparencia Activa no disponible"
            return result

        BeautifulSoup = self._bs4
        soup = BeautifulSoup(html, "html.parser")

        # Buscar link que contenga el número de resolución
        resolution_url = None
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a["href"]
            if numero in text or numero in href:
                resolution_url = urljoin(self.base_url, href)
                break

        if not resolution_url:
            result.notes = f"Resolución {ref} no encontrada públicamente (aceptable)"
            logger.info(f"[resoluciones] {ref} no pública — evento sigue válido")
            return result

        result.found = True
        result.url = resolution_url

        # Fetch del documento de resolución
        res_html = self._get(resolution_url)
        if not res_html:
            result.notes = "Link encontrado pero documento no accesible"
            return result

        res_soup = BeautifulSoup(res_html, "html.parser")
        body_text = res_soup.get_text(separator=" ", strip=True)
        result.raw_text = body_text[:2000]  # truncar para storage

        # Validar que aplica al activo
        text_lower = body_text.lower()
        if any(alias.lower() in text_lower for alias in asset_aliases):
            result.validated = True
            result.effective_date = _parse_date_from_text(body_text)
            result.condition = _extract_condition(body_text)
            logger.info(f"[resoluciones] {ref} validada para activo (aliases match)")
        else:
            result.notes = "Resolución encontrada pero no aplica al activo"
            logger.warning(f"[resoluciones] {ref} no aplica a activo — no se adjunta")

        return result


# ---------------------------------------------------------------------------
# IngestBase implementation (modo batch — resoluciones conocidas)
# ---------------------------------------------------------------------------

# Samples heredados del MVP1 para testing
SAMPLE_RESOLUCIONES = [
    {
        "ref": "REX-2025-001",
        "date": "2025-06-15T08:00:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 001 — Delegación Provincial Cordillera. "
            "Prohíbese el tránsito vehicular por Ruta G-25, sector Cajón del Maipo, "
            "desde km 28 hasta km 45, debido a condiciones climáticas adversas. "
            "Vigencia: desde las 08:00 hrs del 15 de junio de 2025."
        ),
    },
    {
        "ref": "REX-2025-002",
        "date": "2025-06-15T09:30:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 002 — Delegación Provincial Cordillera. "
            "Se prohíbe el tránsito vehicular por Camino a Las Melosas, sector "
            "Baños Morales, ruta G-465, debido a acumulación de nieve en la calzada."
        ),
    },
    {
        "ref": "REX-2025-003",
        "date": "2025-06-16T07:00:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 003 — Delegación Regional Metropolitana. "
            "Habilítase el tránsito vehicular por Ruta G-21, camino a Farellones, "
            "en horario de 08:00 a 18:00 hrs, con cadenas obligatorias."
        ),
    },
    {
        "ref": "REX-2025-004",
        "date": "2025-06-16T10:00:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 004 — Delegación Regional Metropolitana. "
            "Se cierra el acceso por ruta G-455 al Embalse El Yeso por condiciones "
            "de riesgo en el camino de acceso. Restricción total de tránsito."
        ),
    },
    {
        "ref": "REX-2025-005",
        "date": "2025-06-17T06:00:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 005 — Delegación Provincial Cordillera. "
            "Apertura de Ruta G-25, Cajón del Maipo. Se restablece el tránsito "
            "vehicular en ambos sentidos a partir de las 06:00 hrs."
        ),
    },
    {
        "ref": "REX-2025-006",
        "date": "2025-06-17T14:00:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 006 — Delegación Provincial Cordillera. "
            "Se habilita el tránsito vehicular por Ruta G-345, camino al Alfalfal, "
            "para vehículos livianos. Vehículos de carga suspendidos."
        ),
    },
    {
        "ref": "REX-2025-007",
        "date": "2025-06-18T08:00:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 007 — Delegación Regional Metropolitana. "
            "Se prohíbe el tránsito por ruta G-19, camino a La Parva, "
            "por acumulación de nieve. Se prohíbe ingreso a vehículos sin cadenas."
        ),
    },
    {
        "ref": "REX-2025-008",
        "date": "2025-06-19T09:00:00",
        "text": (
            "RESOLUCIÓN EXENTA N° 008 — Delegación Regional Metropolitana. "
            "Se cierra la ruta G-251, camino a Valle Nevado, "
            "hasta nuevo aviso por condiciones climáticas adversas."
        ),
    },
]

_FILTER_KEYWORDS = [
    "cierre", "apertura", "tránsito", "transito",
    "habilita", "prohíbe", "prohibe", "suspende",
    "camino", "ruta", "paso",
]


def _is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in _FILTER_KEYWORDS)


class ResolucionesIngest(IngestBase):
    """Ingesta batch de resoluciones exentas (Transparencia Activa).

    Complementa a DelegacionIngest cuando se tienen URLs directas
    o se quiere procesar resoluciones independientemente de noticias.
    """

    def __init__(self, urls: Optional[list[str]] = None, use_samples: bool = True):
        self.urls = urls or []
        self.use_samples = use_samples

    def fetch(self) -> list[RawMessage]:
        messages: list[RawMessage] = []

        if self.urls:
            messages.extend(self._scrape_urls())

        if not messages and self.use_samples:
            logger.info("ResolucionesIngest: usando datos de ejemplo")
            messages.extend(self._load_samples())

        logger.info(f"ResolucionesIngest: {len(messages)} mensajes")
        return messages

    def _scrape_urls(self) -> list[RawMessage]:
        try:
            httpx, BeautifulSoup = _try_import_deps()
        except ImportError:
            logger.warning("httpx/bs4 no disponibles para scraping de resoluciones")
            return []

        messages = []
        headers = {"User-Agent": "CordilleraAgent/1.0"}
        for url in self.urls:
            try:
                r = httpx.get(url, timeout=15, headers=headers, follow_redirects=True)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")

                # Buscar links a PDFs o páginas de resoluciones
                for a in soup.find_all("a", href=True):
                    text = a.get_text(strip=True)
                    if not _is_relevant(text):
                        continue
                    href = urljoin(url, a["href"])
                    messages.append(
                        RawMessage(
                            text=text,
                            source=Source.RESOLUCION,
                            source_ref=href,
                            captured_at=datetime.utcnow(),
                        )
                    )
            except Exception as e:
                logger.error(f"Error scraping resoluciones {url}: {e}")
        return messages

    def _load_samples(self) -> list[RawMessage]:
        return [
            RawMessage(
                text=r["text"],
                source=Source.RESOLUCION,
                source_ref=r["ref"],
                captured_at=datetime.fromisoformat(r["date"]),
            )
            for r in SAMPLE_RESOLUCIONES
            if _is_relevant(r["text"])
        ]
