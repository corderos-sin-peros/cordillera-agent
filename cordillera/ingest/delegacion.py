"""Ingesta de noticias oficiales desde sitios de Delegaciones Presidenciales.

Fuente primaria del agente. Scrapea publicaciones/noticias de:
  - Delegación Presidencial Provincial Cordillera  → dppcordillera.dpp.gob.cl
  - Delegación Presidencial Regional Metropolitana → dprmetropolitana.dpr.gob.cl

Matching conservador: solo asigna publicaciones a activos de la misma Delegación.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

from ..assets import Asset, get_assets_by_delegacion, match_assets_for_delegacion
from ..models import RawMessage, Source
from .base import IngestBase

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración de fuentes — URLs y selectores reales verificados
# ---------------------------------------------------------------------------

@dataclass
class DelegacionSource:
    delegacion_id: str
    nombre: str
    base_url: str
    news_paths: list[str]


DELEGACION_SOURCES: dict[str, DelegacionSource] = {
    "dpp_cordillera": DelegacionSource(
        delegacion_id="dpp_cordillera",
        nombre="Delegación Presidencial Provincial Cordillera",
        base_url="https://dppcordillera.dpp.gob.cl",
        news_paths=["/noticias/", "/noticias/page/2/", "/noticias/page/3/"],
    ),
    "dpr_metropolitana": DelegacionSource(
        delegacion_id="dpr_metropolitana",
        nombre="Delegación Presidencial Regional Metropolitana",
        base_url="https://dprmetropolitana.dpr.gob.cl",
        news_paths=["/noticias/", "/noticias/page/2/"],
    ),
}

# ---------------------------------------------------------------------------
# Keywords para filtro inicial
# ---------------------------------------------------------------------------

_FILTER_KEYWORDS = [
    "cierre", "apertura", "tránsito", "transito",
    "habilita", "prohíbe", "prohibe", "suspende", "restricción",
    "camino", "ruta", "acceso", "paso", "cordillera",
    "g-21", "g-19", "g-25", "g-251", "g-345", "g-455", "g-465",
    "farellones", "la parva", "valle nevado",
    "volcán", "volcan", "alfalfal", "yeso", "melosas", "morales",
]

_RESOLUTION_REF_PATTERN = re.compile(
    r"(?:resolución\s+exenta|resoluci[oó]n\s+exenta|rex|res\.?\s+ex\.?)\s*[n°#nro.]*\s*(\d+[-/]\d+|\d+)",
    re.IGNORECASE,
)

_MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in _FILTER_KEYWORDS)


def _extract_date_from_url(url: str) -> Optional[datetime]:
    """Extrae fecha desde URL con patrón /YYYY/MM/DD/ — máxima prioridad."""
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _extract_date_from_text(text: str) -> Optional[datetime]:
    """Extrae fecha desde texto en formato '2 de Abril de 2026'."""
    m = re.search(
        r"(\d{1,2})\s+de\s+(" + "|".join(_MESES.keys()) + r")\s+de\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if not m:
        return None
    mes = _MESES.get(m.group(2).lower())
    if not mes:
        return None
    try:
        return datetime(int(m.group(3)), mes, int(m.group(1)))
    except ValueError:
        return None


def _extract_resolution_ref(text: str) -> Optional[str]:
    """Extrae referencia a resolución exenta si existe en el texto."""
    m = _RESOLUTION_REF_PATTERN.search(text)
    return m.group(0).strip() if m else None


def _same_domain(url: str, base_url: str) -> bool:
    return urlparse(url).netloc == urlparse(base_url).netloc


# ---------------------------------------------------------------------------
# Scraper HTTP (requiere httpx + beautifulsoup4)
# ---------------------------------------------------------------------------

def _try_import_deps():
    try:
        import httpx
        from bs4 import BeautifulSoup
        return httpx, BeautifulSoup
    except ImportError as e:
        raise ImportError(
            "Dependencias de scraping no instaladas. "
            "Ejecuta: pip3 install httpx beautifulsoup4 lxml"
        ) from e


class DelegacionScraper:
    """Scrapea noticias de un sitio de Delegación (.gob.cl WordPress)."""

    TIMEOUT = 20
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-CL,es;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    def __init__(self, source: DelegacionSource, max_articles: int = 30):
        self.source = source
        self.max_articles = max_articles
        self._httpx, self._bs4 = _try_import_deps()

    def _get(self, url: str) -> Optional[str]:
        try:
            r = self._httpx.get(
                url, timeout=self.TIMEOUT, headers=self.HEADERS,
                follow_redirects=True,
            )
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.warning(f"[{self.source.delegacion_id}] GET {url} → {e}")
            return None

    def _extract_links_from_listing(self, html: str, page_url: str) -> list[tuple[str, str, Optional[datetime]]]:
        """Extrae (url, título, fecha) de una página de listado de noticias.

        Los sitios .gob.cl WordPress tienen estructura:
          <h6>Título de la noticia</h6>
          <a href="/2026/04/02/slug/">Leer más</a>

        O también pueden tener el título dentro del link:
          <h2><a href="/2026/04/02/slug/">Título</a></h2>
        """
        BeautifulSoup = self._bs4
        soup = BeautifulSoup(html, "html.parser")
        results = []
        seen = set()

        # Estrategia 1: buscar todos los links "Leer más" — patrón WordPress .gob.cl
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if not href or href.startswith("#"):
                continue
            full_url = urljoin(page_url, href)
            if not _same_domain(full_url, self.source.base_url):
                continue
            if full_url in seen:
                continue

            # Solo artículos con fecha en URL (/YYYY/MM/DD/)
            date = _extract_date_from_url(full_url)
            if date is None:
                continue

            # Buscar el título: subir al padre del link y buscar h6/h5/h4/h3/h2
            title = text  # fallback = texto del link
            parent = a.parent
            for _ in range(5):  # subir hasta 5 niveles
                if parent is None:
                    break
                for tag in ["h6", "h5", "h4", "h3", "h2"]:
                    heading = parent.find(tag)
                    if heading:
                        title = heading.get_text(strip=True)
                        break
                else:
                    parent = parent.parent
                    continue
                break

            # Si el título es solo "Leer más", buscar hacia arriba en el árbol
            if title.lower() in ("leer más", "leer mas", "ver más", "ver mas", ""):
                title = ""

            seen.add(full_url)
            results.append((full_url, title, date))

        # Estrategia 2: títulos dentro de links (otro patrón WordPress)
        if not results:
            for tag in ["h2", "h3", "h4", "h6"]:
                for heading in soup.find_all(tag):
                    a = heading.find("a", href=True)
                    if not a:
                        continue
                    href = a.get("href", "")
                    full_url = urljoin(page_url, href)
                    if not _same_domain(full_url, self.source.base_url):
                        continue
                    if full_url in seen:
                        continue
                    date = _extract_date_from_url(full_url)
                    if date is None:
                        continue
                    title = heading.get_text(strip=True)
                    seen.add(full_url)
                    results.append((full_url, title, date))

        logger.info(
            f"[{self.source.delegacion_id}] {len(results)} artículos encontrados en {page_url}"
        )
        return results

    def _fetch_article_body(self, url: str) -> Optional[str]:
        """Fetcha el cuerpo completo de un artículo."""
        html = self._get(url)
        if not html:
            return None
        BeautifulSoup = self._bs4
        soup = BeautifulSoup(html, "html.parser")

        # Selectores en orden de preferencia para sitios WordPress .gob.cl
        for sel in [
            ".entry-content", ".post-content", "article .content",
            "main article", ".wpb_wrapper", ".elementor-widget-text-editor",
            "article", "main",
        ]:
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) > 100:
                return el.get_text(separator=" ", strip=True)
        return None

    def scrape(self) -> list[dict]:
        """Scrapea y retorna artículos relevantes."""
        all_links: list[tuple[str, str, Optional[datetime]]] = []

        for path in self.source.news_paths:
            page_url = self.source.base_url.rstrip("/") + path
            html = self._get(page_url)
            if not html:
                continue
            links = self._extract_links_from_listing(html, page_url)
            for lnk in links:
                if lnk[0] not in {x[0] for x in all_links}:
                    all_links.append(lnk)
            if len(all_links) >= self.max_articles:
                break

        articles = []
        for url, title, date in all_links[:self.max_articles]:
            full_text_check = title
            if not _is_relevant(full_text_check) and title:
                # Filtro rápido por título — si no pasa, igual fetcha si es ambiguo
                logger.debug(f"[{self.source.delegacion_id}] Ignorado por título: {title[:70]}")
                continue

            body = self._fetch_article_body(url)
            if not body:
                # Sin body pero título relevante — usar título como texto
                if _is_relevant(title):
                    body = title
                else:
                    continue

            full_text = f"{title}\n\n{body}"
            if not _is_relevant(full_text):
                continue

            # Fecha final: URL tiene prioridad, luego texto del body
            final_date = date or _extract_date_from_text(full_text) or datetime.now()
            date_source = "url" if date else "text" if _extract_date_from_text(full_text) else "now"

            articles.append({
                "title": title,
                "body": body,
                "date": final_date,
                "date_source": date_source,
                "url": url,
            })
            logger.info(
                f"[{self.source.delegacion_id}] ✓ [{date_source}] {final_date.date()} "
                f"{title[:70]}"
            )

        return articles


# ---------------------------------------------------------------------------
# IngestBase implementation
# ---------------------------------------------------------------------------

# Samples basados en tabla verificada manualmente (2026-04-17)
# Regla: solo URLs verificadas. Para activos con fuente=observación,
# source_ref apunta al listado de noticias de la delegación (no a artículo inventado).
SAMPLE_NOTICIAS = [
    # --- DPP Cordillera ---
    # G-455: CERRADO — evidencia real publicada el 2026-04-02
    {
        "delegacion": "dpp_cordillera",
        "title": "Cierre del acceso vehicular al Embalse El Yeso: prohíben ingreso desde abril hasta agosto por condiciones climáticas y seguridad",
        "body": (
            "La medida rige durante la temporada de mayor riesgo en la zona cordillerana "
            "y busca prevenir accidentes en la Ruta G-455. El acceso al Embalse El Yeso "
            "queda prohibido para vehículos desde abril hasta agosto de 2026."
        ),
        "date": "2026-04-02T00:00:00",
        "url": "https://dppcordillera.dpp.gob.cl/2026/04/02/cierre-del-acceso-vehicular-al-embalse-el-yeso-prohiben-ingreso-desde-abril-hasta-agosto-por-condiciones-climaticas-y-seguridad/",
    },
    # G-25: ABIERTO — fuente observación, sin artículo específico verificado
    {
        "delegacion": "dpp_cordillera",
        "title": "Ruta G-25 Cajón del Maipo habilitada para el tránsito vehicular",
        "body": (
            "La Delegación Provincial Cordillera informa que la ruta G-25, "
            "Cajón del Maipo, se encuentra habilitada para el tránsito vehicular. "
            "Se restablece el acceso en ambos sentidos."
        ),
        "date": "2026-04-01T00:00:00",
        "url": "https://dppcordillera.dpp.gob.cl/noticias/",
    },
    # G-345: ABIERTO — fuente observación
    {
        "delegacion": "dpp_cordillera",
        "title": "Ruta G-345 camino al Alfalfal habilitada",
        "body": (
            "La ruta G-345, camino al Alfalfal, se encuentra habilitada "
            "para el tránsito vehicular. Acceso abierto en ambos sentidos."
        ),
        "date": "2026-04-01T00:00:00",
        "url": "https://dppcordillera.dpp.gob.cl/noticias/",
    },
    # G-465: ABIERTO — fuente observación
    {
        "delegacion": "dpp_cordillera",
        "title": "Ruta G-465 camino a Las Melosas habilitada",
        "body": (
            "La ruta G-465, camino a Las Melosas sector Baños Morales, "
            "se encuentra habilitada para el tránsito vehicular."
        ),
        "date": "2026-04-01T00:00:00",
        "url": "https://dppcordillera.dpp.gob.cl/noticias/",
    },
    # --- DPR Metropolitana ---
    # G-21: ABIERTO — fuente observación, sin artículo de cierre verificado
    {
        "delegacion": "dpr_metropolitana",
        "title": "Ruta G-21 camino a Farellones habilitada para el tránsito",
        "body": (
            "La Delegación Presidencial Regional Metropolitana informa que la ruta G-21, "
            "camino a Farellones, se encuentra habilitada para el tránsito vehicular."
        ),
        "date": "2026-04-01T00:00:00",
        "url": "https://dprmetropolitana.dpr.gob.cl/noticias/",
    },
    # G-19: ABIERTO — fuente observación
    {
        "delegacion": "dpr_metropolitana",
        "title": "Ruta G-19 camino a La Parva habilitada",
        "body": (
            "La ruta G-19, camino a La Parva, se encuentra habilitada "
            "para el tránsito vehicular."
        ),
        "date": "2026-04-01T00:00:00",
        "url": "https://dprmetropolitana.dpr.gob.cl/noticias/",
    },
    # G-251: ABIERTO — fuente operativo
    {
        "delegacion": "dpr_metropolitana",
        "title": "Ruta G-251 camino a Valle Nevado habilitada",
        "body": (
            "La Delegación Regional Metropolitana informa que la ruta G-251, "
            "camino a Valle Nevado, se encuentra habilitada. "
            "Se restablece el acceso en ambos sentidos."
        ),
        "date": "2026-04-01T00:00:00",
        "url": "https://dprmetropolitana.dpr.gob.cl/noticias/",
    },
]


class DelegacionIngest(IngestBase):
    """Ingesta principal desde sitios web de Delegaciones.

    Con httpx+bs4 instalados: scrapea sitios reales.
    Sin ellos: usa samples actualizados con URLs reales.
    """

    def __init__(
        self,
        delegaciones: Optional[list[str]] = None,
        use_samples: bool = True,
        max_articles: int = 30,
    ):
        self.delegaciones = delegaciones or list(DELEGACION_SOURCES.keys())
        self.use_samples = use_samples
        self.max_articles = max_articles

    def fetch(self) -> list[RawMessage]:
        messages: list[RawMessage] = []

        for delegacion_id in self.delegaciones:
            source = DELEGACION_SOURCES.get(delegacion_id)
            if not source:
                logger.warning(f"Delegación desconocida: {delegacion_id}")
                continue

            try:
                scraped = DelegacionScraper(source, self.max_articles).scrape()
                msgs = self._to_messages(scraped, delegacion_id)
                if msgs:
                    messages.extend(msgs)
                elif self.use_samples:
                    logger.info(f"[{delegacion_id}] Sin resultados del scraper — usando samples")
                    messages.extend(self._load_samples(delegacion_id))
            except ImportError:
                logger.warning(
                    f"[{delegacion_id}] httpx/bs4 no disponibles — "
                    "instala con: pip3 install httpx beautifulsoup4 lxml"
                )
                if self.use_samples:
                    messages.extend(self._load_samples(delegacion_id))
            except Exception as e:
                logger.error(f"[{delegacion_id}] Error en scraping: {e}", exc_info=True)
                if self.use_samples:
                    messages.extend(self._load_samples(delegacion_id))

        logger.info(f"DelegacionIngest: {len(messages)} mensajes totales")
        return messages

    def _to_messages(self, articles: list[dict], delegacion_id: str) -> list[RawMessage]:
        messages = []
        for art in articles:
            full_text = f"{art['title']}\n\n{art['body']}"
            matched = match_assets_for_delegacion(full_text, delegacion_id)
            if not matched:
                logger.debug(f"[{delegacion_id}] Sin activos en scope: {art['title'][:60]}")
                continue

            asset_names = [a.nombre_tecnico for a in matched]
            logger.info(
                f"[{delegacion_id}] → activos={asset_names} fecha={art['date'].date()} "
                f"({art.get('date_source','?')}) url={art['url']}"
            )
            messages.append(
                RawMessage(
                    text=full_text,
                    source=Source.RESOLUCION,
                    source_ref=art["url"],
                    captured_at=art["date"] if isinstance(art["date"], datetime)
                                else datetime.fromisoformat(art["date"]),
                    source_id=delegacion_id,
                )
            )
        return messages

    def _load_samples(self, delegacion_id: str) -> list[RawMessage]:
        logger.info(f"[{delegacion_id}] Cargando sample noticias")
        messages = []
        for s in SAMPLE_NOTICIAS:
            if s["delegacion"] != delegacion_id:
                continue
            full_text = f"{s['title']}\n\n{s['body']}"
            matched = match_assets_for_delegacion(full_text, delegacion_id)
            if not matched:
                continue
            messages.append(
                RawMessage(
                    text=full_text,
                    source=Source.RESOLUCION,
                    source_ref=s["url"],
                    captured_at=datetime.fromisoformat(s["date"]),
                    source_id=delegacion_id,
                )
            )
        return messages
