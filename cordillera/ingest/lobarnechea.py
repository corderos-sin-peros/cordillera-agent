"""Ingesta desde Municipalidad de Lo Barnechea.

Fuente autoritativa para rutas del sector oriente:
  G-21  (Camino a Farellones)
  G-19  (Camino a La Parva)
  G-251 (Camino a Valle Nevado)

URL base: https://lobarnechea.cl/Noticias/
source_id fijo: "lobarnechea"
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

from ..models import RawMessage, Source
from .base import IngestBase

logger = logging.getLogger(__name__)

BASE_URL = "https://lobarnechea.cl"
NEWS_PATHS = ["/Noticias/", "/Noticias/?page=2"]
SOURCE_ID = "lobarnechea"

_FILTER_KEYWORDS = [
    "cierre", "apertura", "tránsito", "transito",
    "habilita", "prohíbe", "prohibe", "suspende", "restricción",
    "camino", "ruta", "acceso", "paso",
    "g-21", "g-19", "g-251",
    "farellones", "la parva", "valle nevado",
    "cordillera",
]

_MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _is_relevant(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in _FILTER_KEYWORDS)


def _extract_date_from_url(url: str) -> Optional[datetime]:
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _extract_date_from_text(text: str) -> Optional[datetime]:
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


def _same_domain(url: str) -> bool:
    netloc = urlparse(url).netloc
    return netloc in ("lobarnechea.cl", "www.lobarnechea.cl")


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


class _Scraper:
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

    def __init__(self, max_articles: int = 30):
        self.max_articles = max_articles
        self._httpx, self._bs4 = _try_import_deps()

    def _get(self, url: str) -> Optional[str]:
        try:
            r = self._httpx.get(
                url, timeout=self.TIMEOUT, headers=self.HEADERS, follow_redirects=True,
            )
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.warning(f"[lobarnechea] GET {url} → {e}")
            return None

    def _extract_links(self, html: str, page_url: str) -> list[tuple[str, str, Optional[datetime]]]:
        """Extrae (url, título, fecha) del listado."""
        BeautifulSoup = self._bs4
        soup = BeautifulSoup(html, "html.parser")
        results: list[tuple[str, str, Optional[datetime]]] = []
        seen: set[str] = set()

        # Estrategia 1: links con /YYYY/MM/DD/ en URL
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href or href.startswith("#"):
                continue
            full_url = urljoin(page_url, href)
            if not _same_domain(full_url) or full_url in seen:
                continue
            date = _extract_date_from_url(full_url)
            if date is None:
                continue

            title = a.get_text(strip=True)
            parent = a.parent
            for _ in range(5):
                if parent is None:
                    break
                for tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                    heading = parent.find(tag)
                    if heading:
                        t = heading.get_text(strip=True)
                        if t and t.lower() not in ("leer más", "leer mas", "ver más", "ver mas"):
                            title = t
                        break
                else:
                    parent = parent.parent
                    continue
                break

            seen.add(full_url)
            results.append((full_url, title, date))

        # Estrategia 2: headings que contienen links
        if not results:
            for tag in ["h2", "h3", "h4", "h1"]:
                for heading in soup.find_all(tag):
                    a = heading.find("a", href=True)
                    if not a:
                        continue
                    full_url = urljoin(page_url, a["href"])
                    if not _same_domain(full_url) or full_url in seen:
                        continue
                    title = heading.get_text(strip=True)
                    date = _extract_date_from_url(full_url)
                    seen.add(full_url)
                    results.append((full_url, title, date))

        # Estrategia 3: links con texto relevante (fallback)
        if not results:
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True)
                if not _is_relevant(text):
                    continue
                full_url = urljoin(page_url, a["href"])
                if not _same_domain(full_url) or full_url in seen:
                    continue
                seen.add(full_url)
                results.append((full_url, text, None))

        logger.info(f"[lobarnechea] {len(results)} artículos encontrados en {page_url}")
        return results

    def _fetch_body(self, url: str) -> Optional[str]:
        html = self._get(url)
        if not html:
            return None
        BeautifulSoup = self._bs4
        soup = BeautifulSoup(html, "html.parser")
        for sel in [
            ".entry-content", ".post-content", "article .content",
            "main article", ".wpb_wrapper", ".elementor-widget-text-editor",
            ".article-body", ".noticia-contenido", ".content-area",
            "article", "main",
        ]:
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) > 80:
                return el.get_text(separator=" ", strip=True)
        return None

    def scrape(self) -> list[dict]:
        all_links: list[tuple[str, str, Optional[datetime]]] = []

        for path in NEWS_PATHS:
            page_url = BASE_URL.rstrip("/") + path
            html = self._get(page_url)
            if not html:
                continue
            links = self._extract_links(html, page_url)
            for lnk in links:
                if lnk[0] not in {x[0] for x in all_links}:
                    all_links.append(lnk)
            if len(all_links) >= self.max_articles:
                break

        articles = []
        for url, title, date in all_links[:self.max_articles]:
            # Filtro rápido por título
            if title and not _is_relevant(title):
                logger.debug(f"[lobarnechea] Ignorado por título: {title[:70]}")
                continue

            body = self._fetch_body(url)
            if not body:
                if title and _is_relevant(title):
                    body = title
                else:
                    continue

            full_text = f"{title}\n\n{body}" if title else body
            if not _is_relevant(full_text):
                continue

            final_date = (
                date
                or _extract_date_from_text(full_text)
                or datetime.now()
            )

            articles.append({
                "title": title or "",
                "body": body,
                "date": final_date,
                "url": url,
            })
            logger.info(f"[lobarnechea] ✓ {final_date.date()} {(title or '')[:70]}")

        return articles


class LoBarnecheaIngest(IngestBase):
    """Ingesta desde Municipalidad de Lo Barnechea.

    source_id = "lobarnechea" — autorizado en ASSET_SOURCE_MAP para G-21, G-19, G-251.
    """

    def __init__(self, max_articles: int = 30):
        self.max_articles = max_articles

    def fetch(self) -> list[RawMessage]:
        try:
            articles = _Scraper(self.max_articles).scrape()
        except ImportError:
            logger.warning(
                "[lobarnechea] httpx/bs4 no disponibles — "
                "instala con: pip3 install httpx beautifulsoup4 lxml"
            )
            return []
        except Exception as e:
            logger.error(f"[lobarnechea] Error en scraping: {e}", exc_info=True)
            return []

        messages = [
            RawMessage(
                text=f"{art['title']}\n\n{art['body']}" if art["title"] else art["body"],
                source=Source.RESOLUCION,
                source_ref=art["url"],
                captured_at=art["date"] if isinstance(art["date"], datetime)
                            else datetime.fromisoformat(art["date"]),
                source_id=SOURCE_ID,
            )
            for art in articles
        ]

        logger.info(f"[lobarnechea] {len(messages)} mensajes totales")
        return messages
