#!/usr/bin/env python3
"""Cordillera Agent — MVP entry point.

Modos:
  python -m cordillera                  # batch (una ejecución)
  python -m cordillera --schedule       # continuo (loop cada POLL_INTERVAL)
  python -m cordillera status           # estado actual de cada activo
  python -m cordillera postprocess      # re-procesar eventos existentes
"""

from __future__ import annotations

import argparse
import logging
import signal
import time

from cordillera import config
from cordillera.assets import ASSETS
from cordillera.db import Database
from cordillera.ingest import DelegacionIngest, LoBarnecheaIngest, ResolucionesIngest, WhatsAppIngest
from cordillera.pipeline import run_pipeline
from cordillera.status import get_all_statuses, post_process_all

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    logging.getLogger(__name__).info("Señal recibida, cerrando...")


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)-5s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_sources() -> list:
    sources = []
    if "delegacion" in config.ACTIVE_SOURCES:
        sources.append(
            DelegacionIngest(
                delegaciones=config.ACTIVE_DELEGACIONES,
                use_samples=config.USE_SAMPLES,
                max_articles=config.MAX_ARTICLES,
            )
        )
    if "resoluciones" in config.ACTIVE_SOURCES:
        sources.append(
            ResolucionesIngest(urls=config.RESOLUCION_URLS, use_samples=config.USE_SAMPLES)
        )
    if "whatsapp" in config.ACTIVE_SOURCES:
        sources.append(WhatsAppIngest(use_samples=config.USE_SAMPLES))
    if "lobarnechea" in config.ACTIVE_SOURCES:
        sources.append(LoBarnecheaIngest(max_articles=config.MAX_ARTICLES))
    return sources


def run_once(db: Database, sources: list) -> None:
    result = run_pipeline(sources, db)
    print(
        f"  → nuevos={result.events_new} "
        f"duplicados={result.events_duplicate} "
        f"errores={result.events_error} "
        f"total_db={db.count()}"
    )
    if result.source_errors:
        for err in result.source_errors:
            print(f"  ⚠ {err}")


def cmd_run(args, db: Database) -> None:
    """Ejecutar pipeline (batch o schedule)."""
    sources = build_sources()
    logger = logging.getLogger(__name__)
    logger.info(f"Fuentes: {[type(s).__name__ for s in sources]}")

    if args.schedule:
        logger.info(f"Modo scheduler — intervalo={config.POLL_INTERVAL}s")
        while not _shutdown:
            run_once(db, sources)
            for _ in range(config.POLL_INTERVAL):
                if _shutdown:
                    break
                time.sleep(1)
    else:
        run_once(db, sources)


def cmd_status(args, db: Database) -> None:
    """Mostrar estado actual de cada activo con antigüedad del evento."""
    from cordillera import config as _cfg
    statuses = get_all_statuses(db)
    if not statuses:
        print("No hay eventos en la base de datos.")
        return

    today = __import__("datetime").datetime.now().date()
    print()
    print(f"  Hoy: {today}  (eventos más viejos de {_cfg.STALE_DAYS} días se marcan ⚠)")
    print()
    print(f"  {'Activo':<8} {'Estado':<18} {'Fecha evento':<14} {'Hace':<9} {'Fuente':<12} {'Conf.'}")
    print(f"  {'─' * 8} {'─' * 18} {'─' * 14} {'─' * 9} {'─' * 12} {'─' * 5}")
    for s in statuses:
        ts_full = s["timestamp"] or ""
        ts = ts_full[:10] if ts_full else "—"          # solo YYYY-MM-DD
        src = s["source"] or "—"
        conf = f"{s['confidence']:.2f}" if s["confidence"] else "—"
        days_ago = s.get("days_ago")
        is_stale = s.get("is_stale")

        # Símbolo de estado + alerta si es viejo
        if s["status"] == "OPEN":
            symbol = "🟢 OPEN   "
        elif s["status"] == "CLOSED":
            symbol = "🔴 CLOSED " + ("⚠" if is_stale else " ")
        else:
            symbol = "⚪ UNKNOWN"

        hace = f"{days_ago}d" if days_ago is not None else "—"
        print(f"  {s['asset']:<8} {symbol:<18} {ts:<14} {hace:<9} {src:<12} {conf}")
        url = s.get("source_ref")
        if url:
            print(f"  {'':8}   {url}")

    # Aviso resumen si hay eventos viejos
    viejos = [s for s in statuses if s.get("is_stale")]
    if viejos:
        print()
        print(f"  ⚠  {len(viejos)} activo(s) con eventos de más de {_cfg.STALE_DAYS} días — verificar fuente.")
    print()


def cmd_postprocess(args, db: Database) -> None:
    """Re-procesar eventos existentes para detectar status."""
    updated = post_process_all(db)
    print(f"  → {updated} eventos actualizados")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cordillera Agent MVP")
    sub = parser.add_subparsers(dest="command")

    # Default: run pipeline
    run_parser = sub.add_parser("run", help="Ejecutar pipeline de ingesta")
    run_parser.add_argument(
        "--schedule", action="store_true",
        help=f"Loop continuo cada {config.POLL_INTERVAL}s",
    )

    # Status
    sub.add_parser("status", help="Mostrar estado actual de cada activo")

    # Postprocess
    sub.add_parser("postprocess", help="Re-procesar eventos existentes")

    # Backwards compat: --schedule at top level
    parser.add_argument(
        "--schedule", action="store_true", dest="schedule_compat",
        help=argparse.SUPPRESS,
    )

    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    db = Database(config.DB_PATH)
    db.connect()
    logger.info(f"DB: {config.DB_PATH}")

    # Inicializar baseline para todos los activos conocidos (idempotente)
    baseline = {
        a.nombre_tecnico: {"status": "open", "date": "2026-03-01"}
        for a in ASSETS
    }
    inserted = db.init_baseline(baseline)
    if inserted:
        logger.info(f"Baseline: {inserted} activos inicializados con estado 'open'")

    if args.command == "status":
        cmd_status(args, db)
    elif args.command == "postprocess":
        cmd_postprocess(args, db)
    elif args.command == "run":
        cmd_run(args, db)
    else:
        # No subcommand: default to run (backwards compat)
        args.schedule = getattr(args, "schedule_compat", False)
        cmd_run(args, db)

    db.close()
    logger.info("Fin.")


if __name__ == "__main__":
    main()
