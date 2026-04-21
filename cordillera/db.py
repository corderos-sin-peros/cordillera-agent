"""Capa de persistencia SQLite con idempotencia."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from .models import Event, Source

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    asset       TEXT    NOT NULL,
    asset_type  TEXT    NOT NULL CHECK (asset_type IN ('paso', 'camino')),
    source      TEXT    NOT NULL CHECK (source IN ('whatsapp', 'resolucion')),
    event_type  TEXT    NOT NULL CHECK (event_type IN ('APERTURA', 'CIERRE')),
    date_event  TEXT    NOT NULL,
    source_ref  TEXT    NOT NULL,
    raw_text    TEXT    NOT NULL,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(source_ref, asset, event_type)
);

CREATE INDEX IF NOT EXISTS idx_events_asset  ON events(asset);
CREATE INDEX IF NOT EXISTS idx_events_date   ON events(date_event);
CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);
"""

_CREATE_ASSET_STATUS = """
CREATE TABLE IF NOT EXISTS asset_status (
    asset_id    TEXT PRIMARY KEY,
    status      TEXT NOT NULL CHECK(status IN ('open', 'closed')),
    updated_at  TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'baseline',
    confidence  REAL NOT NULL DEFAULT 1.0,
    source_ref  TEXT
);
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(_CREATE_TABLE)
        self.conn.executescript(_CREATE_ASSET_STATUS)
        self._run_migrations()

    def _run_migrations(self) -> None:
        existing = {
            row[1] for row in self.conn.execute("PRAGMA table_info(events)").fetchall()
        }
        if "status" not in existing:
            self.conn.execute("ALTER TABLE events ADD COLUMN status TEXT DEFAULT 'unknown'")
            logger.info("Migración: columna 'status' agregada")
        if "confidence" not in existing:
            self.conn.execute("ALTER TABLE events ADD COLUMN confidence REAL DEFAULT 0.0")
            logger.info("Migración: columna 'confidence' agregada")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)")
        self.conn.commit()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    # ------------------------------------------------------------------
    # events
    # ------------------------------------------------------------------

    def insert_event(self, event: Event) -> Optional[int]:
        assert self.conn is not None
        try:
            cursor = self.conn.execute(
                """INSERT OR IGNORE INTO events
                   (asset, asset_type, source, event_type, date_event, source_ref, raw_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    event.asset,
                    event.asset_type.value,
                    event.source.value,
                    event.event_type.value,
                    event.date_event.isoformat(),
                    event.source_ref,
                    event.raw_text,
                ),
            )
            self.conn.commit()
            if cursor.rowcount == 0:
                logger.debug(f"Duplicado ignorado: {event.source_ref} / {event.asset}")
                return None
            return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error insertando evento: {e}")
            self.conn.rollback()
            return None

    def insert_events(self, events: list[Event]) -> list[Optional[int]]:
        return [self.insert_event(e) for e in events]

    def get_events(
        self,
        asset: Optional[str] = None,
        source: Optional[Source] = None,
        limit: int = 100,
    ) -> list[dict]:
        assert self.conn is not None
        query = "SELECT * FROM events WHERE 1=1"
        params: list = []
        if asset:
            query += " AND asset = ?"
            params.append(asset)
        if source:
            query += " AND source = ?"
            params.append(source.value)
        query += " ORDER BY date_event DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def count(self) -> int:
        assert self.conn is not None
        return self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    # ------------------------------------------------------------------
    # asset_status
    # ------------------------------------------------------------------

    def init_baseline(self, baseline: dict) -> int:
        """Inserta estado baseline para activos sin registro previo. Idempotente."""
        assert self.conn is not None
        inserted = 0
        for asset_id, data in baseline.items():
            dt = data["date"] if "T" in data["date"] else data["date"] + "T00:00:00"
            cursor = self.conn.execute(
                """INSERT OR IGNORE INTO asset_status
                   (asset_id, status, updated_at, source, confidence)
                   VALUES (?, ?, ?, 'baseline', 1.0)""",
                (asset_id, data["status"], dt),
            )
            if cursor.rowcount:
                inserted += 1
        self.conn.commit()
        if inserted:
            logger.info(f"Baseline: {inserted} activos inicializados")
        return inserted

    def update_asset_status(
        self,
        asset: str,
        status: str,
        updated_at: datetime,
        source: str,
        confidence: float,
        source_ref: str = "",
    ) -> bool:
        """Actualiza asset_status solo si el evento es más reciente. Retorna True si actualizó."""
        assert self.conn is not None
        current = self.conn.execute(
            "SELECT updated_at FROM asset_status WHERE asset_id = ?", (asset,)
        ).fetchone()

        if current:
            try:
                current_dt = datetime.fromisoformat(current["updated_at"])
                if updated_at <= current_dt:
                    logger.debug(
                        f"asset_status {asset}: {updated_at.date()} <= {current_dt.date()} — ignorado"
                    )
                    return False
            except Exception:
                pass

        self.conn.execute(
            """INSERT OR REPLACE INTO asset_status
               (asset_id, status, updated_at, source, confidence, source_ref)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (asset, status, updated_at.isoformat(), source, confidence, source_ref or ""),
        )
        self.conn.commit()
        return True

    def get_asset_status(self, asset: str) -> Optional[dict]:
        assert self.conn is not None
        row = self.conn.execute(
            "SELECT * FROM asset_status WHERE asset_id = ?", (asset,)
        ).fetchone()
        return dict(row) if row else None

    def get_all_asset_statuses(self) -> list[dict]:
        assert self.conn is not None
        rows = self.conn.execute(
            "SELECT * FROM asset_status ORDER BY asset_id"
        ).fetchall()
        return [dict(r) for r in rows]
