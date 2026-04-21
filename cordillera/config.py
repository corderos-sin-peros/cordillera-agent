"""Configuración central — lee de variables de entorno con defaults sensatos."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

# Paths
DB_PATH = Path(os.getenv("CORDILLERA_DB_PATH", str(PROJECT_ROOT / "cordillera.db")))

# Scheduler
POLL_INTERVAL = int(os.getenv("CORDILLERA_POLL_INTERVAL", "300"))

# Fuentes activas (csv: "delegacion,resoluciones,whatsapp")
ACTIVE_SOURCES = os.getenv("CORDILLERA_SOURCES", "delegacion,lobarnechea").split(",")

# Delegaciones a monitorear (csv)
ACTIVE_DELEGACIONES = os.getenv(
    "CORDILLERA_DELEGACIONES",
    "dpp_cordillera,dpr_metropolitana",
).split(",")

# URLs de Transparencia Activa (para ResolucionesIngest directo)
RESOLUCION_URLS = [
    u.strip()
    for u in os.getenv("CORDILLERA_RESOLUCION_URLS", "").split(",")
    if u.strip()
]

# Modo sample (para dev/test — desactivar en producción)
USE_SAMPLES = os.getenv("CORDILLERA_USE_SAMPLES", "true").lower() == "true"

# Máximo artículos por Delegación por ciclo
MAX_ARTICLES = int(os.getenv("CORDILLERA_MAX_ARTICLES", "20"))

# Staleness — eventos más viejos que esto se marcan como "viejos"
STALE_DAYS = int(os.getenv("CORDILLERA_STALE_DAYS", "30"))

# Logging
LOG_LEVEL = os.getenv("CORDILLERA_LOG_LEVEL", "INFO").upper()
