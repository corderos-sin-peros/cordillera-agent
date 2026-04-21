# Cordillera Agent — MVP

Captura eventos de apertura y cierre de caminos cordilleranos (RM) desde fuentes oficiales.

## Fuentes activas

| Fuente | Activos | Estado |
|--------|---------|--------|
| DPP Cordillera (Transparencia Activa) | G-25, G-345, G-455, G-465 | ✅ Activo |
| Lo Barnechea (Transparencia Activa) | G-21, G-19, G-251 | ✅ Activo |
| WhatsApp (pasos fronterizos) | — | ⏸ Pendiente |

## Output

Genera `data/status.json` cada 6 horas vía GitHub Actions.
El front lo consume como archivo estático.

## Estructura

```
cordillera/          # módulo principal
  ingest/            # scrapers por fuente
  assets.py          # diccionario canónico de rutas
  db.py              # capa SQLite
  parser.py          # extracción de eventos desde texto
  pipeline.py        # orquestador
  status.py          # resolución de estado actual
export_status.py     # genera data/status.json
data/status.json     # output consumido por el front
.github/workflows/   # GitHub Actions (cron cada 6h)
```

## Correr local

```bash
pip install -r requirements.txt

# Pipeline real (scraping)
CORDILLERA_USE_SAMPLES=false python -m cordillera run

# Pipeline con datos de muestra (dev)
python -m cordillera run

# Ver estado actual
python -m cordillera status

# Exportar status.json
python export_status.py
```

## Variables de entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `CORDILLERA_USE_SAMPLES` | `true` | `false` para scraping real |
| `CORDILLERA_SOURCES` | `delegacion,lobarnechea` | Fuentes activas |
| `CORDILLERA_MAX_ARTICLES` | `20` | Artículos por fuente por ciclo |
| `CORDILLERA_STALE_DAYS` | `30` | Días hasta marcar estado como viejo |
| `CORDILLERA_DB_PATH` | `cordillera.db` | Ruta a la base de datos |
| `CORDILLERA_LOG_LEVEL` | `INFO` | Nivel de logging |
