"""Diccionario canónico de activos cordilleranos.

Fuente única de verdad para rutas, aliases, delegación y tipo.
Usado por parser, status y scraper. No duplicar en otros módulos.

Rutas en scope:
  dpp_cordillera    → G-25, G-345, G-455, G-465
  dpr_metropolitana → G-21, G-19, G-251
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional, Tuple

from .models import AssetType


@dataclass
class Asset:
    asset_id: str
    nombre_tecnico: str
    nombre: str
    delegacion: str          # "dpp_cordillera" | "dpr_metropolitana"
    asset_type: AssetType
    aliases: list[str]       # minúsculas; sin tilde opcional en alias alternativo
    estado_actual: str = "unknown"
    fecha_estado: Optional[str] = None
    notas: Optional[str] = None
    _pattern: Optional[re.Pattern] = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        sorted_aliases = sorted(self.aliases, key=len, reverse=True)
        # Lookbehind/lookahead evita match parcial: "g-25" no debe matchear en "G-251"
        self._pattern = re.compile(
            r"(?<!\w)(?:" + "|".join(re.escape(a) for a in sorted_aliases) + r")(?!\w)",
            re.IGNORECASE,
        )

    def matches(self, text: str) -> bool:
        return bool(self._pattern.search(text))

    def to_dict(self) -> dict:
        return {
            "asset_id": self.asset_id,
            "nombre_tecnico": self.nombre_tecnico,
            "nombre": self.nombre,
            "delegacion": self.delegacion,
            "asset_type": self.asset_type.value,
            "aliases": self.aliases,
            "estado_actual": self.estado_actual,
            "fecha_estado": self.fecha_estado,
        }


# ---------------------------------------------------------------------------
# Tabla canónica
# ---------------------------------------------------------------------------

ASSETS: list[Asset] = [
    # --- Delegación Presidencial Provincial Cordillera ---
    Asset(
        asset_id="g25",
        nombre_tecnico="G-25",
        nombre="Camino al Volcán",
        delegacion="dpp_cordillera",
        asset_type=AssetType.CAMINO,
        aliases=[
            "g-25", "g25", "ruta g-25", "ruta g25",
            "camino al volcán", "camino al volcan",
            "cajón del maipo", "cajon del maipo",
        ],
    ),
    Asset(
        asset_id="g345",
        nombre_tecnico="G-345",
        nombre="Camino al Alfalfal",
        delegacion="dpp_cordillera",
        asset_type=AssetType.CAMINO,
        aliases=[
            "g-345", "g345", "ruta g-345", "ruta g345",
            "camino al alfalfal", "alfalfal", "el alfalfal",
        ],
    ),
    Asset(
        asset_id="g455",
        nombre_tecnico="G-455",
        nombre="Camino al Embalse El Yeso",
        delegacion="dpp_cordillera",
        asset_type=AssetType.CAMINO,
        aliases=[
            "g-455", "g455", "ruta g-455", "ruta g455",
            "embalse el yeso", "camino al yeso",
            "acceso embalse el yeso", "el yeso", "yeso",
        ],
    ),
    Asset(
        asset_id="g465",
        nombre_tecnico="G-465",
        nombre="Camino a Las Melosas",
        delegacion="dpp_cordillera",
        asset_type=AssetType.CAMINO,
        aliases=[
            "g-465", "g465", "ruta g-465", "ruta g465",
            "las melosas", "camino a las melosas",
            "baños morales", "banos morales",
            "lo valdés", "lo valdes",
        ],
        notas="Incluye sector Baños Morales y Lo Valdés",
    ),
    # --- Delegación Presidencial Regional Metropolitana ---
    Asset(
        asset_id="g21",
        nombre_tecnico="G-21",
        nombre="Camino a Farellones",
        delegacion="dpr_metropolitana",
        asset_type=AssetType.CAMINO,
        aliases=[
            "g-21", "g21", "ruta g-21", "ruta g21",
            "camino a farellones", "farellones",
        ],
    ),
    Asset(
        asset_id="g19",
        nombre_tecnico="G-19",
        nombre="Camino a La Parva",
        delegacion="dpr_metropolitana",
        asset_type=AssetType.CAMINO,
        aliases=[
            "g-19", "g19", "ruta g-19", "ruta g19",
            "camino a la parva", "la parva",
        ],
    ),
    Asset(
        asset_id="g251",
        nombre_tecnico="G-251",
        nombre="Camino a Valle Nevado",
        delegacion="dpr_metropolitana",
        asset_type=AssetType.CAMINO,
        aliases=[
            "g-251", "g251", "ruta g-251", "ruta g251",
            "camino a valle nevado", "valle nevado",
        ],
    ),
]

# ---------------------------------------------------------------------------
# Índices
# ---------------------------------------------------------------------------

_BY_ID: dict[str, Asset] = {a.asset_id: a for a in ASSETS}
_BY_TECNICO: dict[str, Asset] = {a.nombre_tecnico: a for a in ASSETS}
_BY_DELEGACION: dict[str, list[Asset]] = {}
for _a in ASSETS:
    _BY_DELEGACION.setdefault(_a.delegacion, []).append(_a)

DELEGACIONES = list(_BY_DELEGACION.keys())


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def get_asset(asset_id: str) -> Optional[Asset]:
    return _BY_ID.get(asset_id)


def get_asset_by_tecnico(nombre_tecnico: str) -> Optional[Asset]:
    return _BY_TECNICO.get(nombre_tecnico)


def get_assets_by_delegacion(delegacion: str) -> list[Asset]:
    return _BY_DELEGACION.get(delegacion, [])


def match_asset(text: str) -> Optional[Tuple[str, AssetType]]:
    """Primer activo que matchee en el texto, o None."""
    for asset in ASSETS:
        if asset.matches(text):
            return (asset.nombre_tecnico, asset.asset_type)
    return None


def match_all_assets(text: str) -> list[Tuple[str, AssetType]]:
    """Todos los activos que matcheen en el texto."""
    return [
        (asset.nombre_tecnico, asset.asset_type)
        for asset in ASSETS
        if asset.matches(text)
    ]


def match_assets_for_delegacion(text: str, delegacion: str) -> list[Asset]:
    """Activos de una delegación específica que matcheen.

    Matching conservador: solo considera activos de la fuente emisora.
    """
    return [
        asset
        for asset in get_assets_by_delegacion(delegacion)
        if asset.matches(text)
    ]
