from dataclasses import dataclass
from typing import Optional


@dataclass
class GeoInfo:
    """Wrapper pour grouper les paramètres de colonnes contenant des geométries
    """
    column: str
    table: str
    schema: str
    condition: Optional[str] = None
