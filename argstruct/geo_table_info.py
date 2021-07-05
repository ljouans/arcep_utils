from dataclasses import dataclass


@dataclass
class GeoInfo:
    """Wrapper pour grouper les paramètres de colonnes contenant des geométries
    """
    column: str
    table: str
    schema: str = "test_loic"
