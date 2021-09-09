"""
Structure de description d'une table de géométries
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class GeoInfo:
    """
    Wrapper pour grouper les paramètres de colonnes contenant des geométries.
    Décris la colonne `<table_path>.<column>` sous l'éventuelle condition `condition`.

    Condition est ajouté à une clause WHERE.
    ```
        SELECT <column>
        FROM <table_path>
        (WHERE <condition>)
    ```
    """
    column: str
    table_path: str
    condition: Optional[str] = None
