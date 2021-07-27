from dataclasses import dataclass
from typing import Dict


@dataclass
class TableQueryInfo:
    schema: str
    table: str
    columns: Dict[str, str]