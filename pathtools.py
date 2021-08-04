"""
Outils de manipulation et de gestion de chemin.
"""

import hashlib
import os
from pathlib import Path
from typing import Any
from typing import Optional
from typing import Union


def get_tool_path() -> Path:
    """
    Donne le chemin absolu vers le dossier contenant les outils.

    Returns:
        Path vers le dossier utils/

    """
    return Path(os.path.realpath(__file__)).parent


def data_path(base_path: Optional[Union[Path, str]] = None) -> Path:
    """
    En notant parent/utils/pathtools.py l'arborescence du fichier actuel, renvoi
    le chemin absolu vers le dossier parent/data/.
    Crée le dossier si nécessaire
    """
    path = base_path if base_path is not None else get_tool_path().parent
    path /= "data/"
    path.mkdir(parents=True, exist_ok=True)
    return path


def prod_path(base_path: Optional[Union[Path, str]] = None) -> Path:
    """
    Renvoi le chemin absolu vers le dossier {data}/prod/
    """
    path = base_path if base_path is not None else get_tool_path().parent
    path /= "data/prod/"
    path.mkdir(parents=True, exist_ok=True)
    return path


def tmp_path(base_path: Optional[Union[Path, str]] = None) -> Path:
    """Chemin vers le dossier {ce fichier}/../data/tmp. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = base_path if base_path is not None else get_tool_path().parent
    path /= "data/tmp"
    path.mkdir(parents=True, exist_ok=True)
    return path


def config_path(base_path: Optional[Union[Path, str]] = None) -> Path:
    """Chemin vers le dossier {ce fichier}/../config. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = base_path if base_path is not None else get_tool_path().parent
    path /= "config"
    path.mkdir(parents=True, exist_ok=True)
    return path


def outer_path() -> Path:
    path = get_tool_path().parent.parent
    path.mkdir(parents=True, exist_ok=True)
    return path


def hashname_from_data(data: Any) -> str:
    """
    Donne le haché des données en string python. Mauvais pour une utilisation classique du haché, mais utile pour des
    noms de fichier.

    Args:
        data: Données à hacher

    Returns:
        Une chaine de caractères.

    """
    return str(hashlib.md5(str(data).encode("UTF8")).hexdigest())


def outer_out_path() -> Path:
    """Chemin vers le dossier {ce fichier}/../../out/. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = get_tool_path().parent.parent / "out"
    path.mkdir(parents=True, exist_ok=True)
    return path


def outer_test_path() -> Path:
    """Chemin vers le dossier {ce fichier}/../../test/. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = get_tool_path().parent.parent / "tests"
    path.mkdir(parents=True, exist_ok=True)
    return path


def specific_datapath(stem: Union[str, Path], base_path: Optional[Union[Path, str]] = None):
    """
    Renvoi le chemin absolu vers le dossier {data}/stem/
    Créé les dossiers nécessaires en chemin.
    """
    path = data_path(base_path=base_path) / stem
    path.mkdir(parents=True, exist_ok=True)
    return path
