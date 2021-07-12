import os
from pathlib import Path
from typing import Union

"""
Outils de manipulation et de gestion de chemin.
"""


def _get_tool_path() -> Path:
    return Path(os.path.realpath(__file__)).parent


def data_path() -> Path:
    """
    En notant parent/utils/pathtools.py l'arborescence du fichier actuel, renvoi
    le chemin absolu vers le dossier parent/data/.
    Crée le dossier si nécessaire
    """
    path = _get_tool_path().parent / "data"
    path.mkdir(parents=True, exist_ok=True)
    return path


def prod_path() -> Path:
    """
    Renvoi le chemin absolu vers le dossier {data}/prod/
    """
    path = _get_tool_path().parent / "data/prod/"
    path.mkdir(parents=True, exist_ok=True)
    return path


def tmp_path() -> Path:
    """Chemin vers le dossier {ce fichier}/../data/tmp. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = _get_tool_path().parent / "data/tmp"
    path.mkdir(parents=True, exist_ok=True)
    return path


def config_path() -> Path:
    """Chemin vers le dossier {ce fichier}/../config. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = _get_tool_path().parent / "config"
    path.mkdir(parents=True, exist_ok=True)
    return path

def outer_path() -> Path:
    path = _get_tool_path().parent.parent
    path.mkdir(parents=True, exist_ok=True)
    return path

def outer_out_path() -> Path:
    """Chemin vers le dossier {ce fichier}/../../out/. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = _get_tool_path().parent.parent / "out"
    path.mkdir(parents=True, exist_ok=True)
    return path




def outer_test_path() -> Path:
    """Chemin vers le dossier {ce fichier}/../../test/. Crée le dossier si nécessaire.

    Returns:
        Path: le chemin
    """
    path = _get_tool_path().parent.parent / "tests"
    path.mkdir(parents=True, exist_ok=True)
    return path


def specific_datapath(stem: Union[str, Path]):
    """
    Renvoi le chemin absolu vers le dossier {data}/stem/
    Créé les dossiers nécessaires en chemin.
    """
    path = data_path() / stem
    path.mkdir(parents=True, exist_ok=True)
    return path
