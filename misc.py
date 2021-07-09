import os
import pickle
import random
import sys
from pathlib import Path
from typing import Any, Iterable, Optional, Sized, Union

import tqdm

CuPath = Optional[Union[str, Path]]


def _clean_path(filepath: CuPath = None, basepath: CuPath = None, filename: CuPath = None) -> Path:  # type: ignore
    if filepath is not None:
        return Path(filepath)
    elif basepath is not None and filename is not None:
        return Path(basepath) / filename
    else:
        print("Error. Please provide at least a filename or a filepath in {save} function.")
        sys.exit()


def save(
        data: Any,
        filename: Optional[str] = None,
        basepath: Optional[Union[str, Path]] = None,
        filepath: Union[str, Path] = None,
):
    """Raccourcis pour sauvegarder des données en utilisant pickle.

    Args:
        data (Any): Donnée à sauvegarder. 
        filename (Optional[str], optional): Nom de fichier. Defaults to None.
        basepath (Optional[Union[str, Path]], optional): Dossier de sauvegarde. Si laissé à None 
                alors que filename est spécifié, utilisera {pathtools.data()}. Defaults to None.
        filepath (Union[str, Path], optional): Chemin complet du fichier de sauvegarde. Defaults
                to None.
    """
    path = _clean_path(filepath, basepath, filename)
    with open(str(path), "wb") as f:
        pickle.dump(data, f)


def load(filepath: CuPath = None, filename: CuPath = None, basepath: CuPath = None):
    """Raccourcis pour charger des données en utilisant pickle.

    Args:
        data (Any): Donnée à sauvegarder. 
        filename (Optional[str], optional): Nom de fichier. Defaults to None.
        basepath (Optional[Union[str, Path]], optional): Dossier de sauvegarde. Si laissé à None
                alors que filename est spécifié, utilisera {pathtools.data()}. Defaults to None.
        filepath (Union[str, Path], optional): Chemin complet du fichier de sauvegarde. Defaults
                to None.
    """
    path = _clean_path(filepath, basepath, filename)
    with open(str(path), "rb") as f:
        return pickle.load(f)


def make_iterator(
        iterator: Union[Sized, Iterable],
        low_bound: int = 100,
        size: Optional[int] = None,
        desc: str = "",
        leave: bool = False,
) -> Any:
    """Raccourcis pour créer un itérateur avec TQDM seulement s'il est assez gros.

    Args:
        iterator (Union[Sized, Iterable]): Chose à itérer
        low_bound (int, optional): Nombre minimal d'élement pour demander l'affichage TQDM. Defaults
                to 100.
        size (Optional[int], optional): Taille de l'itérateur. À spécifier s'il ne s'agit pas d'un 
                Sized (pour un dict.items() par exemple). Defaults to None.
        desc (str, optional): Description TQDM. Defaults to "".
        leave (bool, optional): Chainer les itérateurs. À laisser à false si vous avez plusieurs 
                TQDM imbriqués. Defaults to False.

    Returns:
        Any: L'itérateur
    """
    if size is None:
        size = len(iterator)  # type: ignore

    if size > low_bound:
        return tqdm.tqdm(iterator, desc=desc, total=size, leave=leave)
    else:
        return iterator


def first_file(rootpath: Union[str, Path]) -> Path:
    """Donne le premier fichier d'une arborescence, en descendant en profondeur d'abord.

    Args:
        rootpath (Union[str, Path]): chemin racine de recherche 

    Returns:
        Path: Un chemin vers le fichier
    """
    iterator = os.walk(rootpath)
    e = next(iterator)
    while e[2] == []:
        e = next(iterator)
    return Path(e[0]) / e[2][0]


def nth_file(rootpath: Union[str, Path], n: int) -> Path:
    """Donne le n-ieme fichier d'une arborescence, en descendant en profondeur d'abord.

    Args:
        rootpath (Union[str, Path]): chemin racine de recherche 

    Returns:
        Path: Un chemin vers le fichier
    """
    iterator = os.walk(rootpath)
    e = next(iterator)
    while e[2] == []:
        e = next(iterator)

    return Path(e[0]) / e[2][n]


def random_first_file(rootpath: Union[str, Path]) -> Path:
    """Donne un fichier aléatoire d'une arborescence, en descendant en profondeur d'abord.

    Args:
        rootpath (Union[str, Path]): chemin racine de recherche 

    Returns:
        Path: Un chemin vers le fichier
    """
    iterator = os.walk(rootpath)
    e = next(iterator)
    while e[2] == []:
        e = next(iterator)

    return Path(e[0]) / e[2][random.randint(0, len(e[2]))]


def iterate_files(rootpath: Union[str, Path]) -> Iterable[Path]:
    """Crée un itérateur pour les fichiers d'une arborescence. Parcours en profondeur d'abord.

    Args:
        rootpath (Union[str, Path]): Chemin racine de l'énumération

    Returns:
        Iterable[Path]: L'itérateur

    Yields:
        Iterator[Iterable[Path]]: un chemin de fichier
    """
    iterator = os.walk(rootpath)

    e = next(iterator, None)
    while e is not None:
        while e[2] == []:
            e = next(iterator)

        i = 0
        while i < len(e[2]):
            yield Path(e[0]) / e[2][i]
            i += 1
        e = next(iterator, None)


def insee_department_from_city(insee_city: str) -> str:
    """Donne le code INSEE du département à partir du code INSEE d'une ville.

    Args:
        insee_city (str): code insee d'une ville

    Returns:
        str: code insee du département.
    """
    dpt = insee_city[:2]
    if dpt in {"97", "98"}:
        dpt = insee_city[:3]
    return dpt
