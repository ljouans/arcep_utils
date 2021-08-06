"""
Outil de requêtes SQL en base, avec fonction de mise en cache des résultats.
"""
import hashlib
import logging
import os
import re
import warnings
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union

import geopandas as pdg
import pandas as pd
import sqlalchemy as sqa
from sqlalchemy import create_engine
from sqlalchemy.engine import Inspector

from . import pathtools as pth
from .argstruct.database_secret import ExtendedDatabaseSecret
from .argstruct.geo_table_info import GeoInfo

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")


# TODO REMOVE
def _create_dir(folder_path: Path):
    folder_path.mkdir(exist_ok=True)


def _rm_string_marker(string: str) -> str:
    for marker in ['"', "'"]:
        if string.startswith(marker) and string.endswith(marker):
            return string.strip(marker)
    return string


def _connection_string_from_secret_file(secret_path_file: Optional[Union[Path, str]] = None):
    parser = ConfigParser()
    secretpath = pth.get_tool_path() / "secret/db.cfg"  # Pour la rétro-compatibilité
    secretpath = Path(secret_path_file) if secret_path_file is not None else secretpath
    if not secretpath.exists():
        raise FileNotFoundError(
                "Could not find the secret folder. Please specify the connexion secrets"
                )
    _ = parser.read(secretpath)
    return _rm_string_marker(parser.get("database", "conn_string"))


def _connection_string_from_db_secret(database_secret: ExtendedDatabaseSecret):
    return f'postgresql://{database_secret.user}:{database_secret.password}@{database_secret.host}:' \
           f'{database_secret.port}/{database_secret.db}'


class Tool:
    """
    Outil de connexion et de requête en base, de chargement de geo/dataframe.
    """

    def __init__(self,
                 secret_path_file: Optional[Union[Path, str]] = None,
                 connection_string: Optional[str] = None,
                 database_secret: Optional[ExtendedDatabaseSecret] = None
                 ):
        self._tmp = pth.tmp_path()
        self._connexion_string = ""
        self._engine = self._create_engine(secret_path_file, connection_string, database_secret)

    @property
    def tmp(self) -> Path:
        """Chemin du dossier des temporaires utilisé pat dbtools.py.
        Relatifs uniquement au fichier pathtools.py

        Returns:
            Path: chemin vers le dossier des tmp
        """
        return self._tmp

    @property
    def engine(self):
        """
        Renvoi le moteur de connexion SQLAlchemy

        Returns:
            Le moteur de connexion
        """
        return self._engine

    @property
    def connexion_string(self) -> str:
        """Retourne la chaine de connexion utilisée pour parler avec la base

        Returns:
            str: chaine de connexion
        """
        return self._connexion_string

    def _create_engine(
            self,
            secret_path_file: Optional[Union[str, Path]] = None,
            connection_string: Optional[str] = None,
            database_secret: Optional[ExtendedDatabaseSecret] = None,
            ):
        if connection_string is not None:
            pass
        elif database_secret is not None:
            connection_string = _connection_string_from_db_secret(database_secret)
        else:
            connection_string = _connection_string_from_secret_file(secret_path_file)

        self._connexion_string = connection_string
        engine = create_engine(connection_string)
        return engine

    def has_table(self, table: str, schema: str) -> bool:
        """Teste si <schema>.<table_name> existe dans la base cible

        Args:
            table (str): nom de table
            schema (str): nom du schema

        Returns:
            bool: True ssi la table existe
        """
        insp = sqa.inspect(self._engine)
        return insp.has_table(table, schema=schema)

    def _get_crs(self, geo_info) -> str:
        query = f"SELECT ST_SRID({geo_info.column}) FROM {geo_info.table_path} where {geo_info.column} is not NULL "
        if geo_info.condition is not None:
            query += f"AND {geo_info.condition} "
        query += "LIMIT 1;"

        df = pd.read_sql(
                query,
                self._engine,
                )
        if not df.empty:
            return str(df["st_srid"].values[0])
        else:
            return '4326'  # Pas de CRS. On se rabat sur un par défaut.

    @staticmethod
    def _get_proper_loader(geo_info: Optional[GeoInfo]):
        if geo_info is not None:
            loader = pdg.read_feather  # type: ignore
        else:
            loader = pd.read_feather  # type: ignore
        return loader

    def fetch_query(
            self,
            query: str,
            geo_info: Optional[GeoInfo] = None,
            force_refetch: bool = False,
            params: Optional[Dict[str, Any]] = None,
            ) -> Union[pd.DataFrame, pdg.GeoDataFrame]:
        """Exécute une requête qui va chercher des données en base et les formatte en Geo/Dataframe.

        Args:
            query (str): Requête à exécuter. DOIT RETOURNER DES VALEURS
            geo_info (Optional[GeoInfo], optional): informations sur la colonne contenant une géométrie,
                            si elle existe. Nécessaire pour la charger correctement dans Geopandas. Peut ajouter une
                            condition sur les lignes (la restriction au code postal, par exemple).
            force_refetch (bool, optional): Ignore le cache. Defaults to False.
            params (Optional[Dict[str, Any]], optional): Paramètres de requêtes supplémentaires.
                                                         Defaults to None.

        Returns:
            Union[pd.DataFrame, pdg.GeoDataFrame]: La géo/dataframe contenant les données requêtées.
        """

        crs = None
        if geo_info is not None:
            if geo_info.condition is None:
                logging.warning(
                        "You specified the informations to retrieve the CRS info, but you did not "
                        "provide any condition over the database. Are you sure the table is meant to be"
                        "unfiltered?"
                        )

            crs = "EPSG:" + self._get_crs(geo_info)
            logging.debug("Found CRS = %s", crs)

        _loader = self._get_proper_loader(geo_info)

        eqry = str(query) + str(geo_info) + str(crs)
        save_path = self.tmp / (str(hashlib.md5(eqry.encode("UTF8")).hexdigest()) + ".fthr")
        loaded_from_server = False

        # Load
        if os.path.exists(save_path) and not force_refetch:
            df = _loader(save_path)  # type: ignore
        else:
            df = pd.read_sql(query, self._engine, params=params)
            loaded_from_server = True

        # Parse
        if loaded_from_server and geo_info is not None:
            gk = pdg.GeoSeries.from_wkb(df[geo_info.column])  # type: ignore
            df = pdg.GeoDataFrame(df, geometry=gk, crs=crs)
            if geo_info.column != 'geometry':
                df = df.drop([geo_info.column], axis=1)  # type: ignore

        # Save
        if df.empty:
            logging.warning("The dataframe from the following query was empty\n%s", query)
        else:
            df.to_feather(str(save_path))

        return df

    def drop_table(self, regex: str, schema: str):
        """
        Supprime la ou les tables ciblées par l'expression régulière.

        Args:
            regex: expression régulière désignant les tables
            schema: Schéma de travail

        Returns:
            La liste des tables supprimées.

        """
        engine = self.engine

        inspector: Inspector = sqa.inspect(engine)

        all_tables: List[str] = inspector.get_table_names(schema=schema)
        pattern = re.compile(regex)
        to_drop = []
        for table in all_tables:
            if pattern.search(table):
                logging.debug(f'Dropping table {table}')
                to_drop.append(f"{schema}.{table}")

        if to_drop:  # != []:
            engine.execute(f'DROP TABLE {", ".join(to_drop)};')
        return to_drop
