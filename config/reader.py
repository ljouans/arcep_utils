import configparser
from pathlib import Path
from typing import List, NoReturn, Union
from utils.config.InvalidValueException import InvalidValueError

import utils.pathtools as pth
from utils.argstruct.database_secret import DatabaseSecret


class ConfigManager(configparser.RawConfigParser):
    """Lecteur de configuration. Cherche le fichier base.cfg et run.cfg.
    Le second prends la précedence sur le premier.
    """

    def __init__(self):
        super().__init__()
        self.read(
            [
                pth.outer_out_path() / "base.cfg",
                pth.outer_out_path() / "run.cfg",
            ]
        )

    def getlist(self, section: str, key: str) -> List[str]:
        """Lis le paramètre de configuration comme une liste python.
        Utilise un parser json. Se heurtera aux problèmes inhérents du JSON 
        (mauvais caractères par exepmle).

        Args:
            section (str): Section de configuration
            key (str): clef de configuration

        Returns:
            List[str]: Liste des paramètres lus.
        """
        elems = self.get(section, key)
        return [e.strip() for e in elems.split(',')]

    @staticmethod
    def invalid_value(section: str, key: str, value: str) -> NoReturn:
        """Raccourcis pour lever une erreur de configuration.

        Args:
            section (str): nom de la section où se trouve l'erreur
            key (str): clef où se trouve l'erreur
            value (str): valeur donnée

        Raises:
            InvalidValueError: Wrapper d'erreur spécifique

        Returns:
            NoReturn: Ne renvoi jamais de valeur. Lève toujours une exception
        """
        # TODO: Add expected result config
        raise InvalidValueError(
            "Wrong parameter in config file", section, key, value
        )

    @staticmethod
    def load_db_secret(secret_file_path: Union[str, Path]) -> DatabaseSecret:
        cfg = configparser.RawConfigParser()
        cfg.read(secret_file_path)
        user = cfg.get("database", "user")
        pwd = cfg.get("database", "password")
        host = cfg.get("database", "host")
        port = cfg.get("database", "port")

        return DatabaseSecret(user=user, password=pwd, host=host, port=port)

    @staticmethod
    def create_connection_string(
        secret_file_path: Union[str, Path], millesime: str
    ) -> str:
        """Raccourcis pour créer la chaine de connexion sqlalchemy à partir des
        paramètres.

        Args:
            secretFilePath (Union[str, Path]): Chemin vers le fichier de secret
            millesime (str): Millésime de base auquel se connecter

        Returns:
            str: La chaine de connexion
        """
        cfg = configparser.RawConfigParser()
        cfg.read(secret_file_path)
        user = cfg.get("database", "user")
        pwd = cfg.get("database", "password")
        host = cfg.get("database", "host")
        port = cfg.get("database", "port")

        tablename = f"base_infra_a{millesime}"
        conn_string = f"postgresql://{user}:{pwd}@{host}:{port}/{tablename}"

        return conn_string