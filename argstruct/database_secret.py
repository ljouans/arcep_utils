"""
Structure enrobant les secrets de connexion à la base de donnée
"""
from dataclasses import dataclass


@dataclass
class DatabaseSecret:
    """
    Secrets de base
    """
    user: str
    password: str
    host: str
    port: str


@dataclass
class ExtendedDatabaseSecret:
    """
    Secrets de base et nom de la base, pour la génereation d'un connection string.
    """
    user: str
    password: str
    host: str
    port: str
    db: str
