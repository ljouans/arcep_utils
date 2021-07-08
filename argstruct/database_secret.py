from dataclasses import dataclass


@dataclass
class DatabaseSecret:
    user: str
    password: str
    host: str
    port: str

@dataclass
class ExtendedDatabaseSecret:
    user: str
    password: str
    host: str
    port: str
    db: str
