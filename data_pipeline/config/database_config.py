import os
from dataclasses import dataclass
from typing import Dict

from dotenv import load_dotenv

@dataclass
class DatabaseConfig:
    def __init__(self):
        self.db_name = None
        self.uri = None

    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"----------Missing configuration value: {key}----------")
        
@dataclass
class MongoDBConfig(DatabaseConfig):
    uri: str
    db_name: str

@dataclass
class MySQLConfig(DatabaseConfig):
    host: str
    port: int
    user: str
    password: str
    # db_name: str

@dataclass
class RedisConfig(DatabaseConfig):
    host: str
    port: int
    user: str
    password: str
    db_name: str


def get_database_config() -> Dict[str, DatabaseConfig]:
    load_dotenv()
    config = {
        "mongodb": MongoDBConfig(
            uri = os.getenv("MONGO_URI"),
            db_name = os.getenv("MONGO_DB_NAME")
        ),
        "mysql": MySQLConfig(
            host = os.getenv("MYSQL_HOST"),
            port = int(os.getenv("MYSQL_PORT")),
            user =  os.getenv("MYSQL_USER"),
            password = os.getenv("MYSQL_PASSWORD"),
            #db_name = os.getenv("MYSQL_DATABASE")
        ),
        "redis": RedisConfig(
            host = os.getenv("REDIS_HOST"),
            port = int(os.getenv("REDIS_PORT")),
            user = os.getenv("REDIS_USER"),
            password = os.getenv("REDIS_PASSWORD"),
            db_name = os.getenv("REDIS_DATABASE")
        )
    }

    for db, setting in config.items():
        setting.validate()
    return config
