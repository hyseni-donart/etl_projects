import functools
from sqlalchemy.engine import Engine

from src.config import configuration
import sqlalchemy

@functools.lru_cache(maxsize=100, typed=False)
def get_connection() -> Engine:
    print("Creating db engine.")

    db_engine = configuration.DB_ENGINE
    db_user = configuration.DB_USERNAME
    db_host = configuration.DB_HOST
    db_pass = configuration.DB_PASSWORD
    db_port = configuration.DB_PORT
    db_name = configuration.DB_NAME

    connection_string = (
        f"{db_engine}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )

    engine: Engine = sqlalchemy.create_engine(
        connection_string,
        # encoding="ascii",
        pool_pre_ping=True,
        pool_size=5,  # default
        max_overflow=100,
        # echo=True,  # debug
    )
    # engine.execute("SET SESSION group_concat_max_len = 100000000000;")

    return engine
