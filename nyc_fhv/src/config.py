import os
from functools import lru_cache


@lru_cache(maxsize=100, typed=False)
def stage_loading():
    STAGE = os.getenv("STAGE", "local")
    if STAGE == "local":
        try:
            from dotenv import load_dotenv, find_dotenv
            print("Loading .env file")
            dot_file = find_dotenv("local.env", raise_error_if_not_found=True)
            load_dotenv(dot_file, override=True, verbose=True)
            print("found", dot_file)
        except ImportError or ModuleNotFoundError:
            print("Loading .env file failed due to python-dotenv is not available.")


stage_loading()


class Config(object):
    # DB Credentials
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_ENGINE = os.getenv("DB_ENGINE", "postgresql+psycopg2")

    LOG_LEVEL = int(os.environ.get("LOG_LEVEL", 20))
    FILE_NAME = os.getenv("FILE_NAME")

    TRANSFORM_ENGINE = os.getenv("TRANSFORM_ENGINE")


@lru_cache
def get_config():
    return Config()


configuration: Config = get_config()
