from datetime import date, datetime, timezone

from src.config import configuration
from src.utils.db import get_connection

from src.utils.logger import get_logger

from src.utils.etl import (
    # etl functions that you will create
    extract,
    load,
    transform_pandas,
    transform_duckdb
)

def main():
    logger = get_logger(log_level=configuration.LOG_LEVEL)
    engine = get_connection()
    logger.info("Starting")

    ### ETL Functions here
    df = extract(configuration.FILE_NAME)
    logger.debug(df.shape)
    logger.debug(df.columns)

    
    df = transform_pandas(df)
    # df = transform_duckdb(df)

    print("Columns after transform:", df.columns.tolist())
    print("Preview of transformed data:")
    print(df.head(5))

    load(df, engine)

    


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = get_logger(log_level=configuration.LOG_LEVEL) # Need to call because of multiprocessing
        logger.error(f"Process Failed. Error: {str(e)}")