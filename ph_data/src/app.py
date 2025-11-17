from datetime import date, datetime, timezone

from src.config import configuration
from src.utils.db import get_connection

from src.utils.logger import get_logger

from src.utils.etl import (
    # etl functions that you will create
    extract_pandas,
    extract_duckdb,
    load_pandas,
    load_csv_pandas,
    load_parquet_pandas,
    load_duckdb,
    load_csv_duckdb,
    load_parquet_duckdb,
    transform_pandas,
    transform_duckdb
)

def main():
    logger = get_logger(log_level=configuration.LOG_LEVEL)
    engine = get_connection()
    logger.info("Starting")

    ### ETL Functions here
    if configuration.TRANSFORM_ENGINE == "pandas":

        df = extract_pandas(configuration.FILE_NAME)
        logger.debug(df.shape)
        logger.debug(df.columns)

        df = transform_pandas(df)

        print("Columns after transform:", df.columns.tolist())
        print("Preview of transformed data:")
        print(df.head(5))

        load_csv_pandas(df, "producthierarchy_clean.csv")
        load_parquet_pandas(df, "producthierarchy_clean.parquet")
        load_pandas(df, engine)
    
    elif configuration.TRANSFORM_ENGINE == "duckdb":

        con, table_name = extract_duckdb(f"data/{configuration.FILE_NAME}", table_name="product_hierarchy")

        con, table_name = transform_duckdb(con, table_name)

        df_preview = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        print("Columns after transform:", df_preview.columns.tolist())
        print("Preview of transformed data:")
        print(df_preview)

        load_csv_duckdb(con, table_name, "producthierarchy_clean.csv")
        load_parquet_duckdb(con, table_name, "producthierarchy_clean.parquet")
        load_duckdb(con, table_name, table_name)
    
    print("TRANSFORM_ENGINE is", configuration.TRANSFORM_ENGINE)



    


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = get_logger(log_level=configuration.LOG_LEVEL) # Need to call because of multiprocessing
        logger.error(f"Process Failed. Error: {str(e)}")