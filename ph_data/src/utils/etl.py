import pandas as pd
import duckdb
import os
from src.config import configuration
from .utils import (
    clean_text_columns_pandas,
    split_product_brand_pandas,
    split_category_subcategory_pandas,
    clean_type_pandas,
    parse_dimensions_pandas,
    calculate_volume_pandas,
    select_final_columns_pandas
)
from .utils import (
    clean_text_columns_duckdb,
    split_product_brand_duckdb,
    split_category_subcategory_duckdb,
    clean_type_duckdb,
    parse_dimensions_duckdb,
    calculate_volume_duckdb,
    select_final_columns_duckdb
)


#EXTRACT FUNTIONS---------------------------------------------------------------------------

def extract_pandas(file_name: str) -> pd.DataFrame: # simple extract function
    return pd.read_csv(f"data/{file_name}", low_memory=False)


def extract_duckdb(file_path: str, table_name: str = "fhv_data"):
    """
    Extract CSV into DuckDB in-memory table.
    Returns: DuckDB connection and table name.
    """
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
    return con, table_name

#---------------------------------------------------------------------------------------------
#LOAD FUNCTIONS------------------------------------------------------------------------------

#LOAD PANDAS----------------------------------------------------------------------------------
def load_pandas(df: pd.DataFrame, engine) -> pd.DataFrame: # simple load function
    df.to_sql("product_hirearchy_active_cleaned", engine, if_exists="replace", index=False)


def load_csv_pandas(df: pd.DataFrame, file_name: str):
    """
    Save DataFrame as CSV in 'data/curated', creating folder if needed.
    """
    os.makedirs("data/curated", exist_ok=True)
    df.to_csv(f"data/curated/{file_name}", index=False)



def load_parquet_pandas(df: pd.DataFrame, file_name: str):
    """
    Save DataFrame as Parquet in 'data/warehouse', creating folder if needed.
    """
    os.makedirs("data/warehouse", exist_ok=True)
    df.to_parquet(f"data/warehouse/{file_name}", index=False)



#LOAD DUCKDB-------------------------------------------------------------------------------


def load_duckdb(con, table_name: str, engine):
    """
    Load DuckDB table directly into PostgreSQL.
    """
    db_url = (
        f"postgres://{configuration.DB_USERNAME}:"
        f"{configuration.DB_PASSWORD}@"
        f"{configuration.DB_HOST}:"
        f"{configuration.DB_PORT}/"
        f"{configuration.DB_NAME}"
    )

    # Enable DuckDB's Postgres extension
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")

    # Attach the Postgres database
    con.execute(f"ATTACH '{db_url}' AS postgres_db (TYPE POSTGRES);")

    # Insert data from DuckDB table into Postgres table
    new_table_name = "product_hierarchy_active_cleaned_duckdb"
    con.execute(f"""
        CREATE OR REPLACE TABLE postgres_db.public.{new_table_name} AS (
            SELECT * FROM {table_name}
        );
    """)


def load_csv_duckdb(con, table_name: str, file_name: str):
    """
    Export DuckDB table to CSV in 'data/curated', creating folder if needed.
    """
    os.makedirs("data/curated", exist_ok=True)
    con.execute(f"COPY {table_name} TO 'data/curated/{file_name}' (HEADER, DELIMITER ',');")



def load_parquet_duckdb(con, table_name: str, file_name: str):
    """
    Export DuckDB table to Parquet in 'data/warehouse', creating folder if needed.
    """
    os.makedirs("data/warehouse", exist_ok=True)
    con.execute(f"COPY {table_name} TO 'data/warehouse/{file_name}' (FORMAT 'parquet');")


#TRANSFORM FUNCTIONS---------------------------------------------------------------------------


#PANDAS --------------------------------------------------------------------------------------

def transform_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform the product hierarchy DataFrame."""

    # Clean text columns
    df = clean_text_columns_pandas(df, ['product (brand)', 'type', 'category || sub_category'])

    # Split product and brand
    df = split_product_brand_pandas(df)

    # Split category and subcatgory
    df = split_category_subcategory_pandas(df)

    # Clean 'type' column
    df = clean_type_pandas(df)

    # Parse dimensions
    df = parse_dimensions_pandas(df)

    # Calulate volume
    df = calculate_volume_pandas(df)

    # Select final columns
    df = select_final_columns_pandas(df)


    return df



#DUCKDB --------------------------------------------------------------------------------------

def transform_duckdb(con, table_name: str):
    """
    Run the full Product Hierarchy ETL in DuckDB using step functions.
    Each step overwrites the same table.
    Returns the connection and final table name.
    """

    # Clean and normlize text columns
    table_name = clean_text_columns_duckdb(
        con, table_name,
        columns=["product (brand)", "type", "category || sub_category"]
    )

    # Split product & brand
    table_name = split_product_brand_duckdb(con, table_name)

    # Split category & subcategory
    table_name = split_category_subcategory_duckdb(con, table_name)

    # Clean type column
    table_name = clean_type_duckdb(con, table_name)

    # Parse dimensions
    table_name = parse_dimensions_duckdb(con, table_name)

    # calculate volume
    table_name = calculate_volume_duckdb(con, table_name)

    # Select final colmns in order
    table_name = select_final_columns_duckdb(con, table_name)

    # Return the connection and final table
    return con, table_name
