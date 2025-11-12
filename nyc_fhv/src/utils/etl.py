import duckdb
import pandas as pd
from .utils import (
    standardize_column_names_pandas,
    convert_expiration_date_pandas,
    trim_text_columns_pandas,
    drop_duplicates_pandas,
    select_required_columns_pandas,
    drop_missing_key_ids_pandas,
    add_days_until_expiration_pandas
)

from .utils import (
    standardize_column_names_duckdb,
    convert_expiration_date_duckdb,
    trim_text_columns_duckdb,
    drop_duplicates_duckdb,
    select_required_columns_duckdb,
    drop_missing_key_ids_duckdb,
    add_days_until_expiration_duckdb
)


#EXTRACT FFUNCTIONS----------------------------------------------------------------------

def extract_pandas(file_name: str) -> pd.DataFrame: # simple extract function
    return pd.read_csv(f"data/{file_name}", low_memory=False)


def extract_duckdb(con, table_name: str, file_name: str):
    """Load CSV into DuckDB table in memory."""
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('data/{file_name}')
        """
    )
    return con, table_name


#LOAD FUNCTIONS-----------------------------------------------------------------------------

def load_pandas(df: pd.DataFrame, engine) -> pd.DataFrame: # simple load function
    df.to_sql("fhv_active_cleaned", engine, if_exists="replace", index=False)


def load_duckdb(con, table_name: str, engine):
    """Load DuckDB table into external DB."""
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    df.to_sql("fhv_active_cleaned", engine, if_exists="replace", index=False)



#PANDAS--------------------------------------------------------------------------------------

def transform_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Transform and clean the FHV dataset using helper functions."""
    
    # 1. Standardizes column names
    df = standardize_column_names_pandas(df)

    # 2.Converts expiration_date to a DATE
    df = convert_expiration_date_pandas(df)

    # 3. Trims whitespace from all columns
    df = trim_text_columns_pandas(df)

    # 4. Drops duplicates
    df = drop_duplicates_pandas(df)

    # 5. Keeps only required colums
    df = select_required_columns_pandas(df)

    # 6. Drops rows with missing key identifiers
    df = drop_missing_key_ids_pandas(df)

    # 6. Adds a days_until_expiration cplumn
    df = add_days_until_expiration_pandas(df)

    return df


#-----------------------------------------------------------------------------------------
#DUCKDB----------------------------------------------------------------------------------

def transform_duckdb(con, table_name: str):
    """
    Transform FHV data entirely in DuckDB using SQL operations.
    Steps mirror the Pandas pipeline but operate in-memory in DuckDB.
    """

    # 1. Standardizes column names
    table_name = standardize_column_names_duckdb(con, table_name)

    # 2. Converts expiration_date column to DATE
    table_name = convert_expiration_date_duckdb(con, table_name)

    # 3. Trims whitespace from all string columns
    table_name = trim_text_columns_duckdb(con, table_name)

    # 4. Drops duplicates
    table_name = drop_duplicates_duckdb(con, table_name)

    # 5. Keeps only required columns
    table_name = select_required_columns_duckdb(con, table_name)

    # 6. Drops rows with missing key identifiers
    table_name = drop_missing_key_ids_duckdb(con, table_name)

    # 7. Adds days_until_expiration column
    table_name = add_days_until_expiration_duckdb(con, table_name)

    return con, table_name

#------------------------------------------------------------------------------------------    

