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
    create_duckdb_table,
    standardize_column_names_duckdb,
    convert_expiration_date_duckdb,
    trim_text_columns_duckdb,
    drop_duplicates_duckdb,
    select_required_columns_duckdb,
    drop_missing_key_ids_duckdb,
    add_days_until_expiration_duckdb
)



def extract(file_name: str) -> pd.DataFrame: # simple extract function
    return pd.read_csv(f"data/{file_name}", low_memory=False)


def load(df: pd.DataFrame, engine) -> pd.DataFrame: # simple load function
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

def transform_duckdb(df):
    """
    Transform FHV data entirely in DuckDB using SQL operations.
    Steps mirror the Pandas pipeline but operate in-memory in DuckDB.
    """
    # 1. Registers the original DataFrame as a DuckDB table
    table = create_duckdb_table(df, "fhv_data")

    # 2. Standardizes column names
    table = standardize_column_names_duckdb(table)

    # 3. Converts expiration_date column to DATE
    table = convert_expiration_date_duckdb(table)

    # 4. Trims whitespace from all string columns
    table = trim_text_columns_duckdb(table)

    # 5. Drops duplicates
    table = drop_duplicates_duckdb(table)

    # 6. Keeps only required columns
    table = select_required_columns_duckdb(table)

    # 7. Drops rows with missing key identifiers
    table = drop_missing_key_ids_duckdb(table)

    # 8. Adds days_until_expiration column
    table = add_days_until_expiration_duckdb(table)

    # Returns a Pandas DataFrame if needed for further processing or loading
    return duckdb.sql(f"SELECT * FROM {table}").fetchdf()

#------------------------------------------------------------------------------------------    


