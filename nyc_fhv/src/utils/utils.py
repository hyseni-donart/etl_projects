import pandas as pd
import duckdb

def standardize_column_names_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names -> all lowercase, spaces replaced with underscores."""

    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    return df


def convert_expiration_date_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Convert `expiration_date` column to a proper datetime type."""

    df['expiration_date'] = pd.to_datetime(
        df['expiration_date'],
        format="%m/%d/%Y",
        errors="coerce"
    )

    return df


def trim_text_columns_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace from all columns."""

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    return df


def drop_duplicates_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicates based on `vehicle_license_number` and `dmv_license_plate_number`."""

    return df.drop_duplicates(subset=["vehicle_license_number", "dmv_license_plate_number"])


def select_required_columns_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only required columns."""

    columns_to_keep = [
        "vehicle_license_number",
        "license_type",
        "dmv_license_plate_number",
        "vehicle_vin_number",
        "expiration_date",
        "wheelchair_accessible",
        "active"
    ]

    return df[columns_to_keep]


def drop_missing_key_ids_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with missing `vehicle_license_number` or `dmv_license_plate_number`."""

    return df.dropna(subset=["vehicle_license_number", "dmv_license_plate_number"])


def add_days_until_expiration_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Add a `days_until_expiration` column."""

    df['days_until_expiration'] = (df['expiration_date'] - pd.Timestamp.now()).dt.days

    return df


#-------------------------------------------------------------------------------------------
#---------- DUCKDB --------------------------------------------------------------------------

def create_duckdb_table(con, df, table_name: str):
    """Register a Pandas DataFrame as a DuckDB table in-memory."""
    con.register(table_name, df)
    return table_name


def standardize_column_names_duckdb(con, table_name: str):
    columns = con.execute(f"DESCRIBE {table_name}").fetchdf()['column_name'].tolist()
    rename_columns = [f'"{col}" AS "{col.lower().replace(" ", "_")}"' for col in columns]
    new_table_name = f"{table_name}_std"
    con.execute(f"CREATE OR REPLACE TABLE {new_table_name} AS SELECT {', '.join(rename_columns)} FROM {table_name}")
    return new_table_name


def convert_expiration_date_duckdb(con, table_name: str):
    """Convert `expiration_date` column to a proper datetime type."""
    
    new_table = f"{table_name}_date"
    
    con.execute(f"""
        CREATE OR REPLACE TABLE {new_table} AS
        SELECT
            vehicle_license_number,
            license_type,
            dmv_license_plate_number,
            vehicle_vin_number,
            STRPTIME(CAST(expiration_date AS VARCHAR), '%Y-%m-%d') AS expiration_date,
            wheelchair_accessible,
            active
        FROM {table_name}
    """)
    
    return new_table


def trim_text_columns_duckdb(con, table_name: str):
    columns_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
    string_cols = columns_info[columns_info['column_type'] == 'VARCHAR']['column_name'].tolist()
    select_columns = [f"TRIM({col}) AS {col}" if col in string_cols else col for col in columns_info['column_name']]
    new_table = f"{table_name}_trimmed"
    con.execute(f"CREATE OR REPLACE TABLE {new_table} AS SELECT {', '.join(select_columns)} FROM {table_name}")
    return new_table


def drop_duplicates_duckdb(con, table_name: str):
    new_table = f"{table_name}_dedup"
    con.execute(f"""
        CREATE OR REPLACE TABLE {new_table} AS
        SELECT DISTINCT ON (vehicle_license_number, dmv_license_plate_number) *
        FROM {table_name}
    """)
    return new_table


def select_required_columns_duckdb(con, table_name: str):
    columns_to_keep = [
        "vehicle_license_number",
        "license_type",
        "dmv_license_plate_number",
        "vehicle_vin_number",
        "expiration_date",
        "wheelchair_accessible",
        "active"
    ]
    new_table = f"{table_name}_cols"
    con.execute(f"""
        CREATE OR REPLACE TABLE {new_table} AS
        SELECT {', '.join(columns_to_keep)}
        FROM {table_name}
    """)
    return new_table


def drop_missing_key_ids_duckdb(con, table_name: str):
    new_table = f"{table_name}_notnull"
    con.execute(f"""
        CREATE OR REPLACE TABLE {new_table} AS
        SELECT *
        FROM {table_name}
        WHERE vehicle_license_number IS NOT NULL AND dmv_license_plate_number IS NOT NULL
    """)
    return new_table


def add_days_until_expiration_duckdb(con, table_name: str):
    new_table = f"{table_name}_days"
    con.execute(f"""
        CREATE OR REPLACE TABLE {new_table} AS
        SELECT *,
               EXTRACT(DAY FROM expiration_date - CURRENT_DATE) AS days_until_expiration
        FROM {table_name}
    """)
    return new_table
