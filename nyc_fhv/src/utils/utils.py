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
    """Lowercase columns and replace spaces with underscores."""

    columns = con.execute(f"DESCRIBE {table_name}").fetchdf()['column_name'].tolist()
    rename_map = {col: col.lower().replace(" ", "_") for col in columns}
    
    for old, new in rename_map.items():
        if old != new:
            con.execute(f'ALTER TABLE {table_name} RENAME COLUMN "{old}" TO "{new}"')
    
    return table_name


def convert_expiration_date_duckdb(con, table_name: str):
    """Convert expiration_date column to DATE."""

    con.execute(f"""
        UPDATE {table_name}
        SET expiration_date = STRPTIME(CAST(expiration_date AS VARCHAR), '%Y-%m-%d')
    """)

    return table_name


def trim_text_columns_duckdb(con, table_name: str):
    """Trim whitespace from all VARCHAR columns."""

    string_cols = con.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
          AND data_type = 'VARCHAR'
    """).fetchall()

    for (col,) in string_cols:
        con.execute(f"UPDATE {table_name} SET {col} = TRIM({col});")

    return table_name


def drop_duplicates_duckdb(con, table_name: str):
    """Remove duplicate rows."""

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT DISTINCT *
        FROM {table_name}
    """)

    return table_name


def select_required_columns_duckdb(con, table_name: str):
    """Keep only the required columns."""

    cols = [
        "vehicle_license_number",
        "license_type",
        "dmv_license_plate_number",
        "vehicle_vin_number",
        "expiration_date",
        "wheelchair_accessible",
        "active"
    ]

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT {', '.join(cols)}
        FROM {table_name}
    """)

    return table_name


def drop_missing_key_ids_duckdb(con, table_name: str):
    """Drop rows with missing key IDs."""

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT *
        FROM {table_name}
        WHERE vehicle_license_number IS NOT NULL
          AND dmv_license_plate_number IS NOT NULL
    """)

    return table_name


def add_days_until_expiration_duckdb(con, table_name: str):
    """Add days_until_expiration column."""
    
    con.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN days_until_expiration INTEGER;
    """)

    con.execute(f"""
        UPDATE {table_name}
        SET days_until_expiration = DATE_DIFF('day', CURRENT_DATE, expiration_date)
    """)

    return table_name
