import duckdb
import pandas as pd



def extract(file_name: str) -> pd.DataFrame: # simple extract function
    return pd.read_csv(f"data/{file_name}", low_memory=False)


def load(df: pd.DataFrame, engine) -> pd.DataFrame: # simple load function
    df.to_sql("fhv_active_cleaned", engine, if_exists="replace", index=False)


#PANDAS--------------------------------------------------------------------------------------

def transform_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean the fhv dataset.

    Args:
        df (pd.DataFrame): Raw fhv data read from csv.

    Returns:
        pd.DataFrame: Cleaned and transformed DataFrame with:
            - standardized column names (lowercase, underscores)
            - expiration_date as datetime
            - trimmed text columns
            - duplicates removed
            - only required columns kept
            - rows with missing key identifiers removed
            - days_until_expiration column added
    """

    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    df['expiration_date'] = pd.to_datetime(
        df['expiration_date'],
        format="%m/%d/%Y",
        errors="coerce"  
    )

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    df = df.drop_duplicates(subset=["vehicle_license_number", "dmv_license_plate_number"])

    columns_to_keep = [
    "vehicle_license_number",
    "license_type",
    "dmv_license_plate_number",
    "vehicle_vin_number",
    "expiration_date",
    "wheelchair_accessible",
    "active"
    ]

    df = df[columns_to_keep]

    df = df.dropna(subset=["vehicle_license_number", "dmv_license_plate_number"])

    df['days_until_expiration'] = (df['expiration_date'] - pd.Timestamp.now()).dt.days

    duckdb.register("fhv_data", df)

    return df

#-----------------------------------------------------------------------------------------
#DUCKDB----------------------------------------------------------------------------------


#------------------------------------------------------------------------------------------    


