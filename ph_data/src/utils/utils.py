import pandas as pd
import duckdb
import re

#-----------------------------------------------
#PANDAS
def clean_text_columns_pandas(df: pd.DataFrame, columns) -> pd.DataFrame:
    """
    Clean specified text columns by removing control characters, collapsing whitespace,
    and trimming leading/trailing spaces.
    """

    for col in columns:
        if col in df.columns:
            # Remove control characters
            df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[\x00-\x1F\x7F]+', '', x))
            # Collapse whitespace
            df[col] = df[col].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
            # set empty strings to None
            df[col] = df[col].replace({'': None})

    return df


def split_product_brand_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split 'product (brand)' into 'product' and 'brand' columns.
    Last parentheses group is assumed to be the brand.
    """

    def split_pb(value: str):
        if pd.isna(value):
            return None, None
        match = re.search(r'\(([^()]*)\)\s*$', value)
        if match:
            brand = match.group(1).title().strip()
            product = re.sub(r'\([^()]*\)\s*$', '', value).title().strip()
        else:
            product = value.title().strip()
            brand = None
        return product, brand

    df[['product', 'brand']] = df['product (brand)'].apply(lambda x: pd.Series(split_pb(x)))

    return df


def split_category_subcategory_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split 'category || sub_category' into separate 'category' and 'subcategory' columns.
    """

    def split_cat(value: str):
        if pd.isna(value):
            return None, None
        
        parts = [p.strip().title() for p in re.split(r'\s*\|\|\s*', value)]

        category = parts[0] if len(parts) > 0 else None

        subcategory = parts[1] if len(parts) > 1 else None

        return category, subcategory

    df[['category', 'subcategory']] = df['category || sub_category'].apply(lambda x: pd.Series(split_cat(x)))
    
    return df


def clean_type_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 'type' column: remove extra whitespace/control chars and Title Case.
    """

    if 'type' in df.columns:
        df['type'] = df['type'].astype(str).apply(lambda x: re.sub(r'[\x00-\x1F\x7F]+', '', x))
        df['type'] = df['type'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
        df['type'] = df['type'].replace({'': None})
        df['type'] = df['type'].apply(lambda x: x.title() if pd.notna(x) else None)

    return df


def parse_dimensions_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse 'length x depth x width (in cm)' into numeric columns: length_cm, depth_cm, width_cm.
    """

    def parse_dim(value: str):
        if pd.isna(value):
            return None, None, None
        
        pattern = r'^\s*([0-9]+(?:\.[0-9]+)?)\s*x\s*([0-9]+(?:\.[0-9]+)?)\s*x\s*([0-9]+(?:\.[0-9]+)?)(?:\s*cm)?\s*$'
        match = re.match(pattern, value)

        if match:
            return float(match.group(1)), float(match.group(2)), float(match.group(3))
        
        return None, None, None

    df[['length_cm', 'depth_cm', 'width_cm']] = df['length x depth x width (in cm)'].apply(lambda x: pd.Series(parse_dim(x)))
    
    return df


def calculate_volume_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the optional derived metric volume_cm3 = length_cm * depth_cm * width_cm.
    If any dimension is missing (NaN), volume_cm3 will be NaN.
    """

    for col in ['length_cm', 'depth_cm', 'width_cm']:
        if col not in df.columns:
            df[col] = None 

    df['volume_cm3'] = df['length_cm'] * df['depth_cm'] * df['width_cm']

    return df



def select_final_columns_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the target columns in order for analytics-ready dataset.
    """

    final_cols = [
        'product_id', 'product', 'brand', 'type', 
        'category', 'subcategory', 'length_cm',
        'depth_cm', 'width_cm', 'volume_cm3'
    ]

    return df[final_cols]


#--------------------------------------------------------------------------------------------
#DUCKDB -------------------------------------------------------------------------------------

def clean_text_columns_duckdb(con, table_name, columns):
    """
    Clean text columns by removing control chars, collapsing whitespace, trimming.
    """

    set_exprs = []

    for col in columns:

        con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS \"{col}\" TEXT")

        expr = f"""
        "{col}" = NULLIF(
            REGEXP_REPLACE(
                REGEXP_REPLACE("{col}", '[\\x00-\\x1F\\x7F]+', ''),
                '\\s+', ' '                                         
            ),
            ''                                                      
        )
        """
        set_exprs.append(expr)

    sql = f"""
    UPDATE {table_name}
    SET {', '.join(set_exprs)}
    """

    con.execute(sql)

    return table_name


def split_product_brand_duckdb(con, table_name):
    """
    Split 'product (brand)' into 'product' and 'brand', with Title Case.
    """

    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS product TEXT")
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS brand TEXT")

    sql = f"""
    UPDATE {table_name}
    SET 
        product = CASE 
            WHEN REGEXP_MATCHES("product (brand)", '\\(.*\\)$') THEN
                REGEXP_REPLACE(
                    LOWER(TRIM(REGEXP_REPLACE("product (brand)", '\\([^()]*\\)\\s*$', ''))),
                    '(^|\\s)(\\S)',
                    '\\1'||UPPER('\\2')
                )
            ELSE
                REGEXP_REPLACE(
                    LOWER(TRIM("product (brand)")),
                    '(^|\\s)(\\S)',
                    '\\1'||UPPER('\\2')
                )
        END,
        brand = CASE 
            WHEN REGEXP_MATCHES("product (brand)", '\\(.*\\)$') THEN
                REGEXP_REPLACE(
                    LOWER(TRIM(REGEXP_EXTRACT("product (brand)", '\\(([^()]*)\\)\\s*$', 1))),
                    '(^|\\s)(\\S)',
                    '\\1'||UPPER('\\2')
                )
            ELSE NULL
        END
    """

    con.execute(sql)

    return table_name


def split_category_subcategory_duckdb(con, table_name):
    """
    Split 'category || sub_category' into 'category' and 'subcategory', with Title Case.
    """

    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS category TEXT")
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS subcategory TEXT")

    sql = f"""
    UPDATE {table_name}
    SET
        category = REGEXP_REPLACE(
            LOWER(TRIM(REGEXP_EXTRACT("category || sub_category", '^(.*?)\\s*\\|\\|', 1))),
            '(^|\\s)(\\S)',
            '\\1'||UPPER('\\2')
        ),
        subcategory = REGEXP_REPLACE(
            LOWER(TRIM(REGEXP_EXTRACT("category || sub_category", '\\|\\|\\s*(.*)$', 1))),
            '(^|\\s)(\\S)',
            '\\1'||UPPER('\\2')
        )
    """

    con.execute(sql)

    return table_name


def clean_type_duckdb(con, table_name):
    """
    Clean the 'type' column: remove control chars, collapse whitespace, Title Case.
    """

    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS type TEXT")

    sql = f"""
    UPDATE {table_name}
    SET type = CASE
        WHEN TRIM(REGEXP_REPLACE(REGEXP_REPLACE("type", '[\\x00-\\x1F\\x7F]+', ''), '\\s+', '')) = '' THEN NULL
        ELSE REGEXP_REPLACE(
            LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE("type", '[\\x00-\\x1F\\x7F]+', ''), '\\s+', ' '))),
            '(\\b[a-z])',
            UPPER('\\1')
        )
    END
    """

    con.execute(sql)

    return table_name


def parse_dimensions_duckdb(con, table_name):
    """
    Parse 'length x depth x width (in cm)' into numeric columns: length_cm, depth_cm, width_cm.
    """

    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS length_cm DOUBLE")
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS depth_cm DOUBLE")
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS width_cm DOUBLE")

    sql = f"""
    UPDATE {table_name}
    SET
        length_cm = CAST(NULLIF(REGEXP_EXTRACT("length x depth x width (in cm)", '^\\s*([0-9]+(?:\\.[0-9]+)?)', 1), '') AS DOUBLE),
        depth_cm  = CAST(NULLIF(REGEXP_EXTRACT("length x depth x width (in cm)", 'x\\s*([0-9]+(?:\\.[0-9]+)?)', 1), '') AS DOUBLE),
        width_cm  = CAST(NULLIF(REGEXP_EXTRACT("length x depth x width (in cm)", 'x\\s*[0-9]+(?:\\.[0-9]+)?\\s*x\\s*([0-9]+(?:\\.[0-9]+)?)', 1), '') AS DOUBLE)
    """

    con.execute(sql)

    return table_name


def calculate_volume_duckdb(con, table_name):
    """
    Calculate the optional derived metric volume_cm3 = length_cm * depth_cm * width_cm.
    Adds a new column if it doesn't exist.
    """

    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS volume_cm3 DOUBLE")

    con.execute(f"""
    UPDATE {table_name}
    SET volume_cm3 = length_cm * depth_cm * width_cm
    """)

    return table_name


def select_final_columns_duckdb(con, table_name):
    """
    Keep only the final analytics-ready columns in the specified order
    by creating a view. The view will have columns exactly as in final_cols.
    """

    final_cols = [
        'product_id', 'product', 'brand', 'type',
        'category', 'subcategory', 'length_cm', 
        'depth_cm', 'width_cm', 'volume_cm3'
    ]

    # Check which of the final_cols actually exist in the table
    result = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    existing_cols = [col[1] for col in result]

    # Only include columns that exist in the table
    cols_to_select = [col for col in final_cols if col in existing_cols]

    # Name for the view
    view_name = table_name + "_final_view"

    # Drop the view if it already exists
    con.execute(f"DROP VIEW IF EXISTS {view_name}")

    # Create the view with columns in the desired order
    cols_str = ', '.join(f'"{col}"' for col in cols_to_select)
    con.execute(f"CREATE VIEW {view_name} AS SELECT {cols_str} FROM {table_name}")

    return view_name


