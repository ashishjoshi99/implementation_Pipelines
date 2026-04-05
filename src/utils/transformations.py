from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import re

# ==========================================
# Schema & Column Level Transformations
# ==========================================

def clean_column_names(df: DataFrame, **kwargs) -> DataFrame:
    """
    Replaces spaces with underscores and makes all column names lowercase.
    This operates completely on the distributed Spark engines driver without UDFs.
    """
    clean_cols = [
        F.col(c).alias(re.sub('[^0-9a-zA-Z]+', '_', c).lower()) 
        for c in df.columns
    ]
    return df.select(clean_cols)

def drop_columns(df: DataFrame, columns: list, **kwargs) -> DataFrame:
    """Drops a list of specified columns if they exist in the dataframe."""
    existing_cols = [c for c in columns if c in df.columns]
    return df.drop(*existing_cols) if existing_cols else df

def cast_datatypes(df: DataFrame, mapping: dict, **kwargs) -> DataFrame:
    """
    Casts columns to specified datatypes. 
    Expects mapping like: {"age": "int", "created_at": "timestamp"}
    """
    for col_name, data_type in mapping.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(data_type))
    return df

# ==========================================
# Data Cleansing & Formatting Transformations
# ==========================================

def fill_nulls(df: DataFrame, columns: list, value, **kwargs) -> DataFrame:
    """Fills null values in specific columns with a static fallback value."""
    existing_cols = [c for c in columns if c in df.columns]
    return df.fillna(value, subset=existing_cols) if existing_cols else df

def trim_strings(df: DataFrame, columns: list = None, **kwargs) -> DataFrame:
    """
    Trims whitespaces continuously. 
    If columns is not provided, dynamically applies to all StringType columns.
    """
    target_cols = columns if columns else [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in target_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df

def clean_email(df: DataFrame, column: str, **kwargs) -> DataFrame:
    """Trims whitespace and lowercases email addresses in a specific column."""
    if column not in df.columns:
        return df # Safely ignore if column doesn't exist
    return df.withColumn(column, F.lower(F.trim(F.col(column))))

def clean_phone(df: DataFrame, column: str, **kwargs) -> DataFrame:
    """Removes non-numeric characters from a phone number using PySpark regex engine."""
    if column not in df.columns:
        return df
    return df.withColumn(column, F.regexp_replace(F.col(column), r'[^0-9]', ''))

def generic_regex_replace(df: DataFrame, column: str, pattern: str, replacement: str, **kwargs) -> DataFrame:
    """A generic regex replace wrapper allowing users to pass their own regex logic via YAML."""
    if column not in df.columns:
         return df
    return df.withColumn(column, F.regexp_replace(F.col(column), pattern, replacement))

# ==========================================
# Feature Engineering & PII Transformations
# ==========================================

def add_ingestion_timestamp(df: DataFrame, column_name: str = "ingested_at", **kwargs) -> DataFrame:
    """Adds a standard ingestion/processing timestamp."""
    return df.withColumn(column_name, F.current_timestamp())

def extract_date_parts(df: DataFrame, column: str, **kwargs) -> DataFrame:
    """
    Extracts year, month, and day from a date/timestamp column into separate columns. 
    Highly useful for setting up Delta Lake Partitioning natively!
    """
    if column not in df.columns:
         return df
    return df \
        .withColumn(f"{column}_year", F.year(F.col(column))) \
        .withColumn(f"{column}_month", F.month(F.col(column))) \
        .withColumn(f"{column}_day", F.dayofmonth(F.col(column)))

def hash_columns(df: DataFrame, columns: list, target_column: str = "surrogate_hash_key", **kwargs) -> DataFrame:
    """
    Hashes a combination of columns using SHA-256 natively.
    Perfect for generating Surrogate Keys or anonymizing PII identifiers.
    """
    existing_cols = [F.col(c) for c in columns if c in df.columns]
    if not existing_cols:
         return df
    return df.withColumn(target_column, F.sha2(F.concat_ws("||", *existing_cols), 256))

def mask_data(df: DataFrame, column: str, num_chars_to_keep: int = 4, mask_char: str = "*", **kwargs) -> DataFrame:
    """
    Masks string data (e.g. Credit Cards, SSN) replacing all but the last N characters.
    """
    if column not in df.columns:
        return df
    # Safely left pads a string with mask_char, retaining only rightmost num_chars_to_keep
    return df.withColumn(
        column, 
        F.expr(f"lpad(right({column}, {num_chars_to_keep}), length({column}), '{mask_char}')")
    )

# ==========================================
# SQL Integration
# ==========================================

def execute_sql(df: DataFrame, query: str, **kwargs) -> DataFrame:
    """
    Executes an arbitrary Spark SQL query.
    The data moving through the pipeline is available as a temporary view named 'current_df'.
    Because it's Databricks, users can directly JOIN other Unity Catalog tables natively in the query.
    """
    df.createOrReplaceTempView("current_df")
    return df.sparkSession.sql(query)


# ==========================================
# Unified Transformation Registry Map
# ==========================================
# Every new function written above MUST be mapped here to be exposed to the YAML parser!

TRANSFORMATION_REGISTRY = {
    # Schema
    "clean_column_names": clean_column_names,
    "drop_columns": drop_columns,
    "cast_datatypes": cast_datatypes,
    
    # Cleansing
    "fill_nulls": fill_nulls,
    "trim_strings": trim_strings,
    "clean_email": clean_email,
    "clean_phone": clean_phone,
    "regex_replace": generic_regex_replace,
    
    # Feature Engineering / PII
    "add_ingestion_timestamp": add_ingestion_timestamp,
    "extract_date_parts": extract_date_parts,
    "hash_columns": hash_columns,
    "mask_data": mask_data,
    
    # Advanced
    "execute_sql": execute_sql,
}
