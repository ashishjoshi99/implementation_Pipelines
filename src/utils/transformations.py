from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import re

def clean_column_names(df: DataFrame, **kwargs) -> DataFrame:
    clean_cols = [
        F.col(c).alias(re.sub('[^0-9a-zA-Z]+', '_', c).lower()) 
        for c in df.columns
    ]
    return df.select(clean_cols)

def drop_columns(df: DataFrame, columns: list, **kwargs) -> DataFrame:
    existing_cols = [c for c in columns if c in df.columns]
    return df.drop(*existing_cols) if existing_cols else df

def cast_datatypes(df: DataFrame, mapping: dict, **kwargs) -> DataFrame:
    for col_name, data_type in mapping.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(data_type))
    return df

def fill_nulls(df: DataFrame, columns: list, value, **kwargs) -> DataFrame:
    existing_cols = [c for c in columns if c in df.columns]
    return df.fillna(value, subset=existing_cols) if existing_cols else df

def trim_strings(df: DataFrame, columns: list = None, **kwargs) -> DataFrame:
    target_cols = columns if columns else [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in target_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df

def clean_email(df: DataFrame, column: str, **kwargs) -> DataFrame:
    if column not in df.columns:
        return df
    return df.withColumn(column, F.lower(F.trim(F.col(column))))

def clean_phone(df: DataFrame, column: str, **kwargs) -> DataFrame:
    if column not in df.columns:
        return df
    return df.withColumn(column, F.regexp_replace(F.col(column), r'[^0-9]', ''))

def generic_regex_replace(df: DataFrame, column: str, pattern: str, replacement: str, **kwargs) -> DataFrame:
    if column not in df.columns:
         return df
    return df.withColumn(column, F.regexp_replace(F.col(column), pattern, replacement))

def add_ingestion_timestamp(df: DataFrame, column_name: str = "ingested_at", **kwargs) -> DataFrame:
    return df.withColumn(column_name, F.current_timestamp())

def extract_date_parts(df: DataFrame, column: str, **kwargs) -> DataFrame:
    if column not in df.columns:
         return df
    return df \
        .withColumn(f"{column}_year", F.year(F.col(column))) \
        .withColumn(f"{column}_month", F.month(F.col(column))) \
        .withColumn(f"{column}_day", F.dayofmonth(F.col(column)))

def hash_columns(df: DataFrame, columns: list, target_column: str = "surrogate_hash_key", **kwargs) -> DataFrame:
    existing_cols = [F.col(c) for c in columns if c in df.columns]
    if not existing_cols:
         return df
    return df.withColumn(target_column, F.sha2(F.concat_ws("||", *existing_cols), 256))

def mask_data(df: DataFrame, column: str, num_chars_to_keep: int = 4, mask_char: str = "*", **kwargs) -> DataFrame:
    if column not in df.columns:
        return df
    return df.withColumn(
        column, 
        F.expr(f"lpad(right({column}, {num_chars_to_keep}), length({column}), '{mask_char}')")
    )

def execute_sql(df: DataFrame, query: str, **kwargs) -> DataFrame:
    df.createOrReplaceTempView("current_df")
    return df.sparkSession.sql(query)

TRANSFORMATION_REGISTRY = {
    "clean_column_names": clean_column_names,
    "drop_columns": drop_columns,
    "cast_datatypes": cast_datatypes,
    "fill_nulls": fill_nulls,
    "trim_strings": trim_strings,
    "clean_email": clean_email,
    "clean_phone": clean_phone,
    "regex_replace": generic_regex_replace,
    "add_ingestion_timestamp": add_ingestion_timestamp,
    "extract_date_parts": extract_date_parts,
    "hash_columns": hash_columns,
    "mask_data": mask_data,
    "execute_sql": execute_sql,
}
