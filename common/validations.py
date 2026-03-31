# =========================
# common/validations.py
# =========================
from pyspark.sql import DataFrame


def validate_not_null(df: DataFrame, column: str) -> DataFrame:
    return df.filter(f"{column} IS NOT NULL")


def drop_duplicates(df: DataFrame, keys: list) -> DataFrame:
    return df.dropDuplicates(keys)
