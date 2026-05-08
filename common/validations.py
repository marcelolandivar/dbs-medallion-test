# =========================
# common/validations.py
# =========================
from pyspark.sql import DataFrame


def validate_not_null(df: DataFrame, column: str) -> DataFrame:
    return df.filter(f"{column} IS NOT NULL")


def drop_duplicates(df: DataFrame, keys: list) -> DataFrame:
    return df.dropDuplicates(keys)


def safe_transform(df, transform_func, transform_name):
    """Apply transformation with error handling"""
    try:
        print(f"Applying {transform_name}...")
        result = transform_func(df)
        print(f"✓ {transform_name} completed successfully")
        return result
    except Exception as e:
        print(f"✗ ERROR in {transform_name}: {str(e)}")
        raise