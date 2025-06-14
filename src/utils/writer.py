from typing import Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: str | list | None = None,
    add_timestamp: bool = True,
    timestamp_col: str = "ingestion_timestamp",
) -> None:
    """
    Write DataFrame to parquet format with flexible parameters.

    Args:
        df: DataFrame to write
        path: Full path where to write the parquet files (e.g. 'data/bronze/customers')
        mode: Write mode ('overwrite', 'append', 'error', 'ignore')
        partition_by: Column(s) to partition by
        add_timestamp: Whether to add ingestion timestamp
        timestamp_col: Name of the timestamp column if add_timestamp is True

    """

    if add_timestamp:
        df = df.withColumn(timestamp_col, current_timestamp())

    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.parquet(path)


def write_bronze(
    df: DataFrame, table_name: str, base_path: str = "data/bronze", **kwargs
) -> None:

    output_path = f"{base_path}/{table_name}"
    write_parquet(df, output_path, **kwargs)


def write_silver(
    df: DataFrame, table_name: str, base_path: str = "data/silver", **kwargs
) -> None:

    output_path = f"{base_path}/{table_name}"
    write_parquet(df, output_path, **kwargs)


def write_gold(
    df: DataFrame, table_name: str, base_path: str = "data/gold", **kwargs
) -> None:

    output_path = f"{base_path}/{table_name}"
    write_parquet(df, output_path, **kwargs)
