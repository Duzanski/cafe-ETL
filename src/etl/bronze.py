from pyspark.sql import SparkSession
from src.schemas.data_schemas import customers_schema, events_schema, offers_schema
from src.utils.spark_utils import get_spark_session
from src.utils.writer import write_bronze


def ingest_bronze(spark: SparkSession) -> None:
    """Ingest raw data into bronze layer with schema validation."""
    # Read raw data with defined schemas
    customers_df = (
        spark.read.option("header", "true")
        .schema(customers_schema)
        .csv("data/landing_zone/customers.csv")
    )

    write_bronze(customers_df, "customers")

    events_df = (
        spark.read.option("header", "true")
        .schema(events_schema)
        .csv("data/landing_zone/events.csv")
    )
    write_bronze(events_df, "events")

    offers_df = (
        spark.read.option("header", "true")
        .schema(offers_schema)
        .csv("data/landing_zone/offers.csv")
    )
    write_bronze(offers_df, "offers")
