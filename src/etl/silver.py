from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from src.utils.spark_utils import get_spark_session
from src.utils.writer import write_silver
from src.schemas.data_schemas import schema_value


def process_silver(spark: SparkSession) -> None:
    """Process bronze data into silver layer with cleaning and standardization."""
    bronze_path = "data/bronze"

    customers_df = spark.read.parquet(f"{bronze_path}/customers")
    events_df = spark.read.parquet(f"{bronze_path}/events")
    offers_df = spark.read.parquet(f"{bronze_path}/offers")

    customers_filtered = customers_df.filter(
        (F.col("gender").isNotNull())
        & (F.col("income").isNotNull())
        & (F.col("age") != 118)
    ).dropDuplicates(["customer_id"])

    customers_clean = (
        customers_filtered.withColumn(
            "became_member_on",
            F.date_format(
                F.to_date(F.col("became_member_on").cast("string"), "yyyyMMdd"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "gender",
            F.when(F.col("gender").isin("M", "F"), F.upper(F.trim(F.col("gender")))),
        )
        .withColumn("age", F.col("age").cast(IntegerType()))
        .withColumn("income", F.col("income").cast(DoubleType()))
        .withColumn("last_updated", F.current_timestamp())
    )

    events_clean = (
        events_df.withColumn("json", F.from_json(F.col("value"), schema_value))
        .withColumn(
            "offer_id", F.coalesce(F.col("json.offer_id"), F.col("json.`offer id`"))
        )
        .withColumn("amount", F.col("json.amount").cast(DoubleType()))
        .withColumn("reward", F.col("json.reward").cast(IntegerType()))
        .select(
            "customer_id",
            "event",
            "time",
            "offer_id",
            "amount",
            "reward",
            "ingestion_timestamp",
        )
        .withColumn("last_updated", F.current_timestamp())
        .dropDuplicates(["customer_id", "event", "time", "offer_id"])
    )

    offers_clean = (
        offers_df.withColumn("offer_type", F.trim(F.upper(F.col("offer_type"))))
        .withColumn("difficulty", F.col("difficulty").cast(IntegerType()))
        .withColumn("reward", F.col("reward").cast(IntegerType()))
        .withColumn("duration", F.col("duration").cast(IntegerType()))
        .withColumn("last_updated", F.current_timestamp())
        .withColumn("channels_array", F.from_json(F.col("channels"), "array<string>"))
        .withColumn("channel", F.explode(F.col("channels_array")))
        .drop("channels_array")
        .dropDuplicates(["offer_id", "channel"])
    )

    write_silver(
        customers_clean,
        "customers",
        add_timestamp=True,
        partition_by="became_member_on",
    )
    write_silver(
        events_clean,
        "events",
        add_timestamp=True,
        partition_by="event",
    )
    write_silver(
        offers_clean,
        "offers",
        add_timestamp=True,
        partition_by="offer_type",
    )
