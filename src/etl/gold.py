import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    avg,
)
from ..utils.spark_utils import get_spark_session
from ..utils.writer import write_gold


def process_gold(spark: SparkSession) -> None:

    silver_path = "data/silver"
    gold_path = "data/gold"

    # Read silver data
    customers_df = spark.read.parquet(f"{silver_path}/customers")
    events_df = spark.read.parquet(f"{silver_path}/events")
    offers_df = spark.read.parquet(f"{silver_path}/offers")

    # Clean channels data - convert string representation to array
    offers_df = offers_df.dropDuplicates(["offer_id", "channel"])

    # Calculate offer completion rates by channel
    channel_effectiveness = (
        events_df.join(offers_df, on="offer_id", how="inner")
        .groupBy("channel")
        .agg(
            count(when(col("event") == "offer received", 1)).alias("offers_received"),
            count(when(col("event") == "offer completed", 1)).alias("offers_completed"),
        )
        .withColumn(
            "completion_rate",
            (col("offers_completed") / col("offers_received") * 100).cast(
                "decimal(5,2)"
            ),
        )
        .orderBy(col("completion_rate").desc())
    )

    # Write the results
    write_gold(channel_effectiveness, "channel_effectiveness")

    # Analyze age distribution of offer completion
    age_analysis = (
        customers_df.join(
            events_df.filter(col("event").isin("offer received", "offer completed")),
            on="customer_id",
            how="inner",
        )
        .withColumn(
            "age_group",
            when(col("age") < 20, "Under 20")
            .when(col("age") < 30, "20-29")
            .when(col("age") < 40, "30-39")
            .when(col("age") < 50, "40-49")
            .when(col("age") < 60, "50-59")
            .when(col("age") < 70, "60-69")
            .otherwise("70+"),
        )
        .groupBy("age_group")
        .agg(
            count(when(col("event") == "offer received", 1)).alias("offers_received"),
            count(when(col("event") == "offer completed", 1)).alias("offers_completed"),
            avg("age").alias("avg_age"),
        )
        .withColumn(
            "completion_rate",
            (col("offers_completed") / col("offers_received") * 100).cast(
                "decimal(5,2)"
            ),
        )
        .orderBy("age_group")
    )

    # Write age analysis results
    write_gold(age_analysis, "age_analysis")

    # Calculate average time to complete an offer
    received = events_df.filter(col("event") == "offer received").select(
        "customer_id", "offer_id", col("time").alias("received_time")
    )
    completed = events_df.filter(col("event") == "offer completed").select(
        "customer_id", "offer_id", col("time").alias("completed_time")
    )

    offer_timings = (
        received.join(completed, on=["customer_id", "offer_id"], how="inner")
        .withColumn("time_to_complete", col("completed_time") - col("received_time"))
        .filter(col("time_to_complete") >= 0)
    )

    avg_time_to_complete = offer_timings.agg(
        avg("time_to_complete").alias("avg_time_to_complete")
    ).collect()[0]["avg_time_to_complete"]

    avg_time_df = spark.createDataFrame(
        [(avg_time_to_complete,)], ["avg_time_to_complete"]
    )

    write_gold(avg_time_df, "avg_time_to_complete")
