# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.schemas.data_schemas import schema_value

spark = (
    SparkSession.builder.appName("Cafe-ETL-Pipeline")
    .config("spark.log.level", "ERROR")
    .config("spark.driver.log.level", "ERROR")
    .config("spark.executor.log.level", "ERROR")
    .getOrCreate()
)
# %%
df_customers = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/silver/customers"
)
# %%
df_customers.show()
# %%
df_customers.printSchema()

# %%
df_events = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/silver/events"
).show()

df_events = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/silver/customers"
).show()
# %%
df_offers = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/silver/offers"
).show()
# %%
df_events.show()
# %%
df_events.filter(
    (df_events.customer_id == "78afa995795e4d85b5d9ceeca43f5fef")
    & (df_events.offer_id == "9b98b8c7a33c4b65b9aebfe6a799e6d9")
).show()


# %%
df_offers = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/silver/offers"
)
# %%
df_offers.filter(df_offers.offer_id == "0b1e1539f2cc45b7b9fa7c272da2e1d7").show()
df_offers.show()
# %%
df_offers.printSchema()

# %%
from pyspark.sql.functions import (
    col,
    count,
    sum,
    when,
    window,
    current_timestamp,
    date_format,
    avg,
    expr,
    unix_timestamp,
    percentile_approx,
    array,
    lit,
    explode,
    split,
    regexp_replace,
)

# %%
silver_path = "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/silver"
gold_path = "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/gold"

# Read silver data
customers_df = spark.read.parquet(f"{silver_path}/customers")
events_df = spark.read.parquet(f"{silver_path}/events")
offers_df = spark.read.parquet(f"{silver_path}/offers")


offer_channels_df = offers_df.select(
    col("offer_id"), explode(split(col("channels"), ",")).alias("channel")
)

# Get completed offers
completed_offers_df = (
    events_df.filter(col("event") == "offer completed").select("offer_id").distinct()
)

# Join to get which offers (by channel) were completed
channel_stats_df = (
    offer_channels_df.join(completed_offers_df, on="offer_id", how="left_semi")
    .groupBy("channel")
    .agg(count("offer_id").alias("completed_offers"))
)

total_offers_per_channel_df = offer_channels_df.groupBy("channel").agg(
    count("offer_id").alias("total_offers")
)

# Combine and calculate completion rate
from pyspark.sql.functions import round as spark_round

channel_effectiveness_df = (
    total_offers_per_channel_df.join(channel_stats_df, on="channel", how="left")
    .fillna(0, ["completed_offers"])
    .withColumn(
        "completion_rate",
        spark_round(col("completed_offers") * 100.0 / col("total_offers"), 2),
    )
    .orderBy(col("completion_rate").desc())
)

channel_effectiveness_df.show(truncate=False)

# %%
