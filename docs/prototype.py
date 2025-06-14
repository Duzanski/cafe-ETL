# %%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Cafe-ETL-Pipeline")
    .config("spark.log.level", "ERROR")
    .config("spark.driver.log.level", "ERROR")
    .config("spark.executor.log.level", "ERROR")
    .getOrCreate()
)
# %%
df_customers = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/bronze/customers"
)
# %%
df_customers.show()
# %%
df_customers.printSchema()

# %%
df_events = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/bronze/events"
)
# %%
df_events.show()
# %%
df_events.printSchema()
# %%

# %%
df_offers = spark.read.parquet(
    "/home/duzanski/projetos/cafe-ETL/cafe-ETL/data/bronze/offers"
)
# %%
df_offers.show()
# %%
df_offers.printSchema()
