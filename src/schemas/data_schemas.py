from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
    DateType,
)

# Customer data schema
customers_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("became_member_on", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", StringType(), True),
        StructField("income", StringType(), True),
    ]
)

# Events data schema
events_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("event", StringType(), False),
        StructField("value", StringType(), True),
        StructField("time", StringType(), False),
    ]
)

# Offers data schema
offers_schema = StructType(
    [
        StructField("offer_id", StringType(), False),
        StructField("offer_type", StringType(), False),
        StructField("difficulty", StringType(), False),
        StructField("reward", StringType(), False),
        StructField("duration", StringType(), False),
        StructField("channels", StringType(), False),
    ]
)
