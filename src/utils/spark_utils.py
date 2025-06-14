from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Get or create a Spark session with optimized settings."""
    return (
        SparkSession.builder.appName("Cafe-ETL-Pipeline")
        # Logging settings
        .config("spark.log.level", "ERROR")
        .config("spark.driver.log.level", "ERROR")
        .config("spark.executor.log.level", "ERROR")
        .getOrCreate()
    )
