import logging
from src.etl.bronze import ingest_bronze
from src.utils.spark_utils import get_spark_session
from src.etl.silver import process_silver
from src.etl.gold import process_gold

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def main():
    """Main ETL pipeline execution."""
    spark = None
    try:
        logger.info("Starting ETL pipeline...")

        # Initialize Spark
        spark = get_spark_session()
        logger.info("Spark session created successfully")

        # Process Bronze Layer
        logger.info("Processing Bronze layer...")
        ingest_bronze(spark)
        logger.info("Bronze layer processing completed")

        # Process Silver Layer
        logger.info("Processing Silver layer...")
        # process_silver(spark)
        logger.info("Silver layer processing completed")

        # Process Gold Layer
        logger.info("Processing Gold layer...")
        # process_gold(spark)
        logger.info("Gold layer processing completed")

        logger.info("ETL pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
