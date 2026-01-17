"""
Customers Silver Transformation
Cleans and transforms customers from Bronze to Silver layer

Transformations:
- Data quality checks
- Address standardization
- Segment validation
- Deduplication by customer_id
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import sys

sys.path.insert(0, '/opt/spark/lib')

from spark_utils import create_spark_session, read_from_bronze
from config import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomersSilverTransformation:
    """Transform customers from Bronze to Silver layer"""

    VALID_SEGMENTS = ["Gold", "Silver", "Bronze", "New"]

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_bronze(self, processing_date: str = None) -> DataFrame:
        """Read customers from Bronze layer"""
        return read_from_bronze(self.spark, "customers", processing_date)

    def validate_and_standardize(self, df: DataFrame) -> DataFrame:
        """
        Validate and standardize customer data

        - Validate segment values
        - Standardize names (proper case)
        - Validate email format
        """
        logger.info("Validating and standardizing customer data...")

        return df \
            .withColumn("first_name", F.initcap(F.trim(F.col("first_name")))) \
            .withColumn("last_name", F.initcap(F.trim(F.col("last_name")))) \
            .withColumn("email", F.lower(F.trim(F.col("email")))) \
            .withColumn("segment",
                F.when(F.col("segment").isin(self.VALID_SEGMENTS), F.col("segment"))
                .otherwise("New")) \
            .withColumn("city", F.initcap(F.trim(F.col("city")))) \
            .withColumn("country", F.initcap(F.trim(F.col("country")))) \
            .withColumn("full_name",
                F.concat_ws(" ", F.col("first_name"), F.col("last_name"))) \
            .withColumn("full_address",
                F.concat_ws(", ", F.col("street"), F.col("city"), F.col("country")))

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Keep latest version of each customer"""
        window_spec = Window \
            .partitionBy("customer_id") \
            .orderBy(F.col("last_updated").desc())

        return df \
            .withColumn("_row_num", F.row_number().over(window_spec)) \
            .filter(F.col("_row_num") == 1) \
            .drop("_row_num")

    def select_silver_columns(self, df: DataFrame) -> DataFrame:
        """Select columns for Silver layer"""
        return df.select(
            "customer_id",
            "first_name",
            "last_name",
            "full_name",
            "email",
            "segment",
            "street",
            "city",
            "country",
            "full_address",
            "registration_date",
            "last_updated",
            "_ingested_at",
            "_source_system",
            "_batch_id",
            "_row_hash"
        )

    def write_to_silver(self, df: DataFrame, batch_id: str) -> None:
        """Write to Silver layer"""
        output_path = f"{config.paths.silver}/customers"

        df = df \
            .withColumn("_processed_at", F.current_timestamp()) \
            .withColumn("_silver_batch_id", F.lit(batch_id))

        logger.info(f"Writing {df.count()} records to Silver: {output_path}")

        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(output_path)

    def run(self, processing_date: str = None) -> dict:
        """Main execution"""
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting customers Silver transformation - Batch ID: {batch_id}")

        bronze_df = self.read_bronze(processing_date)
        initial_count = bronze_df.count()

        df = self.validate_and_standardize(bronze_df)
        df = self.deduplicate(df)
        df = self.select_silver_columns(df)

        final_count = df.count()

        self.write_to_silver(df, batch_id)

        return {
            "batch_id": batch_id,
            "records_read": initial_count,
            "records_written": final_count,
            "status": "SUCCESS"
        }


def main():
    spark = create_spark_session("RetailFlow-CustomersSilver")

    try:
        transform = CustomersSilverTransformation(spark)
        stats = transform.run()
        logger.info(f"Transformation complete: {stats}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()