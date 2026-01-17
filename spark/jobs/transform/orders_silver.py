"""
Orders Silver Transformation
Cleans and transforms orders from Bronze to Silver layer

Transformations:
- Data type casting
- Date parsing
- Discount normalization
- Derived columns
- Deduplication
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import sys

sys.path.insert(0, '/opt/spark/lib')

from spark_utils import (
    create_spark_session,
    read_from_bronze,
    add_audit_columns
)
from config import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrdersSilverTransformation:
    """Transform orders from Bronze to Silver layer"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_bronze(self, processing_date: str = None) -> DataFrame:
        """Read orders from Bronze layer"""
        return read_from_bronze(self.spark, "orders", processing_date)

    def cast_data_types(self, df: DataFrame) -> DataFrame:
        """
        Cast columns to appropriate data types

        Bronze layer stores everything as strings to preserve raw data.
        Silver layer applies proper typing.
        """
        logger.info("Casting data types...")

        return df \
            .withColumn("order_date",
                F.to_timestamp(F.col("order_date"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("quantity",
                F.col("quantity").cast("int")) \
            .withColumn("unit_price",
                F.col("unit_price").cast("decimal(10,2)")) \
            .withColumn("discount_pct",
                # Handle both "0.10" and "10%" formats
                F.when(F.col("discount_pct").contains("%"),
                    F.regexp_extract(F.col("discount_pct"), r"(\d+\.?\d*)", 1).cast("decimal(5,4)") / 100
                ).otherwise(
                    F.col("discount_pct").cast("decimal(5,4)")
                ))

    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """
        Add calculated/derived columns

        Business logic applied at Silver layer.
        """
        logger.info("Adding derived columns...")

        return df \
            .withColumn("gross_amount",
                F.col("quantity") * F.col("unit_price")) \
            .withColumn("discount_amount",
                F.col("gross_amount") * F.coalesce(F.col("discount_pct"), F.lit(0))) \
            .withColumn("net_amount",
                F.col("gross_amount") - F.col("discount_amount")) \
            .withColumn("order_date_key",
                F.date_format(F.col("order_date"), "yyyyMMdd").cast("int")) \
            .withColumn("order_year",
                F.year(F.col("order_date"))) \
            .withColumn("order_month",
                F.month(F.col("order_date"))) \
            .withColumn("order_day",
                F.dayofmonth(F.col("order_date")))

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate records

        Uses order_id as unique key.
        Keeps the most recent version based on _ingested_at.
        """
        logger.info("Deduplicating records...")

        window_spec = Window \
            .partitionBy("order_id") \
            .orderBy(F.col("_ingested_at").desc())

        deduped_df = df \
            .withColumn("_row_num", F.row_number().over(window_spec)) \
            .filter(F.col("_row_num") == 1) \
            .drop("_row_num")

        original_count = df.count()
        deduped_count = deduped_df.count()
        duplicates_removed = original_count - deduped_count

        logger.info(f"Removed {duplicates_removed} duplicate records")

        return deduped_df

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Apply data cleaning rules

        - Trim whitespace
        - Standardize status values
        - Handle nulls
        """
        logger.info("Cleaning data...")

        return df \
            .withColumn("order_id", F.trim(F.col("order_id"))) \
            .withColumn("customer_id", F.trim(F.col("customer_id"))) \
            .withColumn("product_id", F.trim(F.col("product_id"))) \
            .withColumn("status",
                F.lower(F.trim(F.col("status")))) \
            .withColumn("status",
                F.when(F.col("status").isin(["complete", "done", "finished"]), "completed")
                .when(F.col("status").isin(["ship", "in transit"]), "shipped")
                .otherwise(F.col("status")))

    def select_silver_columns(self, df: DataFrame) -> DataFrame:
        """Select and order columns for Silver layer"""
        return df.select(
            # Business keys
            "order_id",
            "customer_id",
            "product_id",

            # Order details
            "order_date",
            "order_date_key",
            "order_year",
            "order_month",
            "order_day",

            # Quantities and amounts
            "quantity",
            "unit_price",
            "discount_pct",
            "discount_amount",
            "gross_amount",
            "net_amount",

            # Other attributes
            "shipping_address",
            "status",

            # Audit columns
            "_ingested_at",
            "_source_system",
            "_batch_id",
            "_row_hash"
        )

    def write_to_silver(self, df: DataFrame, batch_id: str) -> None:
        """Write transformed data to Silver layer"""
        output_path = f"{config.paths.silver}/orders"

        # Add Silver layer audit columns
        df = df \
            .withColumn("_processed_at", F.current_timestamp()) \
            .withColumn("_silver_batch_id", F.lit(batch_id))

        logger.info(f"Writing {df.count()} records to Silver: {output_path}")

        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("order_year", "order_month") \
            .save(output_path)

        logger.info("Successfully wrote to Silver layer")

    def run(self, processing_date: str = None) -> dict:
        """Main execution method"""
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting orders Silver transformation - Batch ID: {batch_id}")

        # Read from Bronze
        bronze_df = self.read_bronze(processing_date)
        initial_count = bronze_df.count()

        # Apply transformations
        df = self.cast_data_types(bronze_df)
        df = self.clean_data(df)
        df = self.add_derived_columns(df)
        df = self.deduplicate(df)
        df = self.select_silver_columns(df)

        final_count = df.count()

        # Write to Silver
        self.write_to_silver(df, batch_id)

        stats = {
            "batch_id": batch_id,
            "records_read": initial_count,
            "records_written": final_count,
            "records_dropped": initial_count - final_count,
            "status": "SUCCESS"
        }

        logger.info(f"Silver transformation complete: {stats}")
        return stats


def main():
    spark = create_spark_session("RetailFlow-OrdersSilver")

    try:
        transform = OrdersSilverTransformation(spark)
        stats = transform.run()
        sys.exit(0 if stats["status"] == "SUCCESS" else 1)
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()