"""
Orders Ingestion Job
Reads orders from CSV files and writes to Bronze layer

Usage:
    spark-submit ingest_orders.py [--date YYYY-MM-DD] [--input-path PATH]
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys
import os

# Add lib to path
sys.path.insert(0, '/opt/spark/lib')

from spark_utils import create_spark_session, add_audit_columns, write_to_bronze
from schemas import OrdersSchema
from config import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrdersIngestion:
    """Ingests orders data from CSV to Bronze layer"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.source_system = "orders_csv"

    def read_csv(self, input_path: str) -> DataFrame:
        """
        Read orders from CSV file(s)

        Handles:
        - Multiple files (glob patterns)
        - Header row
        - Quote handling
        - Malformed records
        """
        logger.info(f"Reading orders from: {input_path}")

        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .schema(OrdersSchema.RAW) \
            .csv(input_path)

        record_count = df.count()
        logger.info(f"Read {record_count} records from CSV")

        # Log any corrupt records
        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"Found {corrupt_count} corrupt records")

        return df

    def validate_data(self, df: DataFrame) -> tuple:
        """
        Validate data quality and separate good/bad records

        Returns:
            Tuple of (valid_df, invalid_df)
        """
        logger.info("Validating order data...")

        # Define validation rules
        valid_df = df.filter(
            # Required fields not null
            F.col("order_id").isNotNull() &
            F.col("order_date").isNotNull() &
            F.col("product_id").isNotNull() &
            F.col("quantity").isNotNull() &
            F.col("unit_price").isNotNull() &
            # Basic data quality
            (F.col("quantity").cast("int") > 0) &
            (F.col("unit_price").cast("decimal(10,2)") >= 0)
        )

        invalid_df = df.subtract(valid_df)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        total_count = df.count()

        logger.info(f"Validation results:")
        logger.info(f"  Valid records: {valid_count} ({valid_count/total_count*100:.1f}%)")
        logger.info(f"  Invalid records: {invalid_count} ({invalid_count/total_count*100:.1f}%)")

        return valid_df, invalid_df

    def transform_to_bronze(self, df: DataFrame, batch_id: str) -> DataFrame:
        """
        Transform raw data for Bronze layer

        Bronze layer keeps data mostly as-is but adds:
        - Audit columns
        - Source file information
        - Ingestion timestamp
        """
        # Add audit columns
        df = add_audit_columns(df, self.source_system, batch_id)

        # Add processing metadata
        df = df \
            .withColumn("_row_hash",
                F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit(""))
                                         for c in df.columns if not c.startswith("_")]), 256))

        return df

    def run(self, input_path: str, execution_date: str = None) -> dict:
        """
        Main execution method

        Args:
            input_path: Path to input CSV file(s)
            execution_date: Date for partitioning (YYYY-MM-DD)

        Returns:
            Dictionary with execution statistics
        """
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting orders ingestion - Batch ID: {batch_id}")

        # Read source data
        raw_df = self.read_csv(input_path)

        # Validate
        valid_df, invalid_df = self.validate_data(raw_df)

        # Transform
        bronze_df = self.transform_to_bronze(valid_df, batch_id)

        # Write valid records to Bronze
        write_to_bronze(bronze_df, "orders")

        # Write invalid records to dead letter table (for investigation)
        if invalid_df.count() > 0:
            invalid_with_audit = add_audit_columns(invalid_df, self.source_system, batch_id)
            write_to_bronze(invalid_with_audit, "orders_rejected")
            logger.warning(f"Wrote {invalid_df.count()} rejected records to orders_rejected")

        stats = {
            "batch_id": batch_id,
            "records_read": raw_df.count(),
            "records_valid": valid_df.count(),
            "records_invalid": invalid_df.count(),
            "status": "SUCCESS"
        }

        logger.info(f"Ingestion complete: {stats}")
        return stats


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Ingest orders to Bronze layer")
    parser.add_argument("--input-path",
                        default="/opt/spark/data/sample/orders*.csv",
                        help="Path to input CSV file(s)")
    parser.add_argument("--date",
                        default=datetime.now().strftime("%Y-%m-%d"),
                        help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session("RetailFlow-OrdersIngestion")

    try:
        # Run ingestion
        ingestion = OrdersIngestion(spark)
        stats = ingestion.run(args.input_path, args.date)

        # Exit with appropriate code
        if stats["status"] == "SUCCESS":
            sys.exit(0)
        else:
            sys.exit(1)

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()