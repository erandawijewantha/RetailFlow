"""
Customer Dimension with SCD Type 2
Tracks historical changes to customer attributes

SCD Type 2 Implementation:
- Maintains full history of changes
- Uses effective_date and expiry_date for versioning
- is_current flag for easy current record access
- Surrogate keys for fact table relationships
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, date
import sys

sys.path.insert(0, '/opt/spark/lib')

from spark_utils import create_spark_session
from config import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomerDimensionSCD2:
    """
    Implements SCD Type 2 for Customer Dimension

    Tracked attributes (changes create new version):
    - email
    - segment
    - city
    - country

    Non-tracked attributes (overwritten):
    - first_name, last_name (assumed to be corrections)
    """

    TRACKED_COLUMNS = ["email", "segment", "city", "country"]

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.gold_path = f"{config.paths.gold}/dim_customer"
        self.today = date.today()
        self.max_date = date(2099, 12, 31)

    def read_silver_customers(self) -> DataFrame:
        """Read current customers from Silver layer"""
        silver_path = f"{config.paths.silver}/customers"
        logger.info(f"Reading Silver customers from: {silver_path}")
        return self.spark.read.parquet(silver_path)

    def read_existing_dimension(self) -> DataFrame:
        """Read existing dimension table (if exists)"""
        try:
            df = self.spark.read.parquet(self.gold_path)
            logger.info(f"Read {df.count()} existing dimension records")
            return df
        except Exception as e:
            logger.info("No existing dimension found, will create new")
            return None

    def generate_hash(self, df: DataFrame) -> DataFrame:
        """Generate hash of tracked columns for change detection"""
        return df.withColumn(
            "_tracking_hash",
            F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit(""))
                                      for c in self.TRACKED_COLUMNS]), 256)
        )

    def initialize_dimension(self, silver_df: DataFrame) -> DataFrame:
        """
        Create initial dimension table when none exists
        All records are current, version 1
        """
        logger.info("Initializing new dimension table...")

        # Generate surrogate keys using row_number
        window_spec = Window.orderBy("customer_id")

        return silver_df \
            .withColumn("customer_key", F.row_number().over(window_spec)) \
            .withColumn("effective_date", F.lit(self.today)) \
            .withColumn("expiry_date", F.lit(self.max_date)) \
            .withColumn("is_current", F.lit(True)) \
            .withColumn("version", F.lit(1)) \
            .withColumn("_tracking_hash",
                F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit(""))
                                          for c in self.TRACKED_COLUMNS]), 256)) \
            .select(
                "customer_key",
                "customer_id",
                "first_name",
                "last_name",
                "full_name",
                "email",
                "segment",
                "city",
                "country",
                "full_address",
                "registration_date",
                "effective_date",
                "expiry_date",
                "is_current",
                "version",
                "_tracking_hash"
            )

    def process_scd2(self, silver_df: DataFrame,
                     existing_df: DataFrame) -> DataFrame:
        """
        Process SCD Type 2 changes

        Logic:
        1. Find new customers → INSERT
        2. Find changed customers → EXPIRE old + INSERT new
        3. Find unchanged customers → No action
        4. Find deleted customers → Optional: EXPIRE (not implemented here)
        """
        logger.info("Processing SCD Type 2 changes...")

        # Add hash to incoming data
        silver_df = self.generate_hash(silver_df)

        # Get current dimension records only
        current_dim = existing_df.filter(F.col("is_current") == True)

        # Get max surrogate key
        max_key = existing_df.agg(F.max("customer_key")).collect()[0][0] or 0

        # ═══════════════════════════════════════════════════════════════
        # IDENTIFY CHANGES
        # ═══════════════════════════════════════════════════════════════

        # Join to find matches
        joined = silver_df.alias("new").join(
            current_dim.alias("old"),
            F.col("new.customer_id") == F.col("old.customer_id"),
            "left"
        )

        # NEW: customers not in current dimension
        new_customers = joined.filter(F.col("old.customer_key").isNull())
        new_count = new_customers.count()
        logger.info(f"New customers: {new_count}")

        # CHANGED: hash is different
        changed_customers = joined.filter(
            (F.col("old.customer_key").isNotNull()) &
            (F.col("new._tracking_hash") != F.col("old._tracking_hash"))
        )
        changed_count = changed_customers.count()
        logger.info(f"Changed customers: {changed_count}")

        # UNCHANGED: hash matches
        unchanged_count = joined.filter(
            (F.col("old.customer_key").isNotNull()) &
            (F.col("new._tracking_hash") == F.col("old._tracking_hash"))
        ).count()
        logger.info(f"Unchanged customers: {unchanged_count}")

        # ═══════════════════════════════════════════════════════════════
        # PROCESS NEW CUSTOMERS
        # ═══════════════════════════════════════════════════════════════

        new_records = new_customers.select(
            F.col("new.customer_id"),
            F.col("new.first_name"),
            F.col("new.last_name"),
            F.col("new.full_name"),
            F.col("new.email"),
            F.col("new.segment"),
            F.col("new.city"),
            F.col("new.country"),
            F.col("new.full_address"),
            F.col("new.registration_date"),
            F.col("new._tracking_hash")
        ).withColumn("effective_date", F.lit(self.today)) \
         .withColumn("expiry_date", F.lit(self.max_date)) \
         .withColumn("is_current", F.lit(True)) \
         .withColumn("version", F.lit(1))

        # Assign surrogate keys to new records
        new_window = Window.orderBy("customer_id")
        new_records = new_records.withColumn(
            "customer_key",
            F.row_number().over(new_window) + max_key
        )

        # ═══════════════════════════════════════════════════════════════
        # PROCESS CHANGED CUSTOMERS
        # ═══════════════════════════════════════════════════════════════

        if changed_count > 0:
            # Get customer_ids that changed
            changed_ids = [row.customer_id for row in
                          changed_customers.select("new.customer_id").collect()]

            # Expire old current records (update in place)
            expired_records = existing_df.filter(
                (F.col("customer_id").isin(changed_ids)) &
                (F.col("is_current") == True)
            ).withColumn("expiry_date", F.lit(self.today)) \
             .withColumn("is_current", F.lit(False))

            # Keep unchanged historical records as-is
            unchanged_historical = existing_df.filter(
                ~((F.col("customer_id").isin(changed_ids)) &
                  (F.col("is_current") == True))
            )

            # Create new current records for changed customers
            # Get max version per customer
            version_df = existing_df.groupBy("customer_id").agg(
                F.max("version").alias("max_version"),
                F.max("customer_key").alias("last_key")
            )

            new_versions = changed_customers.select(
                F.col("new.customer_id"),
                F.col("new.first_name"),
                F.col("new.last_name"),
                F.col("new.full_name"),
                F.col("new.email"),
                F.col("new.segment"),
                F.col("new.city"),
                F.col("new.country"),
                F.col("new.full_address"),
                F.col("new.registration_date"),
                F.col("new._tracking_hash")
            ).join(version_df, "customer_id") \
             .withColumn("effective_date", F.lit(self.today)) \
             .withColumn("expiry_date", F.lit(self.max_date)) \
             .withColumn("is_current", F.lit(True)) \
             .withColumn("version", F.col("max_version") + 1)

            # Assign new surrogate keys
            max_key_after_new = max_key + new_count
            change_window = Window.orderBy("customer_id")
            new_versions = new_versions.withColumn(
                "customer_key",
                F.row_number().over(change_window) + max_key_after_new
            ).drop("max_version", "last_key")

            # Combine all records
            result = unchanged_historical \
                .unionByName(expired_records, allowMissingColumns=True) \
                .unionByName(new_records, allowMissingColumns=True) \
                .unionByName(new_versions, allowMissingColumns=True)
        else:
            # No changes, just add new records to existing
            result = existing_df.unionByName(new_records, allowMissingColumns=True)

        return result

    def write_dimension(self, df: DataFrame) -> None:
        """Write dimension table to Gold layer"""
        # Select final columns in correct order
        final_df = df.select(
            "customer_key",
            "customer_id",
            "first_name",
            "last_name",
            "full_name",
            "email",
            "segment",
            "city",
            "country",
            "full_address",
            "registration_date",
            "effective_date",
            "expiry_date",
            "is_current",
            "version",
            "_tracking_hash"
        ).withColumn("_updated_at", F.current_timestamp())

        logger.info(f"Writing {final_df.count()} records to dimension: {self.gold_path}")

        final_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(self.gold_path)

        # Log statistics
        current_count = final_df.filter(F.col("is_current") == True).count()
        historical_count = final_df.filter(F.col("is_current") == False).count()
        logger.info(f"Current records: {current_count}")
        logger.info(f"Historical records: {historical_count}")

    def run(self) -> dict:
        """Main execution"""
        logger.info("Starting Customer Dimension SCD Type 2 processing...")

        # Read inputs
        silver_df = self.read_silver_customers()
        existing_df = self.read_existing_dimension()

        # Process
        if existing_df is None:
            result_df = self.initialize_dimension(silver_df)
        else:
            result_df = self.process_scd2(silver_df, existing_df)

        # Write
        self.write_dimension(result_df)

        return {
            "total_records": result_df.count(),
            "current_records": result_df.filter(F.col("is_current") == True).count(),
            "status": "SUCCESS"
        }


def main():
    spark = create_spark_session("RetailFlow-DimCustomerSCD2")

    try:
        processor = CustomerDimensionSCD2(spark)
        stats = processor.run()
        logger.info(f"SCD2 processing complete: {stats}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()