"""
Customers Ingestion Job
Reads customers from API (simulated) and writes to Bronze layer

Usage:
    spark-submit ingest_customers.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, date
import sys
import json

sys.path.insert(0, '/opt/spark/lib')

from spark_utils import create_spark_session, add_audit_columns, write_to_bronze
from schemas import CustomersSchema
from config import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomersIngestion:
    """Ingests customers data from API to Bronze layer"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.source_system = "customers_api"

    def fetch_from_api(self) -> DataFrame:
        """
        Fetch customers from API

        In production, this would call actual REST API.
        For demo, we generate sample data that simulates API response.
        """
        logger.info("Fetching customers from API...")

        # Simulated API response data
        customers_data = [
            ("CUST-001", "John", "Doe", "john.doe@email.com", "Gold",
             "123 Main St", "Colombo", "Sri Lanka", "2022-01-15", "2024-01-10T08:30:00"),
            ("CUST-002", "Jane", "Smith", "jane.smith@email.com", "Silver",
             "456 Oak Ave", "Kandy", "Sri Lanka", "2022-03-20", "2024-01-12T10:15:00"),
            ("CUST-003", "Bob", "Johnson", "bob.j@email.com", "Bronze",
             "789 Pine Rd", "Galle", "Sri Lanka", "2022-06-10", "2024-01-08T14:20:00"),
            ("CUST-004", "Alice", "Williams", "alice.w@email.com", "Gold",
             "321 Elm St", "Colombo", "Sri Lanka", "2021-11-05", "2024-01-15T09:00:00"),
            ("CUST-005", "Charlie", "Brown", "charlie.b@email.com", "Silver",
             "654 Maple Dr", "Negombo", "Sri Lanka", "2023-02-28", "2024-01-14T16:45:00"),
            ("CUST-006", "Diana", "Miller", "diana.m@email.com", "Bronze",
             "987 Cedar Ln", "Jaffna", "Sri Lanka", "2023-05-15", "2024-01-13T11:30:00"),
            ("CUST-007", "Edward", "Davis", "edward.d@email.com", "Gold",
             "147 Birch Ct", "Colombo", "Sri Lanka", "2021-08-22", "2024-01-11T13:00:00"),
            ("CUST-008", "Fiona", "Garcia", "fiona.g@email.com", "Silver",
             "258 Walnut Way", "Kandy", "Sri Lanka", "2022-09-10", "2024-01-09T15:20:00"),
            ("CUST-009", "George", "Martinez", "george.m@email.com", "Bronze",
             "369 Spruce Ave", "Matara", "Sri Lanka", "2023-07-04", "2024-01-07T10:45:00"),
            ("CUST-010", "Hannah", "Anderson", "hannah.a@email.com", "New",
             "480 Ash Blvd", "Colombo", "Sri Lanka", "2024-01-02", "2024-01-16T08:00:00"),
        ]

        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("last_updated", StringType(), True),
        ])

        df = self.spark.createDataFrame(customers_data, schema)

        logger.info(f"Fetched {df.count()} customers from API")
        return df

    def parse_api_response(self, df: DataFrame) -> DataFrame:
        """
        Parse and clean API response data

        Handles:
        - Date/timestamp parsing
        - Null value handling
        - Data type conversion
        """
        return df \
            .withColumn("registration_date",
                F.to_date(F.col("registration_date"), "yyyy-MM-dd")) \
            .withColumn("last_updated",
                F.to_timestamp(F.col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss"))

    def run(self) -> dict:
        """Main execution method"""
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting customers ingestion - Batch ID: {batch_id}")

        # Fetch from API
        raw_df = self.fetch_from_api()

        # Parse response
        parsed_df = self.parse_api_response(raw_df)

        # Add audit columns
        bronze_df = add_audit_columns(parsed_df, self.source_system, batch_id)

        # Add row hash for change detection
        bronze_df = bronze_df.withColumn(
            "_row_hash",
            F.sha2(F.concat_ws("|",
                F.col("customer_id"),
                F.col("email"),
                F.col("segment"),
                F.col("city"),
                F.col("country")
            ), 256)
        )

        # Write to Bronze
        write_to_bronze(bronze_df, "customers")

        stats = {
            "batch_id": batch_id,
            "records_loaded": bronze_df.count(),
            "status": "SUCCESS"
        }

        logger.info(f"Customers ingestion complete: {stats}")
        return stats


def main():
    spark = create_spark_session("RetailFlow-CustomersIngestion")

    try:
        ingestion = CustomersIngestion(spark)
        stats = ingestion.run()
        sys.exit(0 if stats["status"] == "SUCCESS" else 1)
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()