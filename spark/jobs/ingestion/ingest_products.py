"""
Products Ingestion Job
Reads products from PostgreSQL source database and writes to Bronze layer

Usage:
    spark-submit ingest_products.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import sys

sys.path.insert(0, '/opt/spark/lib')

from spark_utils import create_spark_session, add_audit_columns, write_to_bronze
from config import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProductsIngestion:
    """Ingests products data from source database to Bronze layer"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.source_system = "products_db"

        # Source database config (simulated - would be different from warehouse)
        self.source_jdbc_url = "jdbc:postgresql://postgres-warehouse:5432/retailflow"
        self.source_properties = {
            "user": "warehouse",
            "password": "warehouse123",
            "driver": "org.postgresql.Driver"
        }

    def read_from_source(self) -> DataFrame:
        """
        Read products from source database

        In production, this would read from the actual source system.
        For this demo, we'll read from a staging table or generate sample data.
        """
        logger.info("Reading products from source database...")

        # For demo: Generate sample product data
        # In production: Read from actual source table

        products_data = [
            ("PROD-001", "Laptop Pro 15", "Electronics", "TechBrand", 1299.99, 899.99, True),
            ("PROD-002", "Wireless Mouse", "Electronics", "TechBrand", 49.99, 25.00, True),
            ("PROD-003", "Office Chair", "Furniture", "ComfortCo", 299.99, 150.00, True),
            ("PROD-004", "Standing Desk", "Furniture", "ComfortCo", 599.99, 350.00, True),
            ("PROD-005", "Monitor 27inch", "Electronics", "ViewMax", 399.99, 250.00, True),
            ("PROD-006", "Keyboard Mechanical", "Electronics", "TypeMaster", 149.99, 75.00, True),
            ("PROD-007", "Webcam HD", "Electronics", "ViewMax", 79.99, 40.00, True),
            ("PROD-008", "Desk Lamp LED", "Furniture", "LightUp", 39.99, 15.00, True),
            ("PROD-009", "Cable USB-C", "Accessories", "TechBrand", 19.99, 5.00, True),
            ("PROD-010", "Laptop Bag", "Accessories", "CarryAll", 89.99, 35.00, True),
        ]

        schema = ["product_id", "product_name", "category", "brand",
                  "list_price", "cost_price", "is_active"]

        df = self.spark.createDataFrame(products_data, schema)

        # Add timestamps
        df = df \
            .withColumn("created_at", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp())

        logger.info(f"Read {df.count()} products")
        return df

    def run(self) -> dict:
        """Main execution method"""
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting products ingestion - Batch ID: {batch_id}")

        # Read from source
        df = self.read_from_source()

        # Add audit columns
        df = add_audit_columns(df, self.source_system, batch_id)

        # Write to Bronze (full refresh for dimension data)
        write_to_bronze(df, "products", mode="overwrite")

        stats = {
            "batch_id": batch_id,
            "records_loaded": df.count(),
            "status": "SUCCESS"
        }

        logger.info(f"Products ingestion complete: {stats}")
        return stats


def main():
    spark = create_spark_session("RetailFlow-ProductsIngestion")

    try:
        ingestion = ProductsIngestion(spark)
        stats = ingestion.run()
        sys.exit(0 if stats["status"] == "SUCCESS" else 1)
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()