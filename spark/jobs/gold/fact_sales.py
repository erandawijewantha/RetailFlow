"""
Fact Sales Builder
Creates fact_sales table by joining orders with dimensions

Implements:
- Surrogate key lookups
- Measure calculations
- Late-arriving fact handling
- Partitioning by date
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import sys

sys.path.insert(0, '/opt/spark/lib')

from spark_utils import create_spark_session
from config import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FactSalesBuilder:
    """Build fact_sales table from Silver orders"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_path = f"{config.paths.silver}/orders"
        self.gold_path = f"{config.paths.gold}/fact_sales"

    def read_silver_orders(self) -> DataFrame:
        """Read orders from Silver layer"""
        logger.info(f"Reading Silver orders from: {self.silver_path}")
        return self.spark.read.parquet(self.silver_path)

    def read_dim_customer(self) -> DataFrame:
        """Read customer dimension (current records only for lookup)"""
        dim_path = f"{config.paths.gold}/dim_customer"
        return self.spark.read.parquet(dim_path) \
            .filter(F.col("is_current") == True) \
            .select(
                F.col("customer_key"),
                F.col("customer_id")
            )

    def read_dim_product(self) -> DataFrame:
        """Read product dimension"""
        # For now, create simple product dimension from Silver
        products_path = f"{config.paths.silver}/products"
        try:
            return self.spark.read.parquet(products_path)
        except:
            # If no products Silver, read from Bronze
            products_bronze = f"{config.paths.bronze}/products"
            df = self.spark.read.parquet(products_bronze)

            window_spec = F.row_number().over(
                Window.orderBy("product_id")
            )

            return df.withColumn("product_key", window_spec) \
                .select("product_key", "product_id", "product_name",
                    "category", "brand", "list_price", "cost_price")

    def lookup_dimension_keys(self, orders_df: DataFrame) -> DataFrame:
        """
        Join with dimensions to get surrogate keys

        Uses left join to handle:
        - Missing customers (guest checkout)
        - Missing products (discontinued)
        """
        logger.info("Looking up dimension keys...")

        # Get dimensions
        dim_customer = self.read_dim_customer()
        dim_product = self.read_dim_product()

        # Join with customer dimension
        result = orders_df.join(
            dim_customer,
            orders_df.customer_id == dim_customer.customer_id,
            "left"
        ).drop(dim_customer.customer_id)

        # Join with product dimension
        result = result.join(
            dim_product.select("product_key", "product_id"),
            orders_df.product_id == dim_product.product_id,
            "left"
        ).drop(dim_product.product_id)

        # Handle missing dimension keys (use -1 for unknown)
        result = result \
            .withColumn("customer_key",
                F.coalesce(F.col("customer_key"), F.lit(-1))) \
            .withColumn("product_key",
                F.coalesce(F.col("product_key"), F.lit(-1)))

        return result

    def calculate_measures(self, df: DataFrame) -> DataFrame:
        """
        Calculate fact measures

        Note: Some calculations may already be in Silver,
        but we recalculate to ensure consistency
        """
        logger.info("Calculating measures...")

        return df \
            .withColumn("gross_amount",
                F.col("quantity") * F.col("unit_price")) \
            .withColumn("discount_amount",
                F.col("gross_amount") * F.coalesce(F.col("discount_pct"), F.lit(0))) \
            .withColumn("net_amount",
                F.col("gross_amount") - F.col("discount_amount")) \
            .withColumn("tax_amount",
                F.col("net_amount") * F.lit(0.08))  # 8% tax

    def build_fact_table(self, df: DataFrame) -> DataFrame:
        """Select and order final fact table columns"""

        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        return df.select(
            # Surrogate keys (for joins)
            F.col("customer_key"),
            F.col("product_key"),
            F.col("order_date_key").alias("date_key"),

            # Degenerate dimensions
            F.col("order_id"),

            # Measures
            F.col("quantity"),
            F.col("unit_price"),
            F.col("discount_pct"),
            F.col("discount_amount"),
            F.col("gross_amount"),
            F.col("net_amount"),
            F.col("tax_amount"),

            # Date for partitioning
            F.col("order_date"),
            F.col("order_year"),
            F.col("order_month"),

            # Status
            F.col("status"),

            # Audit
            F.current_timestamp().alias("created_at"),
            F.lit(batch_id).alias("batch_id")
        )

    def write_fact_table(self, df: DataFrame) -> None:
        """Write fact table partitioned by year/month"""
        logger.info(f"Writing {df.count()} records to: {self.gold_path}")

        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("order_year", "order_month") \
            .save(self.gold_path)

        logger.info("Successfully wrote fact_sales")

    def run(self) -> dict:
        """Main execution"""
        logger.info("Starting fact_sales build...")

        # Read Silver orders
        orders_df = self.read_silver_orders()
        initial_count = orders_df.count()

        # Lookup dimension keys
        df = self.lookup_dimension_keys(orders_df)

        # Calculate measures
        df = self.calculate_measures(df)

        # Build fact table
        fact_df = self.build_fact_table(df)

        # Write
        self.write_fact_table(fact_df)

        # Statistics
        stats = {
            "records_processed": initial_count,
            "records_written": fact_df.count(),
            "status": "SUCCESS"
        }

        logger.info(f"Fact sales build complete: {stats}")
        return stats


def main():
    spark = create_spark_session("RetailFlow-FactSales")

    try:
        builder = FactSalesBuilder(spark)
        stats = builder.run()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Build failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()