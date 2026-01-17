"""
Spark utilities for RetailFlow
Provides common functions for Spark job development
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Optional
import logging
from datetime import datetime

from .config import Config, config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str,
                         master: str = "spark://spark-master:7077") -> SparkSession:
    """
    Create configured Spark session for RetailFlow jobs

    Args:
        app_name: Name of the Spark application
        master: Spark master URL

    Returns:
        Configured SparkSession
    """
    logger.info(f"Creating Spark session: {app_name}")

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)

    # Apply S3/MinIO configurations
    for key, value in Config.get_spark_configs().items():
        builder = builder.config(key, value)

    # Additional performance configs
    builder = builder \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Spark session created successfully")
    return spark


def add_audit_columns(df: DataFrame,
                      source_system: str,
                      batch_id: Optional[str] = None) -> DataFrame:
    """
    Add standard audit columns to DataFrame

    Columns added:
    - _ingested_at: Timestamp when data was ingested
    - _source_system: Name of source system
    - _batch_id: Unique batch identifier
    - _file_name: Source file name (if applicable)
    """
    if batch_id is None:
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    return df \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_system", F.lit(source_system)) \
        .withColumn("_batch_id", F.lit(batch_id))


def write_to_bronze(df: DataFrame,
                    table_name: str,
                    partition_cols: list = None,
                    mode: str = "append") -> None:
    """
    Write DataFrame to Bronze layer

    Args:
        df: DataFrame to write
        table_name: Name of the table (e.g., 'orders', 'customers')
        partition_cols: Columns to partition by (default: ['_ingestion_date'])
        mode: Write mode ('append', 'overwrite')
    """
    output_path = f"{config.paths.bronze}/{table_name}"

    if partition_cols is None:
        # Add ingestion date for partitioning
        df = df.withColumn("_ingestion_date", F.current_date())
        partition_cols = ["_ingestion_date"]

    logger.info(f"Writing to Bronze: {output_path}")
    logger.info(f"Partitioning by: {partition_cols}")
    logger.info(f"Record count: {df.count()}")

    writer = df.write \
        .format("parquet") \
        .mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)

    logger.info(f"Successfully wrote to {output_path}")


def read_from_bronze(spark: SparkSession,
                     table_name: str,
                     filter_date: Optional[str] = None) -> DataFrame:
    """
    Read DataFrame from Bronze layer

    Args:
        spark: SparkSession
        table_name: Name of the table
        filter_date: Optional date filter (YYYY-MM-DD)

    Returns:
        DataFrame
    """
    input_path = f"{config.paths.bronze}/{table_name}"

    logger.info(f"Reading from Bronze: {input_path}")

    df = spark.read.parquet(input_path)

    if filter_date:
        df = df.filter(F.col("_ingestion_date") == filter_date)
        logger.info(f"Filtered to date: {filter_date}")

    logger.info(f"Read {df.count()} records")
    return df