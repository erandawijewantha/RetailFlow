"""
Configuration management for RetailFlow Spark jobs
Centralizes all configuration to avoid hardcoding
"""

import os
from dataclasses import dataclass
from typing import Optional 

@dataclass
class MinIOConfig:
    """MinIO/S3 configuration"""
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://mini0:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    bucket: str = os.getenv("MINI_BUCKET", "retailflow")
    
@dataclass
class PostgresConfig:
    """"PostgreSQL warehouse configuration"""
    host: str = os.getenv("WAREHOUSE_HOST", "postgres-warehouse")
    port: int = int(os.getenv("WAREHOUSE_PORT", "5432"))
    database: str = os.getenv("WAREHOUSE_DB", "retailflow")
    user: str = os.getenv("WAREHOUSE_USER", "warehouse")
    password: str = os.getenv("WAREHOUSE_PASSWORD", "warehouse123")
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    @property
    def connection_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }
dataclass
class PathConfig:
    """Data lake path configuration"""
    bronze: str = "s3a://retailflow/bronze"
    silver: str = "s3a://retailflow/silver"
    gold: str = "s3a://retailflow/gold"


class Config:
    """Main configuration class"""

    def __init__(self):
        self.minio = MinIOConfig()
        self.postgres = PostgresConfig()
        self.paths = PathConfig()

    @staticmethod
    def get_spark_configs() -> dict:
        """Get Spark configurations for S3/MinIO access"""
        minio = MinIOConfig()
        return {
            "spark.hadoop.fs.s3a.endpoint": minio.endpoint,
            "spark.hadoop.fs.s3a.access.key": minio.access_key,
            "spark.hadoop.fs.s3a.secret.key": minio.secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }


# Global config instance
config = Config()