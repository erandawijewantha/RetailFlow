"""
Schema definitions for RetailFlow data sources
Defines expected schemas for data validation
"""

from pyspark.sql.types import *


class OrdersSchema:
    """Schema for Orders data"""

    # Raw CSV schema (as strings initially)
    RAW = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", StringType(), False),
        StructField("unit_price", StringType(), False),
        StructField("discount_pct", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("status", StringType(), True),
    ])

    # Typed schema (after transformation)
    TYPED = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_date", TimestampType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("discount_pct", DecimalType(5, 4), True),
        StructField("shipping_address", StringType(), True),
        StructField("status", StringType(), True),
    ])


class ProductsSchema:
    """Schema for Products data"""

    TYPED = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category_id", IntegerType(), True),
        StructField("brand", StringType(), True),
        StructField("list_price", DecimalType(10, 2), True),
        StructField("cost_price", DecimalType(10, 2), True),
        StructField("is_active", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
    ])


class CustomersSchema:
    """Schema for Customers data (from API)"""

    TYPED = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("last_updated", TimestampType(), True),
    ])