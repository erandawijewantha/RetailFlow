"""
RetailFlow Daily Pipeline DAG

Orchestrates the complete ETL pipeline:
Bronze → Silver → Gold → Warehouse

Schedule: Daily at 2:00 AM
SLA: Complete by 6:00 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pendulum

# ═══════════════════════════════════════════════════════════════════════════
# DAG CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@retailflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Spark submit base command
SPARK_SUBMIT = """
    docker exec retailflow-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --packages org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4
"""

# ═══════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id='retailflow_daily_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for RetailFlow data warehouse',
    schedule_interval='0 2 * * *',  # 2:00 AM daily
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    tags=['retailflow', 'production', 'daily'],
    doc_md=__doc__,
) as dag:

    # ═══════════════════════════════════════════════════════════════════════
    # START
    # ═══════════════════════════════════════════════════════════════════════

    start = DummyOperator(
        task_id='start',
        doc='Pipeline start marker'
    )

    # ═══════════════════════════════════════════════════════════════════════
    # BRONZE LAYER - Data Ingestion
    # ═══════════════════════════════════════════════════════════════════════

    with TaskGroup(group_id='bronze_ingestion') as bronze_ingestion:

        ingest_orders = BashOperator(
            task_id='ingest_orders',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/ingestion/ingest_orders.py \
                --input-path /opt/spark/data/sample/orders*.csv \
                --date {{{{ ds }}}}
            """,
            doc='Ingest orders from CSV to Bronze layer'
        )

        ingest_products = BashOperator(
            task_id='ingest_products',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/ingestion/ingest_products.py
            """,
            doc='Ingest products from database to Bronze layer'
        )

        ingest_customers = BashOperator(
            task_id='ingest_customers',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/ingestion/ingest_customers.py
            """,
            doc='Ingest customers from API to Bronze layer'
        )

        # These can run in parallel
        [ingest_orders, ingest_products, ingest_customers]

    # ═══════════════════════════════════════════════════════════════════════
    # BRONZE QUALITY CHECK
    # ═══════════════════════════════════════════════════════════════════════

    with TaskGroup(group_id='bronze_quality') as bronze_quality:

        def check_bronze_orders(**context):
            """Validate Bronze orders data quality"""
            from pyspark.sql import SparkSession

            spark = SparkSession.builder \
                .appName("QualityCheck-BronzeOrders") \
                .getOrCreate()

            try:
                df = spark.read.parquet("s3a://retailflow/bronze/orders")
                count = df.count()

                # Quality checks
                assert count > 0, "No records in Bronze orders"

                null_order_ids = df.filter(df.order_id.isNull()).count()
                assert null_order_ids == 0, f"Found {null_order_ids} null order_ids"

                context['ti'].xcom_push(key='bronze_orders_count', value=count)
                return True
            finally:
                spark.stop()

        quality_check_orders = PythonOperator(
            task_id='quality_check_orders',
            python_callable=check_bronze_orders,
            doc='Validate Bronze orders meet quality requirements'
        )

    # ═══════════════════════════════════════════════════════════════════════
    # SILVER LAYER - Transformations
    # ═══════════════════════════════════════════════════════════════════════

    with TaskGroup(group_id='silver_transform') as silver_transform:

        transform_orders = BashOperator(
            task_id='transform_orders',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/transform/orders_silver.py
            """,
            doc='Transform orders from Bronze to Silver'
        )

        transform_customers = BashOperator(
            task_id='transform_customers',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/transform/customers_silver.py
            """,
            doc='Transform customers from Bronze to Silver'
        )

        transform_products = BashOperator(
            task_id='transform_products',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/transform/products_silver.py
            """,
            doc='Transform products from Bronze to Silver'
        )

        [transform_orders, transform_customers, transform_products]

    # ═══════════════════════════════════════════════════════════════════════
    # GOLD LAYER - Dimensional Model
    # ═══════════════════════════════════════════════════════════════════════

    with TaskGroup(group_id='gold_modeling') as gold_modeling:

        build_dim_customer = BashOperator(
            task_id='build_dim_customer',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/gold/dim_customer_scd2.py
            """,
            doc='Build customer dimension with SCD Type 2'
        )

        build_dim_product = BashOperator(
            task_id='build_dim_product',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/gold/dim_product.py
            """,
            doc='Build product dimension'
        )

        build_dim_date = BashOperator(
            task_id='build_dim_date',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/gold/dim_date.py
            """,
            doc='Build/refresh date dimension'
        )

        # Dimensions must complete before fact table
        [build_dim_customer, build_dim_product, build_dim_date]

    # Fact table depends on all dimensions
    build_fact_sales = BashOperator(
        task_id='build_fact_sales',
        bash_command=f"""
            {SPARK_SUBMIT} /opt/spark/jobs/gold/fact_sales.py
        """,
        doc='Build fact_sales table'
    )

    # ═══════════════════════════════════════════════════════════════════════
    # EXPORT TO WAREHOUSE
    # ═══════════════════════════════════════════════════════════════════════

    with TaskGroup(group_id='warehouse_export') as warehouse_export:

        export_to_postgres = BashOperator(
            task_id='export_to_postgres',
            bash_command=f"""
                {SPARK_SUBMIT} /opt/spark/jobs/export/to_postgres.py
            """,
            doc='Export Gold tables to PostgreSQL warehouse'
        )

        refresh_views = BashOperator(
            task_id='refresh_views',
            bash_command="""
                docker exec retailflow-warehouse psql -U warehouse -d retailflow \
                -c "REFRESH MATERIALIZED VIEW IF EXISTS gold.mv_daily_sales;"
            """,
            doc='Refresh materialized views'
        )

        export_to_postgres >> refresh_views

    # ═══════════════════════════════════════════════════════════════════════
    # END
    # ═══════════════════════════════════════════════════════════════════════

    end = DummyOperator(
        task_id='end',
        doc='Pipeline end marker'
    )

    # ═══════════════════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ═══════════════════════════════════════════════════════════════════════

    start >> bronze_ingestion >> bronze_quality >> silver_transform >> gold_modeling >> build_fact_sales >> warehouse_export >> end