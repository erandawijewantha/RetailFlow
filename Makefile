# ============================================================================
# RETAILFLOW MAKEFILE
# ============================================================================
# Common commands for development and deployment
# Usage: make <target>
# ============================================================================

.PHONY: help up down build logs clean test spark-shell pyspark warehouse-shell

# Default target
help:
	@echo "RetailFlow - Available Commands"
	@echo "================================"
	@echo "make up              - Start all services"
	@echo "make down            - Stop all services"
	@echo "make build           - Build Docker images"
	@echo "make logs            - View all logs"
	@echo "make logs-airflow    - View Airflow logs"
	@echo "make logs-spark      - View Spark logs"
	@echo "make clean           - Stop and remove all data"
	@echo "make test            - Run all tests"
	@echo "make spark-shell     - Open Spark shell"
	@echo "make pyspark         - Open PySpark shell"
	@echo "make warehouse-shell - Open PostgreSQL shell"
	@echo "make airflow-shell   - Open Airflow shell"

# Start all services
up:
	cd docker && docker-compose up -d
	@echo ""
	@echo "Services starting..."
	@echo "===================="
	@echo "Airflow UI:     http://localhost:8081 (admin/admin)"
	@echo "Spark UI:       http://localhost:8080"
	@echo "MinIO Console:  http://localhost:9001 (minioadmin/minioadmin123)"
	@echo "Warehouse:      localhost:5433 (warehouse/warehouse123)"
	@echo ""

# Stop all services
down:
	cd docker && docker-compose down

# Build images
build:
	cd docker && docker-compose build

# View logs
logs:
	cd docker && docker-compose logs -f

logs-airflow:
	cd docker && docker-compose logs -f airflow-webserver airflow-scheduler

logs-spark:
	cd docker && docker-compose logs -f spark-master spark-worker-1

# Clean everything (WARNING: Removes all data!)
clean:
	cd docker && docker-compose down -v
	docker system prune -f
	@echo "All containers, volumes, and cached data removed."

# Run tests
test:
	cd docker && docker-compose exec spark-master spark-submit \
		--master local[*] \
		/opt/spark/jobs/tests/run_tests.py

# Open Spark shell
spark-shell:
	cd docker && docker-compose exec spark-master spark-shell

# Open PySpark shell
pyspark:
	cd docker && docker-compose exec spark-master pyspark

# Open warehouse shell
warehouse-shell:
	cd docker && docker-compose exec postgres-warehouse psql -U warehouse -d retailflow

# Open Airflow shell
airflow-shell:
	cd docker && docker-compose exec airflow-webserver bash

# Trigger DAG manually
trigger-dag:
	cd docker && docker-compose exec airflow-webserver \
		airflow dags trigger retailflow_daily_pipeline

# Check service health
health:
	@echo "Checking service health..."
	@curl -s http://localhost:8081/health | head -1 || echo "Airflow: DOWN"
	@curl -s http://localhost:8080 | head -1 || echo "Spark: DOWN"
	@curl -s http://localhost:9000/minio/health/live || echo "MinIO: DOWN"