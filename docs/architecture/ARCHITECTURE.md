# RetailFlow - System Architecture

## Overview

RetailFlow implements a modern data lakehouse architecture using the
Medallion pattern (Bronze/Silver/Gold) with batch processing.

## Architecture Diagram

````



````

## Technology Choices

| Component | Technology | Justification |
|-----------|------------|---------------|
| Orchestration | Apache Airflow | Industry standard, Python-native, rich UI |
| Processing | Apache Spark | Scalable, handles large data, rich API |
| Lake Storage | MinIO (S3-compatible) | S3 API compatible, free, runs locally |
| Warehouse | PostgreSQL | Reliable, SQL standard, Power BI compatible |
| BI Tool | Power BI | Company standard, rich visualizations |
| Containerization | Docker | Reproducible, portable, industry standard |

## Data Flow

1. **Extract (2:00 AM)**
   - Airflow triggers ingestion DAG
   - Spark jobs read from sources
   - Raw data written to Bronze layer

2. **Transform (3:00 AM)**
   - Spark jobs read Bronze
   - Apply cleaning, validation, deduplication
   - Write to Silver layer

3. **Model (4:00 AM)**
   - Spark jobs read Silver
   - Build dimensional model
   - Apply SCD Type 2 for customers
   - Write to Gold layer

4. **Load (5:00 AM)**
   - Export Gold to PostgreSQL
   - Refresh Power BI datasets

5. **Serve (6:00 AM onwards)**
   - Business users access Power BI dashboards
   - Reports reflect previous day's data

## Scalability Considerations

- **Horizontal Scaling**: Add Spark workers for more processing power
- **Partitioning**: Data partitioned by date for efficient queries
- **Incremental Processing**: Only process new/changed data
- **Caching**: Frequently accessed dimensions cached in Spark