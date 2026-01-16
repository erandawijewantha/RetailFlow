# ADR-001: Use Medallion Architecture

## Status
Accepted

## Date
[Today's Date]

## Context
We need to organize our data lake to handle:
- Raw data ingestion from multiple sources
- Data cleaning and transformation
- Business-ready analytical datasets

We need clear separation between data quality levels to:
- Debug issues easily
- Enable different teams to work independently
- Support reprocessing if needed

## Decision
Implement the Medallion Architecture with three layers:

### Bronze Layer
- Raw data exactly as received from sources
- No transformations applied
- Partitioned by ingestion date
- Enables full reprocessing if needed

### Silver Layer
- Cleaned and validated data
- Standard data types applied
- Duplicates removed
- Business rules validated

### Gold Layer
- Business-level aggregations
- Dimensional model (star schema)
- Optimized for BI queries
- Pre-calculated metrics

## Options Considered

### Option 1: Medallion Architecture (Bronze/Silver/Gold) ✓
**Pros:**
- Clear separation of concerns
- Industry standard (Databricks, Microsoft)
- Easy debugging
- Supports reprocessing

**Cons:**
- 3x storage (acceptable)
- Processing overhead (minimal)

### Option 2: Two-Layer (Raw/Processed)
**Pros:**
- Simpler
- Less storage

**Cons:**
- Harder to debug
- No intermediate validation point

### Option 3: Direct to Warehouse
**Pros:**
- Fastest path to BI

**Cons:**
- No raw data backup
- Very hard to debug
- Can't reprocess

## Consequences

### Positive
- Clear data lineage
- Easy to identify where issues occur
- Teams can own different layers
- Can reprocess any layer independently

### Negative
- Need ~3x storage capacity
- Slightly longer processing time
- More jobs to monitor

## Implementation Notes
- Bronze: Parquet format, partitioned by `ingestion_date`
- Silver: Parquet format, partitioned by `processing_date`
- Gold: Parquet format, partitioned by business key (e.g., `date_key`)