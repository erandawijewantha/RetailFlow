# ADR-002: Implement SCD Type 2 for Customer Dimension

## Status
Accepted

## Date
[Today's Date]

## Context
Business requirement: Track customer attribute changes over time.

Example: When a customer moves from Colombo to Kandy, we need to:
1. Keep historical record (they WERE in Colombo)
2. Track current state (they ARE in Kandy now)
3. Associate old orders with old address
4. Associate new orders with new address

## Decision
Implement Slowly Changing Dimension Type 2 for `dim_customer`.

### Schema Design
```sql
CREATE TABLE dim_customer (
    customer_key    SERIAL PRIMARY KEY,  -- Surrogate key
    customer_id     VARCHAR(50),         -- Natural/business key

    -- Attributes (tracked for changes)
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    segment         VARCHAR(50),
    city            VARCHAR(100),
    country         VARCHAR(100),

    -- SCD Type 2 columns
    effective_date  DATE NOT NULL,
    expiry_date     DATE NOT NULL,
    is_current      BOOLEAN NOT NULL,
    version         INT NOT NULL,

    -- Audit
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);