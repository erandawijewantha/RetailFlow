-- ============================================================================
-- RETAILFLOW WAREHOUSE INITIALIZATION
-- ============================================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA silver TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA gold TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA staging TO warehouse;

-- ============================================================================
-- GOLD LAYER TABLES (Star Schema)
-- ============================================================================

-- Dimension: Date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key            INT PRIMARY KEY,
    full_date           DATE NOT NULL,
    day_of_week         INT NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    day_of_month        INT NOT NULL,
    day_of_year         INT NOT NULL,
    week_of_year        INT NOT NULL,
    month_number        INT NOT NULL,
    month_name          VARCHAR(20) NOT NULL
    quarter             INT NOT NULL,
    year                INT NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_holiday          BOOLEAN DEFAULT FALSE,
    fiscal_quarter      INT NOT NULL,
    fiscal_year         INT NOT NULL
);

-- Dimension: Customer (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         VARCHAR(50) NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    segment             VARCHAR(50),
    city                VARCHAR(100),
    country             VARCHAR(100),
    registration_date   DATE,
    effective_date      DATE NOT NULL,
    expiry_date DATE NOT NULL DEFAULT '2099-12-31',
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    version             INT NOT NULL DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_natural ON gold.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON gold.dim_customer(is_current);


-- Dimensions: Product
CREATE TABLE IF NOT EXISTS gold.dim_product(
    product_key          SERIAL PRIMARY KEY,
    product_id          VARCHAR(50) NOT NULL UNIQUE,
    product_name        VARCHAR(255) NOT NULL,
    brand               VARCHAR(100),
    category_11         VARCHAR(100),
    category_12         VARCHAR(100),
    category_13         VARCHAR(100),
    list_price          DECIMAL(10, 2),
    cost_price          DECIMAL(10, 2),
    is_active           BOOLEAN DEFAULT TRUE,
    launch_date         DATE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS gold.fact_sales(
    sale_key            BIGSERIAL PRIMARY KEY,
    date_key            INT NOT NULL REFERENCES gold.dim_date(date_key),
    customer_key        INT NOT NULL REFERENCES gold.dim_customer(customer_key),
    product_key         INT NOT NULL REFERENCES gold.dim_product(product_key),
    order_id            VARCHAR(50) NOT NULL,
    quantity            INT NOT NULL,
    unit_price          DECIMAL(10,2) NOT NULL,
    discount_amount     DECIMAL(10,2) DEFAULT 0,
    gross_amount        DECIMAL(12,2) NOT NULL,
    net_amount          DECIMAL(12,2) NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id            VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON gold.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON gold.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON gold.fact_sales(product_key);

-- ============================================================================
-- POPULATE DATE DIMENSION
-- ============================================================================

-- Generate dates from 2020-01-01 to 2030-12-31
INSERT INTO gold.dim_date
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_key,
    datum AS full_date,
    EXTRACT(ISODOW FROM datum)::INT AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(DAY FROM datum)::INT AS day_of_month,
    EXTRACT(DOY FROM datum)::INT AS day_of_year,
    EXTRACT(WEEK FROM datum)::INT AS week_of_year,
    EXTRACT(MONTH FROM datum)::INT AS month_number,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(QUARTER FROM datum)::INT AS quarter,
    EXTRACT(YEAR FROM datum)::INT AS year,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    EXTRACT(QUARTER FROM datum)::INT AS fiscal_quarter,
    EXTRACT(YEAR FROM datum)::INT AS fiscal_year
FROM (
    SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
    FROM GENERATE_SERIES(0, 4017) AS SEQUENCE(DAY)
) DQ
ON CONFLICT (date_key) DO NOTHING;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Verify tables created
DO $$
BEGIN
    RAISE NOTICE 'RetailFlow warehouse initialized successfully!';
    RAISE NOTICE 'Schemas created: bronze, silver, gold, staging';
    RAISE NOTICE 'Date dimension populated with % rows', (SELECT COUNT(*) FROM gold.dim_date);
END $$;
