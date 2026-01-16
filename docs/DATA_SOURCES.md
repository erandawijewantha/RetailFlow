# Data Source Inventory

## 1. Orders Data

**Source System:** E-Commerce Platform
**Format:** CSV
**Delivery:** SFTP daily at 00:30
**Volume:** ~100,000 records/day
**History:** 3 years available

### Sample Schema
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| order_id | string | Unique order ID | ORD-2024-001234 |
| customer_id | string | Customer reference | CUST-00123 |
| order_date | datetime | When order placed | 2024-01-15 14:32:00 |
| product_id | string | Product ordered | PROD-5678 |
| quantity | integer | Units ordered | 2 |
| unit_price | decimal | Price per unit | 49.99 |
| discount_pct | decimal | Discount applied | 0.10 |
| shipping_address | string | Delivery address | 123 Main St... |
| status | string | Order status | completed |

### Data Quality Notes
- ~2% of records have null customer_id (guest checkout)
- discount_pct sometimes stored as "10%" instead of 0.10
- Historical data before 2022 has different column names

---

## 2. Products Data

**Source System:** Product Database
**Format:** PostgreSQL
**Connection:** Direct DB connection
**Volume:** ~50,000 products
**Update Frequency:** Real-time

### Sample Schema
| Column | Type | Description |
|--------|------|-------------|
| product_id | varchar(50) | Primary key |
| name | varchar(255) | Product name |
| category_id | int | FK to categories |
| brand | varchar(100) | Brand name |
| list_price | decimal(10,2) | Retail price |
| cost_price | decimal(10,2) | Our cost |
| is_active | boolean | Currently selling |
| created_at | timestamp | When added |

### Data Quality Notes
- Some products have null category_id (uncategorized)
- Discontinued products remain with is_active=false

---

## 3. Customers Data

**Source System:** CRM System
**Format:** REST API (JSON)
**Authentication:** API Key
**Volume:** ~1,000,000 customers
**Rate Limit:** 1000 requests/minute

### API Endpoint