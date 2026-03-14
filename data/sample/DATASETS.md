# Sample Datasets

Reference documentation for all sample datasets in this directory. Each file is designed to be small, readable, and useful across multiple exercises. Data quality issues are intentional — they exist so that validation, transformation, and quality-check exercises have realistic problems to detect and handle.

## Dataset Overview

| File | Format | Rows | Primary Use |
| --- | --- | --- | --- |
| `customers.csv` | CSV | 13 | Ingestion, deduplication, casing normalization |
| `products.csv` | CSV | 12 | Ingestion, validation, join exercises |
| `orders.csv` | CSV | 15 | Ingestion, foreign key validation, date parsing |
| `order_items.csv` | CSV | 20 | Joins, aggregation, duplicate detection |
| `events.json` | JSONL | 20 | Event processing, sessionization, streaming |
| `logs.log` | Log | 26 | Log parsing, observability exercises |
| `old_snapshot.csv` | CSV | 7 | CDC — baseline snapshot |
| `new_snapshot.csv` | CSV | 8 | CDC — comparison snapshot |
| `bad_customers.csv` | CSV | 12 | Validation, data quality checks |
| `sensor_events.json` | JSONL | 16 | Streaming, anomaly detection, windowed aggregation |

## Dataset Details

### customers.csv

A clean-ish customer list with a few embedded issues.

**Fields:** `customer_id`, `first_name`, `last_name`, `email`, `city`, `country`, `created_at`

**Intentional issues:**
- Row C004 has a missing `email`.
- Row C003 appears twice (duplicate).
- Row C010 has `country` as `"portugal"` (lowercase).
- Row C012 has `country` as `"ITALY"` (uppercase).

**Exercises:** CSV ingestion, deduplication, casing standardization, bronze-to-silver promotion.

### products.csv

A product catalog with pricing and stock data.

**Fields:** `product_id`, `name`, `category`, `price`, `currency`, `stock`, `active`

**Intentional issues:**
- P009 has a negative price (`-15.00`).
- P011 has a missing `stock` value.
- P012 uses `USD` while all others use `EUR` (mixed currencies).
- P008 is inactive with zero stock.

**Exercises:** Validation, price/stock checks, filtering inactive products, currency normalization.

### orders.csv

Order records referencing customers and containing totals.

**Fields:** `order_id`, `customer_id`, `order_date`, `status`, `total`, `shipping_country`

**Intentional issues:**
- ORD-008 references `C099`, which does not exist in `customers.csv` (invalid foreign key).
- ORD-009 has a date formatted as `2024/07/01` instead of `2024-06-XX` (inconsistent date format).
- Includes multiple statuses: `completed`, `shipped`, `pending`, `cancelled`.

**Exercises:** Foreign key validation, date parsing, status filtering, joins with customers.

### order_items.csv

Line items for each order, referencing products.

**Fields:** `order_id`, `product_id`, `quantity`, `unit_price`, `line_total`

**Intentional issues:**
- ORD-003 has a `line_total` of `44.99` for a `unit_price` of `45.00` (arithmetic mismatch).
- ORD-006 has a duplicate line item for P006 (same order, same product, same quantity).

**Exercises:** Join with orders and products, aggregation, arithmetic validation, duplicate detection.

### events.json

User interaction events in JSON Lines format, modeling an e-commerce clickstream.

**Fields:** `event_id`, `type`, `user_id`, `page`/`product_id`/`order_id`, `timestamp`

**Intentional issues:**
- evt-014 has a `null` user_id (anonymous visitor).
- evt-005 appears twice (duplicate event).
- Event types vary: `page_view`, `add_to_cart`, `checkout`, `remove_from_cart`.

**Exercises:** Event processing, sessionization, funnel analysis, deduplication, streaming simulation.

### logs.log

Simulated pipeline execution logs in a semi-structured format.

**Format:** `YYYY-MM-DD HH:MM:SS LEVEL [component] message key=value ...`

**Content includes:**
- INFO, WARN, and ERROR level messages.
- Component tags: `ingestion`, `validation`, `storage`, `transform`, `orchestration`.
- Structured key-value pairs: `job_id`, `run_id`, `duration_ms`, `rows`, `status`.
- Retry sequences and timeout errors.

**Exercises:** Log parsing, structured extraction, observability exercises, error rate analysis.

### old_snapshot.csv / new_snapshot.csv

Two point-in-time snapshots of a customer table for change data capture comparison.

**Fields:** `customer_id`, `first_name`, `last_name`, `email`, `city`, `country`, `active`

**Changes between snapshots:**
- C001: `city` changed from `Lisbon` to `Porto` (update).
- C002: `email` changed (update).
- C004: present in old, absent in new (delete).
- C005: `active` changed from `true` to `false` (update).
- C008, C009: absent in old, present in new (inserts).

**Exercises:** CDC snapshot comparison, insert/update/delete detection, slowly changing dimensions.

### bad_customers.csv

A deliberately messy customer file designed to fail validation.

**Fields:** `customer_id`, `first_name`, `last_name`, `email`, `city`, `country`, `created_at`

**Intentional issues:**
- Row 1: missing `customer_id`.
- Row 2 (C101): missing `last_name`.
- Row 3 (C102): `email` is `"not-an-email"` (invalid format).
- Row 4 (C103): missing `city`.
- Row 5 (C104): missing `country`, date formatted as `15/05/2024` (DD/MM/YYYY).
- Row 6 (C105): date `2024-06-31` (June has 30 days — invalid date).
- Row 7 (C106): all-lowercase names, city, and country.
- Row 8: duplicate of C103.
- Row 9 (C107): missing `created_at`.
- Row 10 (C108): completely empty row (all fields blank).
- Row 11 (C109): date `2024-13-01` (month 13 — invalid date).
- Row 12 (C110): clean row (control — should pass validation).

**Exercises:** Validation framework, error reporting, dead-letter routing, data quality scoring.

### sensor_events.json

Simulated IoT sensor readings in JSON Lines format.

**Fields:** `sensor_id`, `type`, `value`, `unit`, `location`, `timestamp`

**Intentional issues:**
- sensor-01 at 08:15 reads `-40.0` celsius (anomalous spike).
- sensor-03 at 08:10 reads `91.7%` humidity (anomalous spike).
- sensor-01 at 08:20 has a duplicate reading (same sensor, timestamp, and value).
- sensor-02 at 08:10 has a `null` value (missing reading).
- sensor-05 reports temperature in `fahrenheit` while others use `celsius` (mixed units).

**Exercises:** Streaming simulation, windowed aggregation, anomaly detection, unit normalization, time-series analysis.
