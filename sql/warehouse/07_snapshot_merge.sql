-- Snapshot merge pattern — apply CDC changes to a target table.
--
-- This simulates the result of the snapshot-diff tool (Exercise 05).
-- Given a target table and a set of changes (inserts, updates, deletes),
-- apply them in the correct order.
--
-- This example uses the customer dimension as the target.

-- -----------------------------------------------------------------------
-- Step 1: Create a staging table for incoming changes
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_customer_changes (
    customer_id  TEXT,
    first_name   TEXT,
    last_name    TEXT,
    email        TEXT,
    city         TEXT,
    country      TEXT,
    created_at   TEXT,
    change_type  TEXT    -- 'insert', 'update', 'delete'
);

-- Example changes (matching the CDC sample from Exercise 05):
INSERT INTO stg_customer_changes VALUES
    ('C001', 'Alice', 'Martins', 'alice.martins@example.com', 'Porto',  'Portugal', '2024-01-15', 'update'),
    ('C002', 'Bob',   'Silva',   'bob.silva@newdomain.com',   'Porto',  'Portugal', '2024-02-20', 'update'),
    ('C004', NULL,    NULL,      NULL,                        NULL,     NULL,       NULL,         'delete'),
    ('C005', 'Eva',   'Costa',   'eva.costa@example.com',     'Lisbon', 'Portugal', '2024-05-12', 'update'),
    ('C008', 'Hugo',  'Pereira', 'hugo.pereira@example.com',  'Berlin', 'Germany',  '2024-08-30', 'insert'),
    ('C009', 'Irene', 'Lopes',   'irene.lopes@example.com',   'Rome',   'Italy',    '2024-09-14', 'insert');

-- -----------------------------------------------------------------------
-- Step 2: Apply deletes
-- -----------------------------------------------------------------------

DELETE FROM dim_customer
WHERE customer_id IN (
    SELECT customer_id
    FROM stg_customer_changes
    WHERE change_type = 'delete'
);

-- -----------------------------------------------------------------------
-- Step 3: Apply updates (upsert with INSERT OR REPLACE)
-- -----------------------------------------------------------------------

INSERT OR REPLACE INTO dim_customer (customer_id, first_name, last_name, email, city, country, created_at)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    upper(substr(country, 1, 1)) || lower(substr(country, 2)),
    created_at
FROM stg_customer_changes
WHERE change_type IN ('insert', 'update');

-- -----------------------------------------------------------------------
-- Step 4: Clean up staging
-- -----------------------------------------------------------------------

DROP TABLE IF EXISTS stg_customer_changes;
