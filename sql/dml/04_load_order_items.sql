-- Load order item data matching data/sample/order_items.csv.
--
-- Includes raw data issues:
--   ORD-003/P003 has line_total 44.99 but unit_price*quantity = 45.00
--   ORD-006/P006 appears twice (duplicate line item)

INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total) VALUES
    ('ORD-001', 'P001', 2, 29.99, 59.98),
    ('ORD-002', 'P002', 1, 89.50, 89.50),
    ('ORD-003', 'P002', 1, 89.50, 89.50),
    ('ORD-003', 'P003', 1, 45.00, 44.99),
    ('ORD-004', 'P001', 1, 29.99, 29.99),
    ('ORD-005', 'P004', 1, 349.99, 349.99),
    ('ORD-006', 'P006', 2, 4.50, 9.00),
    ('ORD-006', 'P007', 1, 12.00, 12.00),
    ('ORD-006', 'P006', 2, 4.50, 9.00),
    ('ORD-007', 'P003', 1, 45.00, 45.00),
    ('ORD-008', 'P005', 1, 79.90, 79.90),
    ('ORD-009', 'P008', 1, 55.00, 55.00),
    ('ORD-010', 'P010', 1, 8.99, 8.99),
    ('ORD-011', 'P002', 1, 89.50, 89.50),
    ('ORD-011', 'P006', 1, 4.50, 4.50),
    ('ORD-012', 'P006', 1, 4.50, 4.50),
    ('ORD-013', 'P012', 1, 35.00, 35.00),
    ('ORD-014', 'P002', 1, 89.50, 89.50),
    ('ORD-014', 'P012', 1, 35.00, 35.00),
    ('ORD-015', 'P001', 1, 29.99, 29.99);
