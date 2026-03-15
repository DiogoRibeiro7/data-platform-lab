-- Load event data matching data/sample/events.json.
--
-- Includes raw data issues:
--   evt-014 has NULL user_id
--   evt-005 is duplicated

INSERT INTO events (event_id, type, user_id, page, product_id, quantity, order_id, timestamp) VALUES
    ('evt-001', 'page_view',        'C001', '/products/P001',          NULL,  NULL, NULL,      '2024-06-01T10:23:45Z'),
    ('evt-002', 'page_view',        'C001', '/products/P002',          NULL,  NULL, NULL,      '2024-06-01T10:24:12Z'),
    ('evt-003', 'add_to_cart',      'C001', NULL,                      'P001', 2,   NULL,      '2024-06-01T10:25:00Z'),
    ('evt-004', 'checkout',         'C001', NULL,                      NULL,  NULL, 'ORD-001', '2024-06-01T10:30:00Z'),
    ('evt-005', 'page_view',        'C002', '/products/P002',          NULL,  NULL, NULL,      '2024-06-03T14:00:00Z'),
    ('evt-006', 'add_to_cart',      'C002', NULL,                      'P002', 1,   NULL,      '2024-06-03T14:05:30Z'),
    ('evt-007', 'checkout',         'C002', NULL,                      NULL,  NULL, 'ORD-002', '2024-06-03T14:10:00Z'),
    ('evt-008', 'page_view',        'C003', '/categories/electronics', NULL,  NULL, NULL,      '2024-06-05T09:00:00Z'),
    ('evt-009', 'page_view',        'C003', '/products/P002',          NULL,  NULL, NULL,      '2024-06-05T09:02:15Z'),
    ('evt-010', 'page_view',        'C003', '/products/P003',          NULL,  NULL, NULL,      '2024-06-05T09:03:45Z'),
    ('evt-011', 'add_to_cart',      'C003', NULL,                      'P002', 1,   NULL,      '2024-06-05T09:05:00Z'),
    ('evt-012', 'add_to_cart',      'C003', NULL,                      'P003', 1,   NULL,      '2024-06-05T09:05:30Z'),
    ('evt-013', 'checkout',         'C003', NULL,                      NULL,  NULL, 'ORD-003', '2024-06-05T09:10:00Z'),
    ('evt-014', 'page_view',        NULL,   '/',                       NULL,  NULL, NULL,      '2024-06-06T12:00:00Z'),
    ('evt-015', 'page_view',        'C005', '/products/P001',          NULL,  NULL, NULL,      '2024-06-10T16:30:00Z'),
    ('evt-016', 'checkout',         'C005', NULL,                      NULL,  NULL, 'ORD-004', '2024-06-10T16:45:00Z'),
    ('evt-005', 'page_view',        'C002', '/products/P002',          NULL,  NULL, NULL,      '2024-06-03T14:00:00Z'),
    ('evt-017', 'page_view',        'C006', '/products/P003',          NULL,  NULL, NULL,      '2024-06-20T11:00:00Z'),
    ('evt-018', 'add_to_cart',      'C006', NULL,                      'P003', 1,   NULL,      '2024-06-20T11:02:00Z'),
    ('evt-019', 'remove_from_cart', 'C006', NULL,                      'P003', NULL, NULL,     '2024-06-20T11:10:00Z');
