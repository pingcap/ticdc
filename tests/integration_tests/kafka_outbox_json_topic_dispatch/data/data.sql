-- @prefix is set to a per-run unique value by run.sh (via sed substitution of
-- the __PREFIX__ token below) so that the Kafka topics created by this test
-- are unique and cannot collide with topics left over from a previous run.
SET @prefix = '__PREFIX__';

-- Three rows for @prefix+tenant-a. The event_key is the order ID so that
-- events for the same order land on the same partition. Orders 10011 and 10012
-- have two and one lifecycle events.
INSERT INTO outbox_test.tenant_events (id, tenant_id, event_key, payload) VALUES
    ('7c9e6679-7425-40de-944b-e07fc1f90ae7', CONCAT(@prefix, 'tenant-a'), 'order-10011',
        '{"order_id": 10011, "status": "order.created",   "user_id": 7201}'),
    ('7c9e6679-7425-40de-944b-e07fc1f90ae8', CONCAT(@prefix, 'tenant-a'), 'order-10011',
        '{"order_id": 10011, "status": "order.shipped",   "user_id": 7201}'),
    ('7c9e6679-7425-40de-944b-e07fc1f90ae9', CONCAT(@prefix, 'tenant-a'), 'order-10012',
        '{"order_id": 10012, "status": "order.created",   "user_id": 9034}');

-- Three rows for @prefix+tenant-b.
INSERT INTO outbox_test.tenant_events (id, tenant_id, event_key, payload) VALUES
    ('6ba7b810-9dad-11d1-80b4-00c04fd430c1', CONCAT(@prefix, 'tenant-b'), 'order-20001',
        '{"order_id": 20001, "status": "order.created",   "user_id": 4451}'),
    ('6ba7b810-9dad-11d1-80b4-00c04fd430c2', CONCAT(@prefix, 'tenant-b'), 'order-20001',
        '{"order_id": 20001, "status": "order.confirmed", "user_id": 4451}'),
    ('6ba7b810-9dad-11d1-80b4-00c04fd430c3', CONCAT(@prefix, 'tenant-b'), 'order-20002',
        '{"order_id": 20002, "status": "order.created",   "user_id": 6670}');

-- One row for @prefix+tenant-c.
INSERT INTO outbox_test.tenant_events (id, tenant_id, event_key, payload) VALUES
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', CONCAT(@prefix, 'tenant-c'), 'order-30001',
        '{"order_id": 30001, "status": "order.created",   "user_id": 5523}');
