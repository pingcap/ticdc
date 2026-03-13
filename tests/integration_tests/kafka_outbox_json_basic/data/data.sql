-- Five inserts across three orders. The event_key is the order ID so that all
-- events for the same order are routed to the same Kafka partition, preserving
-- per-order event ordering. Orders 39877 and 39878 each have two lifecycle
-- events, demonstrating the partition co-location guarantee.
INSERT INTO outbox_test.events (id, event_key, payload) VALUES
    ('550e8400-e29b-41d4-a716-446655440001', 'order-39877',
        '{"order_id": 39877, "status": "order.created",    "user_id": 3412}'),
    ('550e8400-e29b-41d4-a716-446655440002', 'order-39877',
        '{"order_id": 39877, "status": "order.confirmed",  "user_id": 3412}'),
    ('550e8400-e29b-41d4-a716-446655440003', 'order-39878',
        '{"order_id": 39878, "status": "order.created",    "user_id": 8832}'),
    ('550e8400-e29b-41d4-a716-446655440004', 'order-39878',
        '{"order_id": 39878, "status": "order.shipped",    "user_id": 8832}'),
    ('550e8400-e29b-41d4-a716-446655440005', 'order-39879',
        '{"order_id": 39879, "status": "order.created",    "user_id": 1156}');

-- Updates: the outbox-json encoder must silently skip these (no messages produced).
UPDATE outbox_test.events SET status = 'processed' WHERE id IN (
    '550e8400-e29b-41d4-a716-446655440001',
    '550e8400-e29b-41d4-a716-446655440002'
);

-- Delete: the outbox-json encoder must silently skip this (no message produced).
DELETE FROM outbox_test.events WHERE id = '550e8400-e29b-41d4-a716-446655440003';
