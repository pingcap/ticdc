-- Two order lifecycle events for the same order (event_key = 'order-39877'),
-- so both land on the same Kafka partition. Each row carries a distinct W3C
-- TraceContext (traceparent + tracestate), demonstrating per-message header
-- propagation for OTEL distributed tracing.
--
-- TraceContext values are from the W3C TraceContext spec examples:
-- https://www.w3.org/TR/trace-context/
INSERT INTO outbox_test.traced_events
    (id, event_key, payload, trace_parent, trace_state)
VALUES
    ('550e8400-e29b-41d4-a716-446655440001',
     'order-39877',
     '{"order_id": 39877, "status": "order.created",   "user_id": 3412}',
     '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
     'rojo=00f067aa0ba902b7,congo=BleGNlZWRzIHRo'),

    ('550e8400-e29b-41d4-a716-446655440002',
     'order-39877',
     '{"order_id": 39877, "status": "order.confirmed", "user_id": 3412}',
     '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
     'congo=BleGNlZWRzIHRo');
