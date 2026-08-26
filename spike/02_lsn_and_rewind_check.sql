\echo '=== A. fresh slot ==='
SELECT pg_drop_replication_slot('spike_slot');
SELECT slot_name, lsn FROM pg_create_logical_replication_slot('spike_slot', 'pgoutput');

\echo '=== B. one transaction, three updates on the same row ==='
INSERT INTO payments (customer_id, merchant_id, amount, currency, status)
VALUES (1, 1, 10.00, 'BRL', 'pending');

SELECT max(payment_id) AS pid FROM payments \gset

BEGIN;
UPDATE payments SET status = 'authorized', updated_at = now() WHERE payment_id = :pid;
UPDATE payments SET status = 'captured',   updated_at = now() WHERE payment_id = :pid;
UPDATE payments SET amount = 11.00,        updated_at = now() WHERE payment_id = :pid;
COMMIT;

\echo '=== C. does each change get its own LSN? ==='
SELECT lsn,
       xid,
       chr(get_byte(data, 0)) AS msg,
       length(data) AS bytes
FROM pg_logical_slot_peek_binary_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
);

\echo '=== D. distinct LSN count among the three updates ==='
SELECT count(*) AS update_messages,
       count(DISTINCT lsn) AS distinct_lsns
FROM pg_logical_slot_peek_binary_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
)
WHERE chr(get_byte(data, 0)) = 'U';

\echo '=== E. position before consuming ==='
SELECT confirmed_flush_lsn AS before_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot' \gset
SELECT :'before_lsn' AS before_lsn;

\echo '=== F. consume ==='
SELECT count(*) AS rows_consumed
FROM pg_logical_slot_get_binary_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
);

SELECT slot_name, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot';

\echo '=== G. attempt to rewind the slot to the position captured in E ==='
SELECT * FROM pg_replication_slot_advance('spike_slot', :'before_lsn'::pg_lsn);

SELECT slot_name, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot';

\echo '=== H. did anything come back after the rewind attempt? ==='
SELECT count(*) AS rows_after_rewind_attempt
FROM pg_logical_slot_peek_binary_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
);
