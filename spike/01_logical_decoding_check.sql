\echo '=== A. server settings ==='
SELECT name, setting FROM pg_settings
WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders')
ORDER BY name;

\echo '=== B. publication ==='
SELECT pubname, puballtables FROM pg_publication;
SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'payments_pub' ORDER BY tablename;

\echo '=== C. create slot ==='
SELECT slot_name, lsn FROM pg_create_logical_replication_slot('spike_slot', 'pgoutput');

SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot';

\echo '=== D. generate changes ==='
INSERT INTO customers (full_name, email, cpf)
VALUES ('Ana Souza', 'ana@example.invalid', '00000000191');

INSERT INTO merchants (legal_name, category, country)
VALUES ('Loja Exemplo LTDA', 'retail', 'BR');

INSERT INTO payments (customer_id, merchant_id, amount, currency, status)
VALUES (1, 1, 199.90, 'BRL', 'pending');

UPDATE payments SET status = 'captured', updated_at = now() WHERE payment_id = 1;

DELETE FROM payments WHERE payment_id = 1;

\echo '=== E. peek (first read) ==='
SELECT lsn,
       xid,
       length(data) AS bytes,
       chr(get_byte(data, 0)) AS msg,
       encode(substring(data from 1 for 20), 'hex') AS head
FROM pg_logical_slot_peek_binary_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
);

\echo '=== F. peek is non-destructive: same count on second read ==='
SELECT count(*) AS rows_on_second_peek
FROM pg_logical_slot_peek_binary_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
);

SELECT slot_name, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot';

\echo '=== G. capture position before consuming ==='
SELECT confirmed_flush_lsn AS before_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot' \gset

\echo '=== H. get_changes consumes and advances ==='
SELECT count(*) AS rows_consumed
FROM pg_logical_slot_get_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
);

SELECT count(*) AS rows_left_after_consume
FROM pg_logical_slot_peek_binary_changes(
    'spike_slot', NULL, NULL,
    'proto_version', '1',
    'publication_names', 'payments_pub'
);

SELECT slot_name, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot';

\echo '=== I. can the slot rewind? target is the position captured in G ==='
\echo 'target:'
SELECT :'before_lsn'::pg_lsn AS rewind_target;

SELECT * FROM pg_replication_slot_advance('spike_slot', :'before_lsn'::pg_lsn);

SELECT slot_name, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'spike_slot';
