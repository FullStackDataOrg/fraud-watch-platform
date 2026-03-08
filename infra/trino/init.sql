CREATE SCHEMA IF NOT EXISTS delta.bronze
WITH (location = 's3a://fraud-platform/bronze');

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name  => 'transactions',
    table_location => 's3a://fraud-platform/bronze/transactions'
);
