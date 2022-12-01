FROM clickhouse/clickhouse-server:22.11
COPY docker/clickhouse.sql /docker-entrypoint-initdb.d/
