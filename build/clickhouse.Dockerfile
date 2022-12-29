FROM clickhouse/clickhouse-server:22.11
COPY build/clickhouse.sql /docker-entrypoint-initdb.d/
