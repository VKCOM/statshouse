FROM clickhouse/clickhouse-server:22.11
COPY build/clickhouse.sql /docker-entrypoint-initdb.d/
COPY build/clickhouse-config.xml /etc/clickhouse-server/config.d/
