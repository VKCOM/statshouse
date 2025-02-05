FROM clickhouse/clickhouse-server:24.3.16
COPY build/clickhouse.sql /docker-entrypoint-initdb.d/
COPY build/clickhouse-config.xml /etc/clickhouse-server/config.d/
