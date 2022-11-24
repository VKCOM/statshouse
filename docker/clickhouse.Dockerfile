FROM yandex/clickhouse-server:21.8.9
COPY docker/clickhouse.sql /docker-entrypoint-initdb.d/
