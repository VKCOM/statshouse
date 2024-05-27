FROM clickhouse/clickhouse-server:22.11
WORKDIR /var/lib/statshouse/cache/aggregator
WORKDIR /var/lib/statshouse/cache/agent
WORKDIR /var/lib/statshouse/cache/api
WORKDIR /var/lib/statshouse/metadata/binlog
WORKDIR /var/lib/statshouse
WORKDIR /usr/lib/statshouse-api
COPY --from=localrun-statshouse-bin /bin/statshouse /bin/
COPY --from=localrun-statshouse-bin /bin/statshouse-api /bin/
COPY --from=localrun-statshouse-bin /bin/statshouse-metadata /bin/
COPY --from=localrun-statshouse-bin /bin/dlv /bin/
COPY --from=localrun-statshouse-ui /ui /usr/lib/statshouse-api/statshouse-ui
RUN ["/bin/statshouse-metadata", "--binlog-prefix=/var/lib/statshouse/metadata/binlog/bl", "--create-binlog=0,1"]
COPY build/clickhouse.sql /docker-entrypoint-initdb.d/
COPY build/clickhouse-config.xml /etc/clickhouse-server/config.d/
COPY localdebug/consul/entrypoint-aggregator.sh /bin/entrypoint.sh
ENTRYPOINT ["/bin/entrypoint.sh"]
