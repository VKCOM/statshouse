FROM localrun-statshouse-bin
WORKDIR /var/lib/statshouse/cache/agent
COPY --from=prom/node-exporter /bin/node_exporter /bin
COPY --from=hashicorp/consul:1.10.0 /bin/consul /bin
COPY localdebug/consul/entrypoint-agent.sh /bin/entrypoint.sh
ENTRYPOINT ["/bin/entrypoint.sh"]
