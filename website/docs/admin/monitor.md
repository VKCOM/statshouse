---
sidebar_position: 4
title: Monitor StatsHouse
---

import HeartbeatVersion from '../img/heartbeat-version.png'
import AggMetrics from '../img/agg-metrics.png'
import AggInsertMetrics from '../img/agg-insert-metrics.png'

# Monitor StatsHouse

To monitor StatsHouse, use the predefined service [metrics](#service-metrics). Additionally, find [logs](#logs).

## Service metrics

The `__heartbeat_version` metric shows the number of running components. Group the data by the `component` tag:

<img src={HeartbeatVersion} width="700"/>

:::tip
Read more about [checking the agent's version](maintain-upgrade.md).

Learn [how to monitor the agent's health](install.md#how-to-monitor-the-agents-health).
:::

To monitor the aggregator's metrics, use the `__agg` prefix:

<img src={AggMetrics} width="700"/>

To monitor the process of inserting data into the ClickHouse database, use the `__agg_insert` prefix:

<img src={AggInsertMetrics} width="150"/>

## Logs

By default, StatsHouse exports logs to `/dev/stdout`.
We recommend wrapping the process of running the agent into the `Systemd Unit` and managing logs via its `journal`
service.
