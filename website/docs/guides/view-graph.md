---
sidebar_position: 4
---

import Home from '../img/home.png'
import GraphTable from '../img/graph-table.png'
import HostServiceMetrics from '../img/host-service-metrics.png'
import RenameGraph from '../img/rename-graph.png'


# View metric data

Display data on a graph, in a table, or as a [CSV](#csv) file via the StatsHouse UI:

<img src={Home} width="1000"/>

For complicated scenarios, [query StatsHouse with PromQL](query-wth-promql.md).

StatsHouse does not support viewing data via third-party applications.

## Metric name

Choose a metric name to refer to existing metrics.

:::warning
Do not commit configuration changes or send data to someone else's metric as you can spoil the metric or the related data.
:::

Some popular host metrics are built-in.
Metrics having two underscores in the beginning are for StatsHouse internal use only.
You cannot edit them. See [Meta-metrics](#meta-metrics) for details.

<img src={HostServiceMetrics} width="600"/>

If you have StatsHouse deployed in your organization, you can find a set of metrics that are common for all the 
engines, services, microservices, proxies, etc. in this organization.

#### "How to find the metrics author?"

There is no mechanism for checking a metrics author in StatsHouse, but sometimes authors mention how to find them 
in the metric description section. Otherwise, use your organization's internal communication channels.

#### "How to display several metrics on a graph?"

[Query StatsHouse with PromQL](query-wth-promql.md) or [create dashboards](dashboards.md). 

## Graph name

You can edit the graph name so that the metric name remains the same.

<img src={RenameGraph} width="400"/>

## Descriptive statistics



## Time period

## Aggregation interval

## Custom metric resolution

## Tags

## Top N

## Max host

## Switch database

## View table or graph

<img src={GraphTable} width="9000"/>

## Event overlay

## CSV

## Meta-metrics

### Receive status

### Sampling

### Cardinality

### Mapping status

### Receive error



