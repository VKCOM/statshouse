---
sidebar_position: 9
title: Migrate
---

# Migrate

Migrate to StatsHouse from the popular monitoring tools such as Grafana and Prometheus.

Learn more about the StatsHouse [features](../overview/features.md).

## How to migrate from Grafana

Initially, StatsHouse provided users with the 
[Grafana data source plugin](https://github.com/VKCOM/statshouse/tree/master/grafana-plugin-ui). We discontinued the 
plugin development—we put our effort to developing StatsHouse visualizing system.

To help our users migrate, we focus on implementing widgets, thresholds, improved table view, 
support for several metrics on a graph, and more.

To request a feature or to discuss the roadmap, please refer to the [support](../support.md) section.

## How to migrate from Prometheus

Prometheus and StatsHouse differ so much:

| System     | Data model                                                                    | How it works                                                                                                                | Push/pull |
|------------|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|-----------|
| Prometheus | stores data as [time series](https://prometheus.io/docs/concepts/data_model/) | collects metrics from targets by [scraping](https://prometheus.io/docs/prometheus/latest/getting_started/) metrics endpoints | pull      |
| StatsHouse | stores data as [aggregates](../overview/concepts.md#aggregate)   | monitored applications push metric data to the StatsHouse agents,<br/>which aggregate it and transfer it further                 | push      |

How can we bring these two systems together? StatsHouse offers a proof of concept for scraping metrics in a 
Prometheus-like manner.

:::info
The user interface for the experimental scraping feature is under development.
:::

See the top-level description below.

### _Push_ and _pull_: how to combine

To bring the _"pushing"_ StatsHouse with the _"pulling"_ Prometheus together, we implemented the following 
mechanism:

> Install a StatsHouse agent on each machine you want to get metrics from, and the agent will scrape metric 
> data from the corresponding machine using the 
> [Prometheus configuration file](https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

### Configuration file

A configuration file for scraping metrics in StatsHouse is similar to the common 
[Prometheus configuration file](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)—with
the additional fields, such as the [namespace](#scraping-into-the-particular-namespace) to locate the scraped 
metrics, and some others.

### Scraping into the particular namespace

StatsHouse scrapes metrics only into the [explicitly specified](#configuration-file) namespace.
StatsHouse never knows the amount of data it can get upon scraping the metrics, so it has to control the budget.

If you try to scrape metrics to a default namespace, you'll get the `err_metric_not_found` error (see the 
`__src_ingestion_status` metric and the `status` tag).

### How to create metrics (when scraping)

If you specify the namespace in a configuration file, StatsHouse creates the metrics _automatically_ for 
this namespace. If you try to scrape metrics to a default namespace, the metrics will not be created (you will get 
the `err_metric_not_found` error).

### How to create tags (when scraping)

For the scraped metrics, tags are extracted _automatically_. There may be more than 16 tags in
each scraped metric, and StatsHouse does not "know" how to map them to the tag IDs.

These extracted tags appear as the draft tags in the [Edit](../guides/edit-metrics.md) section for the metric.
Learn how to manually 
[map the draft tag names to the tag IDs](../guides/edit-metrics.md#map-the-draft-tag-names-to-the-tag-ids).

### Metric formats: how to map

The basic [Prometheus metric types](https://prometheus.io/docs/concepts/metric_types/) and 
[StatsHouse metric types](../guides/design-metric.md#metric-types) differ as well. For migrating purposes, we 
mapped them so:

| Prometheus                                                                              | StatsHouse                                                                          |
|-----------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| counter →<br/>([cumulative](https://prometheus.io/docs/concepts/metric_types/#counter))  | counter<br/>([an aggregate per time interval](../guides/design-metric.md#counters)) |
| gauge                                                                                 → | value                                                                               |
| histogram                                                                             → | histogram (_experimental_)                                                          |

Further details about scraping Prometheus metrics will be provided later.
