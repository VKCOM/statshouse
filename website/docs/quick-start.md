---
sidebar_position: 3
---

import CreateMetric from './img/create-metric.png'
import MyMetric from './img/quick-start-name-metric.png'
import ViewMetric from './img/quick-start-view.png'
import BasicViewOptions from './img/basic-viewing-options.png'

# Quick start

See how StatsHouse works in two minutes:

<!-- TOC -->
* [Get access to StatsHouse](#get-access-to-statshouse)
* [Create a metric](#create-a-metric)
* [Send metric data](#send-metric-data)
* [View your metric on a dashboard](#view-your-metric-on-a-dashboard)
<!-- TOC -->

Then spend ten minutes more and understand [basic viewing and editing options](#basic-viewing-and-editing-options).

## Get access to StatsHouse

Visit a StatsHouse cluster deployed in your organization, or [run StatsHouse locally](#how-to-run-statshouse-locally).

### How to run StatsHouse locally

:::warning
Make sure you have [Docker](https://docs.docker.com/get-docker/) installed.
:::

Clone the StatsHouse repository, go to the StatsHouse directory, and run a local StatsHouse instance:
```shell
git clone https://github.com/VKCOM/statshouse
cd statshouse
./localrun.sh
```

The StatsHouse UI opens once it is ready.

## Create a metric

1. Go to the main **⚡** menu in the upper-left corner and select **Create metric**:

<img src={CreateMetric} width="300"/>

2. Name your metric:

<img src={MyMetric} width="600"/>

## Send metric data

For this toy example, use a simple `bash` script:
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```

## View your metric on a dashboard

View the metric on the StatsHouse dashboard at 
[localhost:10888](http://localhost:10888/view?live=1&f=-300&t=0&tn=-1&s=example_response_time&t1.s=example_response_time&t1.qw=avg&t2.s=example_runtime_memory&t2.qw=avg&t2.qb=key1).

In this example, we sent the same metric data three times:

<img src={ViewMetric} width="900"/>

## Basic viewing and editing options

For a pre-deployed StatsHouse cluster or your local one, for your own or someone else's metric,
basic options in the UI are the same: for viewing or editing metrics.

### Viewing options

Find the basic viewing options on a picture and their descriptions below:

<img src={BasicViewOptions} width="900"/>

#### Metric name

Choose the name of your previously created metric or refer to someone else's existing one.

#### Descriptive statistics

These are the statistical functions that quantitatively describe or summarize metric data. Here you choose to show 
them on a graph. The most 
common are:
* count and count/sec,
* sum and sum/sec,
* average,
* minimum and maximum,
* standard deviation,
and others.

The range of descriptive statistics available for a metric to view and analyze is related to a metric type.

#### Time interval

#### Metric resolution

#### String top

#### Tags

#### Event overlay



### Editing options


:::info
To try out full StatsHouse features such as [querying with PromQL](guides/query-wth-promql.md) 
or [creating dashboards](guides/dashboards.md), 
refer to the [How-to guides](/category/how-to-guides).
:::








