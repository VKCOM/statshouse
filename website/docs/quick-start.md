---
sidebar_position: 3
---

import CreateMetric from './img/create-metric.png'
import MyMetric from './img/quick-start-name-metric.png'
import ViewMetric from './img/quick-start-view.png'
import BasicViewOptions from './img/basic-viewing-options.png'
import TimePeriod from './img/time-period.png'
import TimeAgo from './img/time-ago.png'
import AggregationInterval from './img/aggregation-interval.png'
import TopN from './img/top-n.png'

# Quick start

See how StatsHouse works in two minutes:

<!-- TOC -->
* [Get access to StatsHouse](#get-access-to-statshouse)
* [Create a metric](#create-a-metric)
* [Send metric data](#send-metric-data)
* [View your metric on a graph](#view-your-metric-on-a-graph)
<!-- TOC -->

Then spend ten minutes more and [check basic viewing options](#check-basic-viewing-options).

## Get access to StatsHouse

Visit StatsHouse deployed in your organization, or [run StatsHouse locally](#how-to-run-statshouse-locally).

### How to run StatsHouse locally

:::important
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

## View your metric on a graph

View the metric on the StatsHouse dashboard at 
[localhost:10888](http://localhost:10888/view?live=1&f=-300&t=0&tn=-1&s=example_response_time&t1.s=example_response_time&t1.qw=avg&t2.s=example_runtime_memory&t2.qw=avg&t2.qb=key1).

In this example, we sent the same metric data three times:

<img src={ViewMetric} width="900"/>

## Check basic viewing options

Find the basic viewing options on a picture and their descriptions below:

<img src={BasicViewOptions} width="900"/>

### Metric name

Choose the name of your previously created metric or refer to someone else's one. 
Learn how to [refer to existing metrics](guides/view-graph.md#refer-to-existing-metrics) 
and check the related warnings.
Check [how to find the metrics author](guides/view-graph.md#how-to-find-the-metrics-author).

Read more about [choosing a metric by its name](guides/view-graph.md#metric-name).

### Descriptive statistics

These are statistical functions that quantitatively describe or summarize metric data. Here you choose if you 
want to show them on a graph or not. The most common are:
* _count_ and _count/sec_,
* _sum_ and _sum/sec_,
* _average_,
* _minimum_ and _maximum_,
* _standard deviation_,
* _percentiles_,
and [more](guides/view-graph.md#descriptive-statistics).

The range of descriptive statistics that are meaningful for a metric is 
[related to a metric type](guides/send-data.md#metric-type).
For example, _percentiles_ are available for _values_ only.

In this dropdown menu, you can see statistics, which may be not relevant for your metric type. If you pick them, you 
will see _0_ values for them on a graph. To switch off showing irrelevant statistics in this dropdown menu, 
[specify the metric type in the UI](guides/edit-metrics.md#aggregation).

:::tip
If you choose to show _count_ or _sum_ as a descriptive statistic, 
while an [aggregation interval](#aggregation-interval) is set to _Auto_, the resulting graph may look difficult to 
grasp.

Instead, choose the _count/sec_ and _sum/sec_ statistics.
These are normalized data, which are independent of an aggregation interval.
:::

Read more about [descriptive statistics](guides/view-graph.md#descriptive-statistics).

### Time periods

Display data for a specific time period: the last five minutes, last hour, last week, 
and more—even for the last two years.

Choose a particular date and particular time. And you can combine the controls:
for example, you can display the last-hour data for the previous day:

<img src={TimePeriod} width="500"/>

Compare the data for a chosen time period with the data for the same period in the past: a day, a 
week, a year ago.

<img src={TimeAgo} width="300"/>

When choosing time periods, please be aware of a chosen [aggregation interval](#aggregation-interval).

:::tip
Make sure the chosen time period is larger than the aggregation interval. 
For example, choose a 7-day time period and a 24-hour aggregation interval.
:::

For real-time monitoring, use the [Live mode](guides/view-graph.md#live-mode).

Read more about choosing [time periods](guides/view-graph.md#time-periods).

### Aggregation interval

The larger aggregation interval you choose, the smoother look your graph has:

<img src={AggregationInterval} width="600"/>

:::tip
An _Auto_ interval uses the _minimal available interval_ for aggregation.
This interval _varies_ depending on the currently available aggregation: 
* per-second aggregated data is stored for the first three days, 
* per-minute aggregated data is stored for a month, 
* per-hour aggregated data is available forever.

The currently available aggregation is also related to 
a metric [resolution](guides/edit-metrics.md#resolution).
:::

Read more about [aggregation](conceptual-overview.md#aggregation) in StatsHouse, [aggregation intervals](guides/view-graph.md#aggregation-interval),
and changing metric [resolution](guides/edit-metrics.md#resolution).

### Tags

Tags help to differentiate the characteristics of what you measure, the contributing factors, or a context. 
Filter or group metric data by available tags.

A particular piece of data can be labeled as related to 
* an environment: `production`, `development`, or `staging`; 
* parts of your application: `video`, `stories`, `feed`, etc.;
* a platform: `mobile`, `web`, etc., 
* a group of methods, a version, or something else.

:::tip
Tags with many different values such as user IDs or email addresses
may lead to [mapping flood](guides/view-graph.md#mapping-status) errors 
or increased [sampling](guides/view-graph.md#sampling). If you need to create such a tag, read more about [user ID as a 
tag](guides/edit-metrics.md#user-id-as-a-tag).
:::

Read more about [filtering with tags](guides/view-graph.md#tags), [setting up](guides/send-data.md#tags) 
and [editing](guides/edit-metrics.md#tags) tags, and [cardinality](conceptual-overview.md#cardinality).

### Top N

When grouping data by one or more tags, e.g., by `environment` and `platform`, 
you may find that there are a lot of tag value combinations:

| Environment × Platform | `web` | `iphone` | `android` |
|:----------------------:|:-----:|:--------:|:--------:|
|      `production`      |  ✔️   |    ✔️    |    ✔️    |
|       `staging`        |  ✔️   |    ✔️    |    ✔️    |
|       `testing`        |  ✔️   |    ✔️    |    ✔️    |

If there are too many of them, use the _Top N_ option to choose the number of combinations 
with the highest values to show on a graph.

So, if you choose _Top 3_, you will get, for example:

<img src={TopN} width="900"/>

:::note
To try out full StatsHouse features such as [editing metrics](guides/edit-metrics.md), 
[querying with PromQL](guides/query-wth-promql.md), [creating dashboards](guides/dashboards.md), and more,
refer to the [How-to guides](/category/how-to-guides).
:::
