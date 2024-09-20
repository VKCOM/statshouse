---
sidebar_position: 3
---

import DemoFlow from './img/demo-flow.png'
import CreateMetric from './img/create-metric.png'
import MyMetric from './img/quick-start-name-metric.png'
import ViewMetric from './img/quick-start-view.png'
import BasicViewOptions from './img/basic-viewing-options.png'
import TimePeriod from './img/time-period.png'
import TimeAgo from './img/time-ago.png'
import AggregationInterval from './img/aggregation-interval.png'
import TopN from './img/top-n.png'

# Quick start

See how StatsHouse works in ten minutes.

<img src={DemoFlow} width="900"/>

See the related sections below:
<!-- TOC -->
* [Get internal permissions](#get-internal-permissions)
* [Run StatsHouse locally](#run-statshouse-locally)
* [Send metrics from a demo web server](#send-metrics-from-a-demo-web-server)
* [Create your metric](#create-your-metric)
* [Send data to your metric](#send-data-to-your-metric)
* [Check basic viewing options](#check-basic-viewing-options)
<!-- TOC -->

:::important
This tutorial is for Linux systems.
For macOS or Windows, there may be Docker-related issues.
:::

For detailed instructions on how to create, send, and view metrics, please refer
to the [user guide](introduction.md#user-guide).

## Get internal permissions

If you have StatsHouse deployed in your organization, please contact your administrators to get the necessary access.

## Run StatsHouse locally

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

## Send metrics from a demo web server

From the StatsHouse directory, run a [simple instrumented Go
web server](https://github.com/VKCOM/statshouse/blob/master/cmd/statshouse-example/statshouse-example.go)—it will send metrics to your local StatsHouse instance:
 ```shell
 go run ./cmd/statshouse-example/statshouse-example.go
 ```

## Create your metric

Go to the main **⚡** menu in the upper-left corner and select **Create metric**:

<img src={CreateMetric} width="300"/>

Name your metric:

<img src={MyMetric} width="600"/>

Read more about [creating metrics](guides/create-metric.md).

## Send data to your metric

For this toy example, use a simple `bash` script:
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```

Read more about [metrics in StatsHouse](guides/design-metric.md#what-are-metrics-in-statshouse) and 
[sending metric data](guides/send-data.md).

View the metric on the StatsHouse dashboard at 
[localhost:10888](http://localhost:10888/view?live=1&f=-300&t=0&tn=-1&s=example_response_time&t1.s=example_response_time&t1.qw=avg&t2.s=example_runtime_memory&t2.qw=avg&t2.qb=key1).

In this example, we sent the same metric data three times:

<img src={ViewMetric} width="900"/>

Read more about [viewing metrics on a graph](guides/view-graph.md).

## Check basic viewing options

Find the basic viewing options on a picture and their descriptions below:

<img src={BasicViewOptions} width="900"/>

### Metric name

Choose the name of your previously created metric or refer to someone else's one. 
Learn how to [refer to existing metrics](guides/view-graph.md#1--metric-name) 
and check the related warnings.

### Descriptive statistics

They are statistical functions that quantitatively describe or summarize metric data. Choose if you 
want to show them on a graph. The most common ones are:
* _count_ and _count/sec_,
* _sum_ and _sum/sec_,
* _average_,
* _standard deviation_,
* _minimum_ and _maximum_.

There are [more](guides/view-graph.md#3--descriptive-statistics) statistics available.
The range of descriptive statistics that are meaningful for a metric is 
[related to a metric type](guides/design-metric.md#metric-types).
For example, _percentiles_ are available for _values_ only.

In this dropdown menu, you can see statistics, which may be not relevant for your metric type. If you pick them, you 
will see 0 values for them on a graph. To switch off showing irrelevant statistics in this dropdown menu, 
[specify the metric type in the UI](guides/edit-metrics.md#aggregation).

:::tip
If you choose to show _count_ or _sum_ as a descriptive statistic, 
while an [aggregation interval](#aggregation-interval) is set to _Auto_, the resulting graph may look difficult to 
grasp.

Instead, choose the _count/sec_ and _sum/sec_ statistics.
These are normalized data, which are independent of an aggregation interval.
:::

Read more about [descriptive statistics](guides/view-graph.md#3--descriptive-statistics).

### Time period

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

For real-time monitoring, use [Live mode](guides/view-graph.md#5--live-mode).

### Aggregation interval

[Aggregation](overview/concepts.md#aggregation) interval is a kind of resolution for your metric data.
The larger aggregation interval you choose, the smoother look your graph has:

<img src={AggregationInterval} width="600"/>

:::tip
An _Auto_ interval uses the 
[minimal available interval_ for aggregation](overview/concepts.md#minimal-available-aggregation-interval) to show data on a 
graph.
This interval _varies_ depending on the currently available aggregation: 
* per-second aggregated data is stored for the first two days, 
* per-minute aggregated data is stored for a month, 
* per-hour aggregated data is available forever.

The currently available aggregation is also related to 
a metric [resolution](overview/concepts.md#resolution).
:::

Read more about [aggregation](overview/concepts.md#aggregation) in StatsHouse, 
and changing metric [resolution](guides/edit-metrics.md#resolution).

### Tags

Tags help to differentiate the characteristics of what you measure, the contributing factors, or a context. 
Filter or group metric data by available tags.

For example, a particular piece of data can be labeled as related to 
* an environment: `production`, `development`, or `staging`; 
* parts of your application: `video`, `stories`, `feed`, etc.;
* a platform: `mobile`, `web`, etc., 
* a group of methods, a version, or something else.

:::tip
Tags with many different values such as user IDs may lead to [mapping flood](guides/view-graph.md#mapping-status) errors 
or increased [sampling](guides/view-graph.md#sampling) due to high [cardinality](overview/concepts.md#cardinality). 
If you need to create such a tag, read more about 
[tags with many different values](guides/design-metric.md#how-many-tag-values).
:::

Read more about [filtering with tags](guides/view-graph.md#7--tags), [setting up](guides/design-metric.md#tags) 
and [editing](guides/edit-metrics.md#tags) tags, and [cardinality](overview/concepts.md#cardinality).

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
To try out full StatsHouse features, refer to the [user guide](introduction.md#user-guide).
:::
