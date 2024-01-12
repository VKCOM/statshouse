---
sidebar_position: 4
---

import HomeLabeled from '../img/home-labeled.png'
import TableView from '../img/table-view.png'
import HostMetrics from '../img/host-metrics.png'
import ServiceMetrics from '../img/service-metrics.png'
import RenameGraph from '../img/rename-graph.png'
import TimePeriod from '../img/time-period.png'
import TimeAgo from '../img/time-ago.png'
import AggregationInterval from '../img/aggregation-interval.png'
import TopN from '../img/top-n.png'
import MaxHostEnable from '../img/max-host-enable.png'
import MaxHostResult from '../img/max-host-result.png'
import GroupByChoose from '../img/groupby-choose.png'
import GroupByGraph from '../img/groupby-graph.png'
import FilterByTag from '../img/filter-by-tag.png'
import Negate from '../img/negate.png'
import Sort from '../img/sort.png'
import Auto from '../img/auto.png'
import TableChoose from '../img/table-choose.png'


# View metric data

Display data on a graph, in a [table](#10--table-view), or as a [CSV](#csv) file via the StatsHouse UI.
For complicated scenarios, [query StatsHouse with PromQL](query-wth-promql.md).
StatsHouse does not support viewing data via third-party applications.

Refer to the picture below and the upper-right navigation bar to learn more about viewing options:

<img src={HomeLabeled} width="1000"/>

## 1 — Metric name

Choose a metric name to refer to existing metrics.

:::warning
Do not commit configuration changes or send data to someone else's metric as you can spoil the metric or the related data.
:::

Some popular host metrics are built-in, for example:

<img src={HostMetrics} width="300"/>

Metrics having two underscores in the beginning are for StatsHouse internal use only, for example:

<img src={ServiceMetrics} width="300"/>

You cannot edit them. See [Meta-metrics](#meta-metrics) for details.

If you have StatsHouse deployed in your organization, you can find a set of metrics that are common for all the 
engines, services, microservices, proxies, etc. in this organization.

#### "How to find the metrics author?"

There is no mechanism for checking a metrics author in StatsHouse, but sometimes authors mention how to find them 
in the metric description section. Otherwise, use your organization's internal communication channels.

#### "How to display several metrics on a graph?"

[Create a dashboard](dashboards.md) or [query StatsHouse with PromQL](query-wth-promql.md). To find relationships 
between the metrics or events, use [Event overlay](#11--event-overlay).

## 2 — Graph name

You can edit the graph name so that the metric name remains the same.

<img src={RenameGraph} width="400"/>

## 3 — Descriptive statistics

They are statistical functions that quantitatively describe or summarize metric data:
* _count_ and _count/sec_
* _sum_ and _sum/sec_
* _average_
* _stddev_ (standard deviation)
* _minimum_ and _maximum_
* _unique_ and _unique/sec_
* [cumulative functions](#cumulative-functions)
* [derivative functions](#derivative-functions)
* [percentiles](#percentiles)

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

#### Cumulative functions

Cumulative functions are not meaningful for _unique_ metrics.

#### Derivative functions

A derivative function shows the rate of change for the initial function, i.e., the difference between the two nearest 
function values.

#### Percentiles

Percentiles are available for _value_ metrics only.
To get them, [enable percentiles](edit-metrics.md#percentiles) in the UI.

Note that the amount of data increases for a metric with percentiles, so enabling them may lead to increased 
[sampling](../conceptual-overview.md#sampling). If it is important for you to have the lower sampling factor, keep an 
eye on your metric [cardinality](#cardinality) or choose custom [resolution](edit-metrics.md#resolution) 
for writing metric data.

## 4 — Time periods

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

For real-time monitoring, use [Live mode](guides/view-graph.md#live-mode).
 
## 5 — Live mode

Enable _Live mode_ to view data in real time.

By default, the metric URL does not contain the _Live mode_ parameter.

:::tip
To share the link to real-time metric data, add the `live=1` query string to the metric URL.
:::

## 6 — Aggregation interval

The larger aggregation interval you choose, the smoother look your graph has:

<img src={AggregationInterval} width="600"/>

### _Auto_ and _Auto (low)_

An _Auto_ interval uses the _minimal available interval_ for aggregation to show data on a graph.
This interval _varies_ depending on the currently available aggregation:
* per-second aggregated data is stored for the first three days,
* per-minute aggregated data is stored for a month,
* per-hour aggregated data is available forever.

The currently available aggregation is also related to
a metric [resolution](guides/edit-metrics.md#resolution).

The _Auto (low)_ aggregation interval reduces the displayed "resolution" by a constant making the graph look smoother 
even when you view data using the minimal available aggregation interval:

<img src={Auto} width="1000"/>

## 7 — Tags

Filter or group data on a graph using tags.

In the dropdown menu for each tag, choose the required tag values to show on a graph.

<img src={FilterByTag} width="400"/>

:::tip
[Hide](edit-metrics.md#tags) the unnecessary tags in the UI, e.g., if you have less than 16 tags for your metric.
:::

### Group by tags

Group data by a single tag or multiple tags.

If there are many tag values or their combinations, use the [Top N](#8--top-n) option to specify the number of groups 
shown.

<img src={GroupByChoose} width="400"/>

For example, see the data grouped by the tag `protocol` with the _Top 3_ tag values shown:

<img src={GroupByGraph} width="600"/>

:::note
The default UI behavior is to use no grouping. There is no way to set up default grouping for a metric. 
:::

### Sort alphabetically

Sort tag values in the dropdown menu alphabetically to get quicker access:

<img src={Sort} width="400"/>

### Negate next selection

Choose the tag value to _exclude_ data from graph:

<img src={Negate} width="400"/>

## 8 — Top N

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

## 9 — Max host

Enable the [Max host](send-data.md#host-name-as-a-tag) option to find the host 
that sends the maximum value for your metric:

<img src={MaxHostEnable} width="100"/>

View the list of all hosts sorted by the maximum value 
or copy the list to clipboard:

<img src={MaxHostResult} width="600"/>

## 10 — Table view

View the metric events in a table, for example:

<img src={TableView} width="900"/>

Choose the columns to show:

<img src={TableChoose} width="500"/>

## 11 — Event overlay

To find relationships between the events of different metrics, overlay them on one graph.

How to overlay a metric with the other metric's events:

1. Choose the metric you want to overlay with events:



2. Add the new graph tab and choose the other metric. Enable the table view for this metric.



3. Get back to the first metric. In the _Event overlay_ dropdown menu, choose your second metric of interest.



The event flags appear on a graph.



## 12 — CSV

## 13 — Meta-metrics

### Receive status

### Sampling

### Cardinality

### Mapping status

##  14 — Lock Y-scale

## 15 — Copy link to clipboard

## 16 — Zoom options

## 17 — Switch database

## 18 — Query with PromQL

## 19 — Metric tabs

Custom metric resolution
