---
sidebar_position: 4
---

import Ui from '../img/ui.png'
import TableView from '../img/table-view.png'
import HostMetrics from '../img/host-metrics.png'
import ServiceMetrics from '../img/service-metrics.png'
import RenameGraph from '../img/rename-graph.png'
import TimePeriod from '../img/time-period.png'
import TimeAgo from '../img/time-ago.png'
import AggregationInterval from '../img/aggregation-interval.png'
import TopN from '../img/top-n.png'
import MaxHostEnable from '../img/max-host-enable.png'
import MaxHostRes from '../img/max-host-res.png'
import MaxHost1 from '../img/max-host-1.png'
import MaxHost2 from '../img/max-host-2.png'
import GroupByChoose from '../img/groupby-choose.png'
import GroupByGraph from '../img/groupby-graph.png'
import FilterByTag from '../img/filter-by-tag.png'
import Negate from '../img/negate.png'
import Sort from '../img/sort.png'
import Auto from '../img/auto.png'
import TableChoose from '../img/table-choose.png'
import Overlay1 from '../img/overlay1.png'
import Overlay2 from '../img/overlay2.png'
import Overlay3 from '../img/overlay3.png'
import CSV from '../img/csv.png'
import ReceiveStatus from '../img/receive-status.png'
import BottomN from '../img/bottom-n.png'
import SamplingSrcAggr from '../img/sampling-src-aggr.png'
import SamplingYellowAlert from '../img/sampling-yellow-alert.png'
import HourCardinality from '../img/hour-cardinality.png'
import ReceiveErrorAlert from '../img/receive-error-alert.png'
import MappingFlood from '../img/mapping-flood.png'
import LockY from '../img/lock-y.png'
import Zoom from '../img/zoom.png'
import SwitchDb from '../img/switch-db.png'
import Prom from '../img/prom.png'
import PromQuery from '../img/prom-query.png'
import MetricTabs from '../img/metric-tabs.png'
import MetricTabDelete from '../img/metric-tab-delete.png'
import DeltaResAggr from '../img/delta-res-aggr.png'
import ResYellow from '../img/res-yellow.png'
import ResRed from '../img/res-red.png'

# View metric data

Display data on a graph, in a [table](#10--table-view), or as a [CSV](#12--csv) file via the StatsHouse UI.
For complicated scenarios, [query StatsHouse with PromQL](#18--promql-query-editor).
StatsHouse does not support viewing data via third-party applications.

To learn more about viewing options, refer to the picture below and the navigation bar.

<img src={Ui} width="1000"/>

## 1 — Metric name

Choose a metric name to refer to existing metrics.

:::warning
Do not commit configuration changes or send data to someone else's metric as you can spoil the metric or the related data.
:::

### Host metrics

Some popular host metrics are built-in such as CPU or disk usage, and more:

<img src={HostMetrics} width="300"/>

:::tip
Host metric names begin with `host_`.
:::

Find the full list of host metrics and their implementation 
on 
[GitHub](https://github.com/VKCOM/statshouse/blob/1c45de2c5ecee27a767a4821ed85315c1a0dff49/internal/format/predefined_hardware.go#L37). 
For more details, see the [Administrator guide](../admin/host-metrics.md).

### Service metrics

Metrics having two underscores in the beginning are for StatsHouse internal use only, for example:

<img src={ServiceMetrics} width="300"/>

You cannot edit them. See [Meta-metrics](#13--meta-metrics) for details.

### Common metrics

If you have StatsHouse deployed in your organization, you can find a set of metrics that are common for all the 
engines, services, microservices, proxies, etc. in the organization.

### "How can I find the metrics author?"

There is no mechanism for checking a metrics author in StatsHouse, but sometimes authors mention how to find them 
in the metric description section. Otherwise, use your organization's internal communication channels.

### "How can I display several metrics on a graph?"

[Query StatsHouse with PromQL](query-wth-promql.md).
To compare metrics, [create a dashboard](dashboards.md). To find relationships 
between the metrics or events, use [Event overlay](#11--event-overlay).

## 2 — Graph name

You can edit the graph name so that the metric name remains the same.

<img src={RenameGraph} width="800"/>

This changed graph name is saved in URL only.

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
[specify the metric type in the UI](edit-metrics.md#aggregation).

:::tip
If you choose to show _count_ or _sum_ as a descriptive statistic,
while an [aggregation interval](#6--aggregation-interval) is set to _Auto_, the resulting graph may look difficult to
grasp.

Instead, choose the _count/sec_ and _sum/sec_ statistics.
These are normalized data, which are independent of an aggregation interval.
:::

#### "Why do I see a non-integer number for a _count_ statistic?"

[Sampling coefficients](#sampling) should sometimes be non-integer to keep aggregates and statistics the same.
This leads to non-integer values for the _count_ statistic, which is an integer number at its core.

#### Cumulative functions

Cumulative functions are not meaningful for _unique_ metrics.

#### Derivative functions

A derivative function shows the rate of change for the initial function, i.e., the difference between the two nearest 
function values.

#### Percentiles

Percentiles are available for _value_ metrics only.
To get them, [enable percentiles](edit-metrics.md#percentiles) in the UI.

Note that the amount of data increases for a metric with percentiles, so enabling them may lead to increased 
[sampling](../overview/concepts.md#sampling). If it is important for you to have the lower sampling factor, keep an 
eye on your metric [cardinality](#cardinality) or choose custom [resolution](edit-metrics.md#resolution) 
for writing metric data.

## 4 — Time period

Display data for a specific time period: the last five minutes, last hour, last week,
and more—even for the last two years.

Choose a particular date and particular time. And you can combine the controls:
for example, you can display the last-hour data for the previous day:

<img src={TimePeriod} width="500"/>

Compare the data for a chosen time period with the data for the same period in the past: a day, a
week, a year ago.

<img src={TimeAgo} width="300"/>

When choosing time periods, please be aware of a chosen [aggregation interval](#6--aggregation-interval).

:::tip
Make sure the chosen time period is larger than the aggregation interval.
For example, choose a 7-day time period and a 24-hour aggregation interval.
:::

For real-time monitoring, use [Live mode](#5--live-mode).

## 5 — Live mode

Enable _Live mode_ to view data in real time.

By default, the metric URL does not contain the _Live mode_ parameter.

:::tip
To share the link to real-time metric data, add the `live=1` query string to the metric URL.
:::

## 6 — Aggregation interval

Aggregation interval is a kind of resolution for your metric data.
The larger aggregation interval you choose, the smoother look your graph has:

<img src={AggregationInterval} width="600"/>

### _Auto_ and _Auto (low)_

An _Auto_ interval uses the 
[_minimal available interval_ for aggregation](../overview/concepts.md#minimal-available-aggregation-interval)
to show data on a graph.
This interval _varies_ depending on the currently available aggregation:
* per-second aggregated data is stored for the first two days,
* per-minute aggregated data is stored for a month,
* per-hour aggregated data is available forever.

The currently available aggregation is also related to
a metric [resolution](../overview/concepts.md#resolution).

The _Auto (low)_ aggregation interval reduces the displayed resolution by a constant making the graph look smoother 
even when you view data using the minimal available aggregation interval:

<img src={Auto} width="1000"/>

### 6a — "Delta"

:::info
The functionality described below will be redesigned.
:::

The Δ ("delta") value indicates the aggregation interval (resolution) corresponding to the interval
between the neighboring points on a plot.

1. When you choose a specific aggregation interval (not the _Auto_ or _Auto (low)_, but _1 second_, _5 minutes_, _1
   hour_, etc.), the "delta" shows the resulting aggregation to be displayed on a graph.

   What does it depend on?

    * On the **[initial resolution](#6b--resolution)** you use for sending data for your metric.
      The default one is to send data once per second, but you can send it once per 5 seconds, for example.
    * On the **[minimal available aggregation interval](../overview/concepts.md#minimal-available-aggregation-interval)**.
      Per-second data is stored for the first two days, then aggregated to per-minute and per-second data.
    * On the **[chosen aggregation interval](#6--aggregation-interval)**.
      Per-second aggregates may be available, but you are free to choose per-hour aggregation.
    * On the **[chosen time period](#4--time-period)**.
      You can view data for an hour (fewer points to display) or a week (more points to display).

2. When you choose the [_Auto_ or _Auto (low)_](#auto-and-auto-low) aggregation interval, the "delta" shows the
   minimal available aggregation interval to be displayed on a graph — with regard to your **display resolution**.
   The display resolution affects the size of the graph in pixels. Sometimes the size of the graph does not
   allow you to display as many points as you need. So the data is displayed at a lower resolution.

For the _Auto_ or _Auto (low)_ aggregation interval, we recommend using the _count/sec_ and _sum/sec_ statistics.
If you still do use the _count_ and _sum_ ones, pay attention to the "delta". In this case, the statistic shows
the number of events for the time interval (which is the "delta" value), and can vary as well.

:::tip
#### How it works in practice

Suppose StatsHouse has detailed data for a metric:
* it is initially written at a high, though not maximum, resolution (5 seconds);
* the data is still fresh (it has not turned into minute or hour aggregates);
* you have chosen a small aggregation interval in the interface (5 seconds also).

But:
* you requested data for a large time period (a week).

StatsHouse will display the data at _NOT the 5-second_ resolution (as you wanted), but at the _300-second_
resolution: data with this aggregation interval (Δ300s) fits on the graph.

<img src={DeltaResAggr} width="1000"/>
:::

### 6b — Resolution

If the owner has [set a custom resolution](edit-metrics.md#resolution) for a metric,
it is displayed above the graph as the yellow badge.

<img src={ResYellow} width="800"/>

If the custom resolution value is greater than the selected aggregation interval, the badge turns red.

<img src={ResRed} width="1000"/>

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

To get the lowest values, choose one of the _Bottom N_ options in the same dropdown menu:

<img src={BottomN} width="150"/>

## 9 — Max host

Enable the [Max host](design-metric.md#host-name-as-a-tag) option to find the host 
that sends the maximum value for your metric:

<img src={MaxHostEnable} width="100"/>

View the list of all hosts sorted by the maximum value or copy the list to clipboard:

<img src={MaxHostRes} width="600"/>

The idea of the maximum value is valid only with respect to the chosen time interval.

If you enable the _Max host_ option and view the whole graph, you see the host that sends the 
value, which is the maximum in the whole [time period](#4--time-period)—the _Last 24 hours_ in the example below:

<img src={MaxHost1} width="900"/>

When you move the cursor over the graph, you see the resulting _Max host_ changing. It now shows you 
the host that sends the value, which is the maximum for the available 
[aggregation interval](#6--aggregation-interval)—in the example below, for the minute you are pointing at:

<img src={MaxHost2} width="900"/>

## 10 — Table view

View the metric events in a table, for example:

<img src={TableView} width="900"/>

Choose the tags to show or hide as columns (an "eye" symbol), or to group by (a "checkmark" sign):

<img src={TableChoose} width="500"/>

## 11 — Event overlay

Overlay a metric with the events of the other metric to find correlations.

1. Choose the metric you want to overlay with events. Add the new [metric tab](#19--metric-tabs):

<img src={Overlay1} width="900"/>

2. On the new metric tab, choose the second metric you are interested in. Enable the table view for this metric:

<img src={Overlay2} width="900"/>

3. Get back to the first metric. In the _Event overlay_ dropdown menu, choose your second metric of interest. The 
   event flags appear on a graph:

<img src={Overlay3} width="900"/>

## 12 — CSV

Export metric data for a chosen time period to a CSV file:

<img src={CSV} width="800"/>

## 13 — Meta-metrics

Metrics having two underscores in the beginning are meta-metrics. The most important ones are shown in the UI:
* [Receive status](#receive-status)
* [Sampling](#sampling)
* [Cardinality](#cardinality)
* [Mapping status](#mapping-status)

Some of these metrics may be not sampled at all. The _Receive status_ and _Sampling_ metrics
are sent in a special compact form to save traffic.

### Receive status

This meta-metric redirects you to the `__src_ingestion_status` metric.
It shows if there are errors when receiving metrics: whether data are formatted properly, 
or a counter has a negative value, or a `NaN` value has been sent.

The red alert informs you about the errors:

<img src={ReceiveErrorAlert} width="800"/>

Here are some error examples:

<img src={ReceiveStatus} width="300"/>

For example, the `err_map_per_metric_queue_overload`, `err_map_tag_value`, or `err_map_tag_value_cached` tags 
indicate the slowdowns or errors of the [mapping mechanism](../overview/components.md#metadata).

This metric uses the sampling budget of a metric it refers to, so the error flood cannot affect the other metrics.

The `err_*_utf8` statuses store the original string values in `hex`.

### Sampling

StatshHouse has two bottlenecks where it samples data: an agent and an aggregator. An agent is also referred 
to as _source_ because it is the same machine the data come from.

[Sampling](../overview/concepts.md#sampling) means that StatsHouse throws away pieces of data to reduce its 
overall amount. 
To keep aggregates and statistics the same, StatsHouse multiplies the rest of data by a sampling coefficient (or a 
sampling factor).

The _Sampling source/aggregator_ meta-metric redirects you to the sampling coefficient information for the agent and 
aggregation levels:
* to `__src_sampling_factor` for the agent (source),
* to `__agg_sampling_factor` for the aggregator.

The non-integer sampling coefficients may lead to 
[non-integer values for the _count_ statistic](#why-do-i-see-a-non-integer-number-for-a-count-statistic).

If the sampling coefficient for a metric is higher than 1, it is displayed with a yellow alert.

<img src={SamplingYellowAlert} width="800"/>

If the sampling coefficient for a metric is higher than 5, it is displayed with a red alert.

<img src={SamplingSrcAggr} width="810"/>

The _count_ statistic for this metric shows the number of agents having set this coefficient in a particular second.

Learn more about StatsHouse [agents](../overview/components.md#agent) and
[aggregators](../overview/components.md#aggregator), and what [sampling](../overview/concepts.md#sampling) is.

### Cardinality

In StatsHouse, metric [cardinality](../overview/concepts.md#cardinality) is how many unique tag value combinations 
you send 
for a metric.

The _Cardinality_ meta-metric redirects you to the `__agg_hour_cardinality` metric:

<img src={HourCardinality} width="600"/>

It shows the estimated hour cardinality for a metric.
Estimation means linear interpolation between cardinality values for the nearest hours.

This cardinality estimation is based on data from all the aggregators and their shards. 
So an _avg_ statistic for this metric shows full cardinality, which may be grouped by aggregator.

### Mapping status

If you create too many tag values, which have not been 
mapped yet, the [mapping](../overview/components.md#the-budget-for-creating-tag-values) flood errors appear:

<img src={MappingFlood} width="800"/>

Mapping errors indicate that the number of newly created tag values exceeds the mapping budget per day.
Learn more about [mapping](../overview/components.md#metadata) 
and [how many tag values](design-metric.md#how-many-tag-values) to create per metric.

## 14 — Lock Y-axis

By default, the Y-axis is self-scaling—it adjusts itself to a data amplitude.
You may need to lock it.

For example, your data normally vary in a range of 1–100, and you may see peaks sometimes.
With the _Lock Y-axis_ feature, you switch autoscaling off to view your data within a given range of values 
regardless of peaks.
For data with daily variations, you may want to zoom in without Y-axis autoscaling:

<img src={LockY} width="1000"/>

:::info
If you need a logarithmic scale, switch to the [PromQL query editor](#18--promql-query-editor). Use standard PromQL 
functions:
* [`ln()`](https://prometheus.io/docs/prometheus/1.8/querying/functions/#ln)
* [`log2()`](https://prometheus.io/docs/prometheus/1.8/querying/functions/#log2)
* [`log10()`](https://prometheus.io/docs/prometheus/1.8/querying/functions/#log10)
  :::

## 15 — Copy link to clipboard

Adjust the viewing options in the UI—group or filter your data by tags—and share the link with these options included.

Please note that the only viewing option not included in the link is _Live mode_.
Check the [tip](#5--live-mode) for sharing data with the _Live mode_ option included.

If you have several [metric tabs](#19--metric-tabs) opened, 
the _Copy link to clipboard_ option copies the link to the current metric tab only.

See also the [_Open in a new browser tab_](#20--open-in-a-new-browser-tab) option.

## 16 — Zoom options

Move back and forth, zoom in or out for both X- and Y-axes.

To get back to the initial view, _Reset zoom_:

<img src={Zoom} width="300"/>

Switch off autoscaling Y-axis with the [_Lock Y-axis_](#14--lock-y-axis) feature.

## 17 — Switch database

Previously, StatsHouse used a slower database that still stores useful historical data.
In most cases, you should not switch to this slow database—you will probably see no data and a warning:

<img src={SwitchDb} width="800"/>

## 18 — PromQL query editor

To broaden the range of operations available when viewing data, we supported PromQL, 
or [Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/).

Switch to PromQL query editor for complex viewing scenarios:

<img src={Prom} width="300"/>

You will get an autogenerated PromQL query describing the current graph view. 

Use the PromQL editor to run your queries.
To switch back to graph mode, press _Filter_:

<img src={PromQuery} width="300"/>

Learn how to [query with PromQL](query-wth-promql.md) in detail.

## 19 — Metric tabs

Use _Metric tabs_ to [create dashboards](dashboards.md) or to [overlay metric events](#11--event-overlay).
Duplicate the current graph view to a new tab and choose the other metric, 
or copy the graph's URL and paste it to a new metric tab:

<img src={MetricTabs} width="400"/>

Remove the tab if necessary:

<img src={MetricTabDelete} width="400"/>

## 20 — Open in a new browser tab

This option implements the same behavior as 
the [_Copy link to clipboard_](#15--copy-link-to-clipboard) 
but instead of copying the link it opens it in the new browser tab.

All the [_Copy link to clipboard_](#15--copy-link-to-clipboard) limitations apply:
only the current metric tab is opened, and _Live mode_ is not included.
