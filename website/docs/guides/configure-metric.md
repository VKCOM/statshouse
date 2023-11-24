---
sidebar_position: 3
---

# Configure your metric



configure the metric via the UI or start sending data to it right away—with minimal configuration
right from your code.


:::danger
If you edit someone's metric or start sending data to it, you can spoil the metric itself or the related data. See
how to [edit metrics] or [send data] safely.
:::


With StatsHouse, you can give your metric a name, set up tags, start sending data to it, and enjoy visualization.
Most likely, you cannot do anything wrong to StatsHouse or other users with your metrics, but to get the most out of
monitoring, check the best practices for [configuring metrics](#configure-a-metric):
set the [metric type](#metric-type), [resolution](#resolution), [tags](#tags), and other parameters.

:::info
To learn more about mechanisms that make StatsHouse robust, highly-available, and scalable, refer to the
[conceptual overview].
:::

The typical workflow is to [create the new metrics](#create-a-metric),
but you still can [use the existing ones](#use-existing-metrics).

:::warning
Check the [limitations to creating metrics](#limitations-to-creating-metrics).
:::

:::danger
If you edit someone's metric or start sending data to it, you can spoil the metric itself or the related data. See
how to [edit metrics] or [send data] safely.
:::



## Configure a metric

You can [start sending data](send-data.md) to your named metric with no additional configuration.
StatsHouse does everything for you, but it may lead to increased sampling. In most cases, you do not need
to worry about sampling, but if you are not sure, check the [conceptual overview].

:::info
Sampling prevents StatsHouse from database overload. If you send too much, StatsHouse _samples_
data: it removes random data rows and multiplies the remaining ones so that the resulting aggregation and
digests stay the same. Find more about [sampling],
[aggregation], and
[digests].
:::

### Metric type

A metric is how you measure the things you are interested in:

> "How many errors did occur while writing crucial data to this database?"

> "How many support requests were serviced properly yesterday?"

> "How does the new feature affect website performance?"

To get the most out of monitoring with StatsHouse, you should clearly understand what you want to measure and how you will use your measurements.

You may want to count the number of API method calls per 5 seconds. Or you may want to
know how long it takes for a method to respond. Or you may want to know how many unique users have sent
their requests to a service. All these measurements are different metric types.

> "But why should I bother about choosing the proper metric type?"

A metric type affects the range of
[desriptive statistics](view-graph#desriptive-statistics-available-for-a-metric) available for your
metric to view and analyze.

With StatsHouse, you can use three basic metric types—see the table below:

| Metric type    | What does it measure?                                                              | Examples                                                                                                                                   |
|----------------|------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| Counter        | It counts the number of times a controllable event has occurred.                   | The number of API method calls<br/>The number of requests to a server<br/>The number of errors received while sending messages             |
| Value metric   | It measures non-controllable parameters so that each measurement becomes an event. | How long does it take for a service to generate a newsfeed?<br/>What is CPU usage for this host?<br/>What is the response size (in bytes)? |
| Unique counter | It counts the number of unique events.                                             | The number of unique users who sent packages to a service                                                                                  |
| **Mixed??**    |                                                                                    |                                                                                                                                            |

For example, percentiles are available






### Disable a metric

Disabling metric stops data recording for this metric and removes it from all lists.
This is most close thing to deleting metric (which statshouse does not support).
You will need a direct link to enable metric again.
