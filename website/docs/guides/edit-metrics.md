---
sidebar_position: 3
description: Get to know how to set up metric types, tags, resolution, and more.
---

import MetricTypes from '../img/metric-types.png'

# Edit metrics


You have just created a metric—it now has a name and nothing more.

:::info 
For a created metric, you have to choose the [metric type](#metric-type). This is the minimal configuration needed 
to start sending data. 
Setting up other metric parameters is optional.
:::

Before you start configuring metrics, you may have a question:

> "Can I skip configuration for my metric?"

No. As soon as your metric has a name, you have to specify a [metric type](#metric-type) in your sending requests.
Then, you can configure more parameters for advanced usage, or start sending data right away.

For a full list of configuration options, see the upper-right navigation menu on this page.

:::warning
Do not commit configuration changes or send data to someone else's metric as you can spoil the metric or the related data.
:::

## Metric type

> "What is a metric type? Why do I have to specify it?"

A metric is a [system for measuring something](https://dictionary.cambridge.org/dictionary/english/metric)—it 
is _how you measure_ the things you are interested in.

You can measure same things in different ways. To evaluate service availability, you can count the 
number of handled requests, which is just a number of events; or you can measure processing time for these requests, 
which is the value accompanying each event (each request).

StatsHouse is a powerful tool, but it cannot decide for you, so you should clearly understand what you want to 
measure, and how you will measure it.

:::important
A metric type affects the range of
[descriptive statistics](view-graph#desriptive-statistics-available-for-a-metric) available for your metric to view 
and analyze. For example, you cannot calculate the sum for unique metrics (you can, but this sum is meaningless); 
and percentiles are available for values only.

See more about [enabling percentiles](#enable-percentiles) 
and [showing the proper descriptive statistics](#aggregation) in the UI. 
:::

> "Which metric types are available in StatsHouse?"

With StatsHouse, you can use three basic metric types.

<img src={MetricTypes} width="800"/>

See the table below for definitions and examples:

| Metric type | What does it measure?                                                                     | Examples                                                                                                                                        |
|-------------|-------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Counter     | It counts the number of times an event has occurred.                                      | The number of API method calls<br/>The number of requests to a server<br/>The number of errors received while sending messages                  |
| Value       | It measures magnitude of a parameter.<br/>A measurement event itself is counted as well.  | How long does it take <br/>for a service to generate a newsfeed?<br/>What is CPU usage for this host?<br/>What is the response size (in bytes)? |
| Unique      | It counts the number of unique events.<br/>The total number of events is counted as well. | The number of unique users who sent packages to a service                                                                                       |

Value and unique metrics have an ordinary counter inside, so you should not implement your own counters 
for these metric types. Imagine you measuring a value metric (e.g. the response size in bytes) once in a second:
* You get the "value level" that is your parameter magnitude:
1024 bytes, then 2048 bytes, etc. Please note that this "level" is [aggregation], not an exact value for a 
  particular moment in time.
* You also get the "counter" for your value metric that shows the number of times you performed 
your measurements: +1 for the first second, +1 for the next one, etc. 

The same applies to unique metrics: they provide you with the number of unique events and the total number of events.
See more on 
[changing or combining metric types](#changing-or-combining-metric-types)
and [user-guided sampling](#user-guided-sampling).

:::note
Metric types should not be confused with [data types](https://en.wikipedia.org/wiki/Data_type) in programming
languages. You should not specify the type of your data: whether it is `int`, `float`, or `double`, etc.
:::

> "OK. How can I configure my metric type?"

You should specify a [metric type](#metric-type) in your sending requests.

For a toy example, you may send your metric data with one of these `bash` scripts:
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"value":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```
where `counter` or `value` are the metric types.

You can also use one of the client libraries. For example, with the
[StatsHouse client library for Python](https://github.com/VKCOM/statshouse-py/tree/master), you have 
to choose one of these methods according to your metric type:
```Python
statshouse.count("my_metric", {}, 0.42)
```
```Python
statshouse.value("my_metric", {}, 0.42)
```
```Python
statshouse.unique("my_metric", {}, 0.42)
```
Having done this, you may send your data, or continue configuring metric parameters.

## Tags
