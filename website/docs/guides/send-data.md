---
sidebar_position: 3
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import MetricTypes from '../img/metric-types.png';
import WhatIsMetric from '../img/what-is-metric.png'
import AbTest from '../img/ab-test.png'

# Send metric data

:::important
Before sending data, [create a metric](create-metric.md) manually via the UI.
:::

:::warning
Do not send data to someone else's metric as you can spoil the metric data.
:::

## What are metrics in StatsHouse?

A metric is how you measure something you are interested in—read more about [metric types](#how-to-choose-a-metric-type).
A basic metric structure in StatsHouse looks like this (here, in a [MessagePack](https://github.com/msgpack/msgpack) 
format):

<img src={WhatIsMetric} width="1000"/>

<details>

    <summary>
        See code from this picture
        </summary>
    
        ```yaml
        {
          metrics: [
            {
              ts:   1670673392,     # uint32, UNIX timestamp in seconds (optional)
              name: "foobar",       # string([a-zA-Z][a-zA-Z0-9_]*), metric name
              tags: {
                "env":              # string([a-zA-Z][a-zA-Z0-9_]*), tag name
                  "production"      # string(printable UTF-8),       tag value
              },
              counter: 100500.1,    # float64,        number of observed events
              value:   [0.7],       # array(float64), observed values array
              unique:  [591068825], # array(int64),   observed IDs array
            }
          ]
        }
        ```

    </details>

(More [data formats](../conceptual%20overview/components.md#agent) are supported.)

To start sending data, check the following: 
* [how to send metric data via client libraries](#how-to-send-data-via-client-libraries),
* [how to use tags](#how-to-use-tags), 
* and [how to choose a metric type](#how-to-choose-a-metric-type).

## How to send data via client libraries

StatsHouse client libraries help to instrument your application code
so that you can send properly formatted data for your metric:

- [Go](https://github.com/VKCOM/statshouse-go)
- [PHP](https://github.com/VKCOM/statshouse-php)
- [C++](https://github.com/VKCOM/statshouse-cpp)
- [Java](https://github.com/VKCOM/statshouse-java)
- [Python](https://github.com/VKCOM/statshouse-py)

There is also a special module for using StatsHouse with [nginx](https://github.com/VKCOM/nginx-statshouse-module).

Below are the simple code examples using some of these libraries. 
Prior to copying and pasting the code, install the library you need using recommendations 
from the corresponding README file.

<Tabs>

<TabItem value="cpp" label="C++">

```cpp
#include "statshouse.hpp"
#include <cstdio>

using namespace statshouse;
    
Registry r{{
    logger: puts // debug output
}};

int main() {
    auto v = r.metric("my_value_metric")
        .tag("subsystem", "foo")
        .tag("protocol", "bar")
        .event_metric_ref();

    v.write_value(42.5);
    return 0;
}
```

</TabItem>

<TabItem value="py" label="Python">

```Python
import statshouse
    
statshouse.value("my_value_metric", {"subsystem": "foo", "protocol": "bar"}, 42.5)
```

</TabItem>

<TabItem value="go" label="Go">
```go
TEST
```
</TabItem>
<TabItem value="php" label="PHP">
```php
TEST
```
</TabItem>
<TabItem value="java" label="Java">
```java
TEST
```
</TabItem>

</Tabs>

As soon as there are only five native client libraries in StatsHouse, you may have questions:

#### "What if there is no client library for a programming language I need?

The preferred way is to file a [feature request](https://github.com/VKCOM/statshouse/issues) for us on GitHub.

You can contribute to StatsHouse by creating a library for the language you need.
Though, we do not recommend doing this as we won't be able to provide guarantees and support.

If you are sure about creating a library,
please use one of the existing StatsHouse libraries as a model for your own one—pay
your attention to StatsHouse [data model](../conceptual%20overview/concepts.md).

#### "What if the existing library does not have the required functionality?"

The preferred way is to file a [feature request](https://github.com/VKCOM/statshouse/issues) for us on GitHub.

Alternatively, you can prepare a JSON file and send your formatted data to StatsHouse,
but we do not recommend doing this as you won't benefit from aggregation and other native StatsHouse features.

## How to send data without client libraries

For a toy example or testing purposes, you may send data using [Netcat](https://netcat.sourceforge.net):

```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```

See the [Quick start](../quick-start.md#send-metric-data) for a context.

:::important
We strongly recommend using the [StatsHouse client libraries](#how-to-send-data-via-client-libraries).

Client libraries [aggregate](../conceptual%20overview/concepts.md#aggregation) data before sending them to StatsHouse.
While it may sound counterintuitive, by aggregating, client libraries prevent you from losing data.
Without a client library, you can create a socket, prepare a JSON file, and send your formatted data.
This sounds simple, but only if you have not so much data.

StatsHouse uses [UDP](../conceptual%20overview/components.md#receiving-data-via-udp).
If you send a datagram per event, and there are too many of them,
there is a risk of dropping datagrams due to UDP socket buffer overflow, and no one will notice it.

If you do not use the client library, the non-aggregated data will reach StatsHouse
[agent](../conceptual%20overview/components.md#agent), and the agent will aggregate them anyway.
:::

## How to use tags

Use tags to differentiate the characteristics of what you measure, the contributing factors, or a context.

### What are tags?

Tags are additional dimensions you use to filter or group your data. They are sometimes mentioned as "labels" or 
"keys." Tags are the _name-value_ pairs.

Imagine you conducting an A/B test: which color-text combination is better for a button? You measure the number 
of clicks per button and use the tags:

<img src={AbTest} width="900"/>

Tagged metrics help to verify hypotheses about your data. For monitoring, troubleshooting, or other purposes, you may 
ask questions like these:

> "Does the error rate differ for platforms?"

or

> "What is the region we have the highest request rate from? Does it differ for environments?"
 
For these example questions, you may send metrics (here, using the client library for Python):

```Python
statshouse.value("error_rate", {"platform": "web"}, 42.5)
                   ↑                 ↑         ↑      ↑          
                metric name          ↑         ↑   measurement      
                                 tag name      ↑       
                                             tag value 
```
or

```Python
statshouse.value("request_rate", {"env": "production", "region": "moscow"}, 42.5)
                   ↑                 ↑         ↑           ↑        ↑         ↑
                metric name          ↑         ↑           ↑        ↑      measurement
                                 tag name      ↑       tag name     ↑
                                             tag value            tag value   
```

Then you can [filter or group](view-graph.md#7--tags) your data using these tags.
When you view a metric on a graph, the default UI behavior is to use no grouping.

### How to name tags

You can use system tag names (`0..15`) to send data. For convenience, add aliases (custom names) to your tags.

Please use these characters:
* Latin alphabet
* integers
* underscores

:::note
Do not start tag names with underscores. They are for StatsHouse internal use only.
:::

You can use the same tag names for different metrics.

In the StatsHouse UI, you can [edit](edit-metrics.md#tags) tag names and add short descriptions to them.

### How many tags

You can use 16 tags per metric:
* tag `0` is usually for an `environment`,
* tags `1..15` are for any other characteristics.

There is also one more [string tag](#string-tag):
* tag `__s`.

#### "What if I want more tags?"

Unfortunately, it is impossible for now. We plan to increase the number of tags in the future.

### How many tag values

Tag values must be UTF-8 string values.

There is no formal limitation for a number of tag values, but the rule is to have **not that many** of them.

Tags with many different values such as user IDs or email addresses may lead to 
[mapping flood](view-graph.md#mapping-status) errors or increased [sampling](view-graph.md#sampling) due to 
high [cardinality](../conceptual%20overview/concepts.md#cardinality).
In StatsHouse, metric cardinality is how many unique tag value combinations you send for a metric.

If a tag has too many values, they will soon exceed the 
[mapping budget](../conceptual%20overview/concepts.md#mapping-and-budgets-for-creating-metrics) and will be lost: tag values 
for your measurements will be `Empty`.

Even if all your tag values have been already mapped, and you 
[avoid the mapping flood](edit-metrics.md#raw-values) but keep sending data with many tag values, 
your data will probably be [sampled](../conceptual%20overview/concepts.md#sampling). Sampling means that 
StatsHouse throws away pieces of data to reduce its overall amount. To keep aggregation, statistics, and overall 
graph's shape the same, StatsHouse multiplies the rest of data by a sampling coefficient.

If it is important for you not to sample data at all, 
[keep an eye on your metric cardinality](view-graph.md#cardinality) or reduce [resolution](edit-metrics.md#resolution) for 
your metric.

We recommend that the very first tags have the lowest cardinality rate. For example, `tag_0` is usually an 
`environment` tag having not that many values.

:::tip
If you need a tag with many different 32-bit integer values (such as `user_ID`), use the 
[Raw](edit-metrics.md#raw-values) tag values to avoid the mapping flood.

For many different string values (such as `search_request`), use a [string tag](#string-tag).
:::

### String tag

Use a _string tag_ (`__s`) when you need a tag with many different `string` values such as referrers or search
requests.

With the common tags, you will get [mapping flood](view-graph.md#mapping-status) errors very soon for this scenario.
The _string tag_ stands apart from the other ones as its values are not 
[mapped](../conceptual%20overview/concepts.md#mapping-and-budgets-for-creating-metrics) to integers. Thus, you can avoid 
[mapping flood](view-graph.md#mapping-status) errors and massive sampling.

The string tag has a special storage: when you send your data labeled with many `string` tag values, only the most 
popular tag values are stored. The other tag values for this metric become `Empty` and are aggregated.

To filter data with the _string tag_ on a graph, [add a name or description](edit-metrics.md#string-tag) to it.

### Host name as a tag

To view statistics for each host separately, you may want to use host names as tag values. 
Try the _Max host_ option instead. You do not have to send something special to get use 
of _Max host_—[enable it in the UI](view-graph.md#9--max-host).

Using host names as tag values prevents data from being aggregated and leads to increased sampling. 
By contrast, the _Max host_ option does not lead to increased sampling but allows you to find the host that sends the 
maximum value for your metric.

The _Max host_ option helps to answer questions like these:
* which host has the maximum disk space usage, or
* which host shows the maximum rate for a particular error type.

In most cases, it is enough to know the name of the most problematic host to get logs and solve the issue.

We also recommend using the `environment` tag (or similar) instead of `host_name`. When you deploy an experimental feature 
to one or more hosts, label them with the `staging` or `development` tag values instead of their host names.

Learn how the [_Max host_](../conceptual%20overview/concepts.md#max-host-tag) option is implemented.

## How to choose a metric type

You can measure same things in different ways—they are metric types.

For example, how to evaluate _service availability_? Try this:
* count the number of handled requests          → get a <text className="orange-text">**counter**</text> for the events
* measure processing time for these requests    → get the <text className="orange-text">**value**</text> accompanying each event
* count the number of unique users whose requests were handled properly → get the <text className="orange-text">**unique**</text> counter
  
`Counter`, `value`, and `unique` are basic metric types in StatsHouse:

<img src={MetricTypes} width="800"/>

See the table below for definitions and examples:

| Metric type  | What does it measure?                                                                     | Examples                                                                                                                                        |
|--------------|-------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `counter`    | It counts the number of times an event has occurred.                                      | The number of API method calls<br/>The number of requests to a server<br/>The number of errors received while sending messages                  |
| `value`      | It measures magnitude of a parameter.<br/>A measurement event itself is counted as well.  | How long does it take <br/>for a service to generate a newsfeed?<br/>What is CPU usage for this host?<br/>What is the response size (in bytes)? |
| `unique`     | It counts the number of unique events.<br/>The total number of events is counted as well. | The number of unique users who sent packages to a service                                                                                       |

:::important
A metric type affects the range of
[descriptive statistics](view-graph.md#3--descriptive-statistics) available for your metric to view
and analyze. For example, percentiles are available for _values_ only.
Or you cannot view the cumulative graph for _uniques_.

See more about [enabling percentiles](edit-metrics.md#percentiles)
and [showing descriptive statistics](edit-metrics.md#aggregation) in the UI.
:::

:::note
Metric types should not be confused with [data types](https://en.wikipedia.org/wiki/Data_type) in programming
languages. You should not specify the type of your data: whether it is `int`, `string`, etc.
:::

### Metric types and their combinations

In the database, where StatsHouse stores metric data, the data model for each metric looks like this:

| timestamp | metric name | tag_0   | tag_1..tag_15 | counter | sum   | min | max  | unique |
|-----------|-------------|---------|---------------|---------|-------|-----|------|--------|
| 13:45:05  | my_metric   | dev     | -             | 100     | 13000 | 20  | 1200 | -      |
| 13:45:05  | my_metric   | staging | -             | 200     | 1600  | 3   | 1100 | -      |
| 13:45:05  | my_metric   | staging | -             | 5       | 80    | 25  | 30   | -      |

The `sum`, `min`, and `max` columns are an [aggregate](../conceptual%20overview/concepts.md#aggregation) for a `value` metric.

Read more about [metric type implementation](../conceptual%20overview/concepts.md#metric-types) in StatsHouse.

Check the valid metric type combinations in the table below:

| What you send                    | What you get                                                         |
|----------------------------------|----------------------------------------------------------------------|
| `"counter":100`                  | `counter`                                                            |
| `"value":[1, 2, 3]`              | `counter` and `value` (i.e., `sum`, `min`, `max`)                    |
| `"unique":[17, 25, 37]`          | `counter`, `value` (i.e., `sum`, `min`, `max`), and `unique`         |
| `"counter":6, "value":[1, 2, 3]` | [User-guided sampling](#user-guided-sampling)                        |
| `"value":100,"unique":100`       | <text className="orange-text">This is not a valid combination</text> |

If you refactor your existing metric, i.e., switch between different metric types for a single metric, the data may
become confusing or uninformative.

:::important
Keep sending data of the **same type per metric**.
:::

### Implementing a separate `counter` for `value` and `unique` metrics

If you send a `value` or `unique` array, the size of this array becomes the `counter` for this metric. 
Thus, you should not implement a separate counter metric for your `value` or `unique` metrics.
You still can specify `counter` to implement [user-guided sampling](#user-guided-sampling).

Imagine you measuring a value metric (e.g., the response size in bytes) once in a second:
* You get `value` that is your parameter magnitude:
  XXXX bytes, then YYYY bytes, etc. Please note that this "level" is [aggregation](../conceptual%20overview/concepts.md#aggregation), not
  an exact value for a particular moment in time.
* You also get `counter` for your value metric that shows the number of times you sent
  your measurements to StatsHouse: +1 for the first second, +1 for the next one, etc.

### User-guided sampling

Though it is better to let StatsHouse sample data for you,
you may want to sample your data before sending them to StatsHouse. 
Use this kind of sampling to control the memory footprint.

In this case, you can explicitly specify `counter` for the `value` metric:
```bash
`{"metrics":[{"name":"my_metric","tags":{},"counter":6, "value":[1, 2, 3]}]}`
```
This means that the number of events is 6, and the values are sampled—as if the original `value` was `[1, 1, 2, 2, 3,
3]`

## Timestamp: sending historical data

StatsHouse writes real-time data as a priority.

:::important
Writing historical data is allowed only for the latest hour and a half.
:::

If the timestamp is in the future, StatsHouse replaces it with the current time.

If the timestamp relates to a moment that is more than 1.5 hours ago, StatsHouse replaces it with the current time 
minus 1.5 hours.

For `cron` jobs that send metric data, use the one-hour sending period:
it is OK to send data once in an hour, but it is not OK to send data once in a day.

