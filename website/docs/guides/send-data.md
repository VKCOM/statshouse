---
sidebar_position: 3
description: TEST
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import MetricTypes from '../img/metric-types.png'

# Send metric data

:::important
Before sending data, [create a metric](create-metric.md) manually 
via the UI.
:::

:::tip
Make sure you understand [metric types](#understand-metric-types),
and how to choose [proper tags](#choose-proper-tags).
:::

:::warning
Do not send data to someone else's metric as you can spoil the metric data.
:::

As soon as your metric has a name, you are almost ready to start sending data. 

## Use client libraries

StatsHouse provides you with client libraries to send metric data from your code:
- [Go](https://github.com/VKCOM/statshouse-go)
- [PHP](https://github.com/VKCOM/statshouse-php)
- [C++](https://github.com/VKCOM/statshouse-cpp)
- [Java](https://github.com/VKCOM/statshouse-java)
- [Python](https://github.com/VKCOM/statshouse-py)

There is also a special module for using StatsHouse with 
[nginx](https://github.com/VKCOM/nginx-statshouse-module).

### How to use the client libraries

Below are the simple code examples using some of these libraries. 
Prior to copying and pasting the code, install the library you need using recommendations 
from the corresponding README file, [for example](https://github.com/VKCOM/statshouse-py/blob/master/README.md):
```Python
pip install statshouse # install a StatsHouse library for Python
```
Or, for a [StatsHouse library for C++](https://github.com/VKCOM/statshouse-cpp):
```shell
git clone https://github.com/VKCOM/statshouse-cpp.git
```
Upon importing or including the library in your module, 
choose the appropriate method to send metric data. It depends on the [metric type](#understand-metric-types) 
and is probably named as `count`, `value`, or `unique`—see the Python code for an example.

<Tabs>
<TabItem value="py" label="Python">
```py
import statshouse

statshouse.count("my_metric", {}, 0.42)

statshouse.value("my_metric", {}, 0.42)

statshouse.unique("my_metric", {}, 0.42)
```
</TabItem>
<TabItem value="cpp" label="C++">
```cpp
#include "statshouse.hpp"
#include <cstdio>

using namespace statshouse;

Registry r{{
    logger: puts // debug output
}};

int main() {
    // Write "value" metric
    auto v = r.metric("demo_value_metric")
        .tag("1", "foo")
        .tag("2", "bar")
        .event_metric_ref();

    v.write_value(42.5);
    return 0;
}

}
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

### Why to use the client libraries

Client libraries [aggregate](../conceptual-overview.md#aggregation) data before sending them to StatsHouse.
While it may sound counterintuitive, by aggregating, client libraries prevent you from losing data.

Without a client library, you can create a socket, prepare a JSON file, and send your formatted data. 
This sounds simple, but only if you have not so much data. 

StatsHouse uses [UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol). 
If you send a datagram per event, and there are too many of them, 
there is a risk of dropping datagrams, and no one will notice it.

If you do not use the client library, the non-aggregated data will reach StatsHouse 
[agent](../conceptual-overview.md#agent), and the agent will aggregate them anyway.

## Understand metric types

:::important
A metric type affects the range of
[descriptive statistics](view-graph#desriptive-statistics-available-for-a-metric) available for your metric to view
and analyze. For example, percentiles are available for _values_ only.
Or you cannot view the cumulative graph for _uniques_.

See more about [enabling percentiles](#enable-percentiles)
and [showing the proper descriptive statistics](#aggregation) in the UI.
:::

### What is a metric type?

A metric is a [system for measuring something](https://dictionary.cambridge.org/dictionary/english/metric)—it is 
_how you measure_ the things you are interested in. The metric type affects the range of mathematical 
operations allowed to your metric data.

You can measure same things in different ways. To evaluate service availability, you can count the
number of handled requests, which is just a number of events; or you can measure processing time for these requests,
which is the value accompanying each event (each request).

StatsHouse is a powerful tool, but it cannot decide for you, so you should clearly understand what you want to
measure, and how you will measure it.

:::note
Metric types should not be confused with [data types](https://en.wikipedia.org/wiki/Data_type) in programming
languages. You should not specify the type of your data: whether it is `int`, `float`, or `double`, etc.
:::

### Metric types in StatsHouse

With StatsHouse, you can use three basic metric types.

<img src={MetricTypes} width="800"/>

See the table below for definitions and examples:

| Metric type | What does it measure?                                                                     | Examples                                                                                                                                        |
|-------------|-------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Counter     | It counts the number of times an event has occurred.                                      | The number of API method calls<br/>The number of requests to a server<br/>The number of errors received while sending messages                  |
| Value       | It measures magnitude of a parameter.<br/>A measurement event itself is counted as well.  | How long does it take <br/>for a service to generate a newsfeed?<br/>What is CPU usage for this host?<br/>What is the response size (in bytes)? |
| Unique      | It counts the number of unique events.<br/>The total number of events is counted as well. | The number of unique users who sent packages to a service                                                                                       |

### Built-in _counters_ for _values_ and _uniques_

Value and unique metrics have an ordinary counter inside, so you should not implement your own counters
for these metric types. Imagine you measuring a value metric (e.g. the response size in bytes) once in a second:
* You get the "value level" that is your parameter magnitude:
  1024 bytes, then 2048 bytes, etc. Please note that this "level" is [aggregation](../conceptual-overview.md#aggregation), not 
  an exact value for a particular moment in time.
* You also get the "counter" for your value metric that shows the number of times you performed
  your measurements: +1 for the first second, +1 for the next one, etc.

The same applies to unique metrics: they provide you with the number of unique events and the total number of events.

See more on [changing or combining metric types](#changing-or-combining-metric-types)
and [user-guided sampling](#user-guided-sampling).

### How to set up metric types

Specify a [metric type](#metric-type) in your sending requests.

For a toy example, you may send your metric data with one of these `bash` scripts:
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"value":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```
where `counter` or `value` are the metric types.

Or, as you have already seen in the tab with the Python code example, you can 
choose one of the methods according to your metric type:
```Python
statshouse.count("my_metric", {}, 0.42)
statshouse.value("my_metric", {}, 0.42)
statshouse.unique("my_metric", {}, 0.42)
```

### Changing or combining metric types

:::important
Keep sending data of the **same type per metric**.
For example, do not switch between sending _values_ and _uniques_ for the same metric.
:::

Some users want to create "one big metric" for the whole system and to differentiate
subsystems using `tag 1`.
So, they use different metric types for different combinations of tag values.
They set the metric type to `mixed`, allowing StatsHouse to write and display 
all the metric types for these data.  

We recommend avoiding such a design for your metric. You will not be able to set tag descriptions 
as they would probably depend on `tag 1`. This "one big metric" will also get the common
[sampling](../conceptual-overview.md#sampling) factor: data for the whole metric with all the subsystem data inside 
will be sampled.

:::tip
Alternatively, create a separate metric with a particular metric type for each subsystem, 
so that you could use tags and avoid massive sampling.
:::

### User-guided sampling

Though it is better to let StatsHouse sample data for you,
you may want to sample your data before sending them to StatsHouse.

In this case, you can explicitly specify `counter` for the `value` metric:
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"value":1000,"counter":100}]}' | nc -q 1 -u 127.0.0.1 13337
```

## Choose proper tags

