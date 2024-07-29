---
sidebar_position: 3
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Send metric data

In this section, you will find:
<!-- TOC -->
* [How to send data via client libraries](#how-to-send-data-via-client-libraries)
    * ["What if there is no client library for a programming language I need?](#what-if-there-is-no-client-library-for-a-programming-language-i-need)
    * ["What if the existing library does not have the required functionality?"](#what-if-the-existing-library-does-not-have-the-required-functionality)
* [How to send data without client libraries](#how-to-send-data-without-client-libraries)
<!-- TOC -->

:::important
Before sending data, [create a metric](create-metric.md) manually via the UI.
:::

:::warning
Do not send data to someone else's metric as you can spoil the metric data.
:::

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
your attention to the StatsHouse [data model](design-metric.md).

#### "What if the existing library does not have the required functionality?"

The preferred way is to file a [feature request](https://github.com/VKCOM/statshouse/issues) for us on GitHub.

Alternatively, you can prepare a JSON file and send your formatted data to StatsHouse,
but we do not recommend doing this as you won't benefit from aggregation and other native StatsHouse features.

## How to send data without client libraries

For a toy example or testing purposes, you may send data using [Netcat](https://netcat.sourceforge.net):

```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```

See the [Quick start](../quick-start.md#send-data-to-your-metric) for a context.

:::important
We strongly recommend using the [StatsHouse client libraries](#how-to-send-data-via-client-libraries).

Client libraries [aggregate](../overview/concepts.md#aggregation) data before sending them to StatsHouse.
While it may sound counterintuitive, by aggregating, client libraries prevent you from losing data.
Without a client library, you can create a socket, prepare a JSON file, and send your formatted data.
This sounds simple, but only if you have not so much data.

StatsHouse uses [UDP](../overview/components.md#receiving-data-via-udp).
If you send a datagram per event, and there are too many of them,
there is a risk of dropping datagrams due to UDP socket buffer overflow, and no one will notice it.

If you do not use the client library, the non-aggregated data will reach StatsHouse
[agent](../overview/components.md#agent), and the agent will aggregate them anyway.
:::



