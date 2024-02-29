---
sidebar_position: 3
---

import Components from '../img/components.png'
import Agent from '../img/agent.png'
import Aggregator from '../img/aggregator.png'
import ShardsReplicas from '../img/shards-replicas.png'
import RealHist from '../img/real-hist.png'
import OddEven from '../img/odd-even.png'

# Components

Find basic StatsHouse components in the picture and check the descriptions below:

<img src={Components} width="1000"/>

## Agent

The agent 
* **validates** and **interprets** metric data, 
* [**aggregates**](concepts.md#aggregation) data over a second, 
* **shards** it,
* and **sends** to aggregators via the TL/RPC protocol.

<img src={Agent} width="500"/>

If aggregators are unavailable, the agent stores data on a local disk within the **quota** and sends it later.

An agent receives metric data via [UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol). Supported formats are
* [JSON](https://www.json.org/json-en.html),
* [Protocol Buffers](https://protobuf.dev),
* [MessagePack](https://msgpack.org),
* [TL](https://core.telegram.org/mtproto/TL).

The [TL/RPC protocol](https://vkcom.github.io/kphp/kphp-client/tl-schema-and-rpc/) is supported too.

### Receiving data via UDP

StatsHouse receives data via UDP in the MessagePack, Protocol Buffers, JSON, and TL formats—they are semantically 
identical.
It automatically detects the format by the first bytes in the packet. 

A packet is an object with an array of metrics inside:

```
    {"metrics":[ ... ]}

```

Each element in this array is an object with the fields:

```
{
 "name":"rpc_call_latency",  // metric name (obligatory)
 "tags":{"protocol": "tcp"}, // tags
 "ts": 1630000000,           // timestamp; 0 and no timestamp means "now"
 "counter": 6,               // event counter
 "value": [1, 2.0, -3.0],    // values if any (do not use with "unique")
 "unique": [15, 18, -60]     // unique counters if any (do not use with "value")
}
```

For example, one can send a packet like this:

```
{"metrics":[
{"name":"rpc_call_latency",
 "tags":{"protocol": "tcp"},
 "value": [15, 18, 60]},
{"name": "rpc_call_errors",
 "tags":{"protocol": "udp","error_code": "-3000"},
 "counter": 5}
]}
{"metrics":[
{"name": "external_landings",
 "tags":{"country": "ru","gender": "m","skey": "lenta.ru"},
 "counter": 1}
]}
```

The schema for Protocol Buffers:

```
message Metric {
  string              name    = 1;
  map<string, string> tags    = 2;
  double              counter = 3;
  uint32              ts      = 4;  // UNIX seconds UTC
  repeated double     value   = 5;
  repeated int64      unique  = 6;
}

message MetricBatch {
  repeated Metric metrics = 13337;  // to autodetect packet format by first bytes
}
```

Сheck the requirements for using formats:

| Format           | Requirement                                                                                                                                                                                                                  |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| TL               | The packet body should be the [Boxed](https://docs.ton.org/develop/data-formats/tl#non-obvious-serialization-rules)-serialized `statshouse.addMetricsBatch` object<br/>(see the [schema](#receiving-data-via-tl-rpc) below). |
| JSON             | The first character should be a curly bracket `{` for correct format autodetection.                                                                                                                                          |
| Protocol Buffers | Do not add fields to the `MetricBatch` object for correct format autodetection.                                                                                                                                              |

#### UDP socket buffer overflow

Without a client library, you can create a socket, prepare a JSON file, and send your formatted data.
This sounds simple, but only if you have not so much data.

StatsHouse uses UDP. If you send a datagram per event, and there are too many of them,
there is a risk of dropping datagrams, and no one will notice it.

If you do not use the client library, the non-aggregated data will reach the StatsHouse
agent, and the agent will aggregate them anyway.

### Receiving data via TL/RPC

For the [TL/RPC protocol](https://vkcom.github.io/kphp/kphp-client/tl-schema-and-rpc/):

```
statshouse.metric#3325d884 fields_mask:#
  name:    string
  tags:    (dictionary string)
  counter: fields_mask.0?double
  ts:      fields_mask.4?#               // UNIX timestamp UTC
  value:   fields_mask.1?(vector double)
  unique:  fields_mask.2?(vector long)

= statshouse.Metric;

@write statshouse.addMetricsBatch#56580239 fields_mask:#
  metrics:(vector statshouse.metric)
= True;

```

Receive errors are returned as TL errors. We do not specify the error codes as we assume only logging and manual 
processing for the errors—with no error processing on a client.

### Receiving data via Unix datagram socket or TCP

To track if the sender drops packets or not, use non-blocking sending for the Unix datagram sockets instead of UDP 
(if possible).

For the same purpose, the clients such as the PHP client can use the non-blocking TCP/Unix socket for the same 
purpose—not to be blocked because of the socket buffer overflow. If the packet does not fit the buffer, the 
non-fitting part is cached. This part is sent as soon as the buffer becomes free. The entire new non-fitting packets 
are thrown away and the related counters increase.

## Aggregator

It aggregates per-second metric data from all the agents and inserts the resulting aggregation into the ClickHouse
database.

<img src={Aggregator} width="500"/>

There are as many aggregators as there are ClickHouse shards with replicas.
Each aggregator inserts data to its local database replica deployed on the same machine.

<img src={ShardsReplicas} width="700"/>

The aggregator has two entry points:
* for real-time data,
* for "historical" data. The data is "historical" if it has not been sent immediately upon creating.

<img src={RealHist} width="800"/>

:::important
Inserting real-time data is the top priority for the aggregator.
:::

Imagine the breakdown situation: it was impossible to insert data for a long time, then the system recovered.

StatsHouse starts to insert real-time data immediately. As for the "historical" data, StatsHouse will insert it as 
soon as possible—if only it does not prevent real-time data from being inserted.

### "Small" inserting window

The aggregator allows agents to insert last 5-minute data—this is a "small" inserting window (customizable).
If an agent was not able to insert data in time, it is required to send data as the "historical" one. 

The aggregator keeps a container with statistics per each second—such a container aggregates data from the agents.
As soon as the next second data arrives, the aggregator inserts this data (from the "small" window) into the database.
And the agents receive the response with the insert result.

The "small" window extends to the future for 2 seconds. It helps the agents to insert data correctly 
if their clock is going too fast.

### "Large" inserting window

The aggregator allows agents to insert last 48-hour data—this is a "large" inserting window (customizable).

If the data is older than 48 hours, StatsHouse records meta-statistics, and throws away this piece of data. The agent 
receives the `OK` response.

The between-host aggregation is really important, so StatsHouse does its best to make it possible:
* Each agent makes a request to insert a few tens of "historical" seconds—starting from the "oldest" one.
* The aggregator receives these requests and chooses the "oldest" second.
* It aggregates data, inserts it into the database and sends the response.
* Then it chooses the "oldest" second again, etc.

This algorithm helps the most distant ("oldest") seconds to come up with the "newest" ones. It makes aggregating 
historical data possible and helps to insert data simultaneously.

### Time correction

Each shard's replica uses time correction: from -1 to 0 seconds depending on the replica's number. It helps to 
prevent sending data from the agents like an avalanche—when switching from one second to another. Otherwise, there 
is a risk of dropping many packets.

When the host is in the sleep mode or hangs up, the agent detects time difference—more than for one second.
In this case, the agent sends this difference in a special field, so that the aggregators could take it into account 
using the special meta-metric.

### Handling aggregator's shutdown

If the aggregator is unavailable or responds with an error, the agent stores data on a local disk.
Storing data on a disk is limited in bytes. It is also limited in time—within the ["large"](#large-inserting-window)
(48-hour) inserting window—as the aggregator still does not receive the older data.

If it is unacceptable or impossible to access the disk, one may run the agent with the empty `--cache-dir` argument. 
StatsHouse will not use the disk. The "historical" data will be stored in memory—while the aggregators are 
unavailable, i.e., for several minutes.

If the aggregator is unavailable, the agents send data to the rest of the replicas.
The data is distributed according to the seconds' ordinal numbers: the even seconds are sent to one of the replicas, 
the odd seconds are sent to another. So the load for both increases by 50%. This is one of the reasons to support 
writing data to exactly three ClickHouse replicas.

<img src={OddEven} width="500"/>

### Handling double inserts

If the aggregator responds with an error, the agent sends data to another aggregator (another replica) on the other 
host. For deduplication, we need a consensus algorithm, which is a rather complicated thing.

In StatsHouse, the main and the back-up aggregators can insert data from the same agent at the same second.
For this rare case, we track the double inserts via the `__heartbeat_version` meta-metric, which is the _number of 
agents sending data at the current second_.
To make this meta-metric stable during normal aggregators' operation, the agents send data every second—even if 
there is no real user data at the moment.

## Database

The [ClickHouse](https://clickhouse.com) database stores [aggregated](concepts.md#aggregation) metric data.

The database is responsible for replication and downsampling.

:::important
Things you need to know about the StatsHouse database:
* it has only three tables, 
* it allows each metric to have only 16 tags, 
* it maps strings to integers.
:::

### Table structure

StatsHouse inserts all the metric data of any type into the same ClickHouse table having the following definition:

```
CREATE TABLE statshouse2_value_1s (
    `time`           DateTime,
    `metric`         Int32,
    `tag0`           Int32,
    `tag1`           Int32,
...
    `tag15`          Int32,
    `stag`           String,
    `count`          SimpleAggregateFunction(sum, Float64),
    `min`            SimpleAggregateFunction(min, Float64),
    `max`            SimpleAggregateFunction(max, Float64),
    `sum`            SimpleAggregateFunction(sum, Float64),
    `max_host`       AggregateFunction(argMax, Int32, Float32), 
    `percentiles`    AggregateFunction(quantilesTDigest(0.5), Float32),
    `uniq_state`     AggregateFunction(uniq, Int64)
) ENGINE = *MergeTree
PARTITION BY toDate(time) ORDER BY (metric, time,tag0,tag1, ...,tag15, stag);
```
:::note
If writing [percentiles](../guides/edit-metrics.md#percentiles) is not enabled for a metric, or the metric is not a 
[_unique counter_](../guides/design-metric.md#unique-counters), the corresponding table columns 
(`percentiles` or `uniq_state`) are empty.

If a metric is a simple [counter](../guides/design-metric.md#counters), all the columns are empty except the `count` 
one.

The `stag` column is not empty only if a metric has a [String top tag](../guides/design-metric.md#string-tag).
:::

## Data access service (API)

## User interface

## Ingress proxy

### Cryptokeys

### A proxy behind the proxy

### Kubernetes-related questions

## Metadata

### Mapping budget

Tag values are usually `string` values and appear repeatedly. For higher efficiency, StatsHouse maps all of them to
`int32` values. This huge `string`↔`int32` map is common for all metrics. The elements of this map are never deleted.
To prevent the uncontrollable increase of the map, the budget for creating new mappings is limited to 300.
Upon exceeding the budget, new mappings can be added once per hour (this rule is customizable).

Mapping flood appears when you exceed this budget.

Then the budget is over and new mappings are not allowed, StatsHouse inserts a service `mapping flood` value to a
tag column not to lose the entire event.
