---
sidebar_position: 3
---

import Components from '../img/components.png'
import Agent from '../img/agent.png'
import Aggregator from '../img/aggregator.png'
import ShardsReplicas from '../img/shards-replicas.png'
import RealHist from '../img/real-hist.png'
import OddEven from '../img/odd-even.png'
import IngressProxy from '../img/ingress-proxy.png'
import AgentParameters from '../img/agent-parameters.png'
import IngressProxyParameters from '../img/ingress-proxy-parameters.png'
import Mapping from '../img/mapping.png'
import MappingCached from '../img/mapping-cached.png'

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

To get data for time intervals longer than a second, StatsHouse aggregates data within them and produces per-minute
and per-hour aggregates.

Data is distributed across the ClickHouse shards using the hash of `metric, key0, … , key15`.
If a metric has multiple tags, its data related to a particular tag (i.e., `"protocol":"tcp"`) are usually stored on
different shards. To get the full statistics, one should always make _distributed queries_ to the whole set of shards.

What is the reason for it?
The set of tag values has a certain cardinality: there is a finite number of possible tag value combinations for a
metric. If we reach this cardinality limit, i.e., we send all these tag value combinations, the amount of data stops
increasing due to [aggregation](concepts.md#aggregation)—StatsHouse aggregates the events with the same tag value
combination.

To store the sample of data for the whole metric, each shard should store as many rows as there are tag value
combinations for a metric—not a part proportional to a number of shards.

StatsHouse does not use buffer tables—each aggregator inserts data once per second. Data is inserted into the 
incoming table. The data is filtered by `time` within a receive window (48 hours) and copied via the meterialized 
view. It prevents StatsHouse from inserting the "garbage" data. Otherwise, ClickHouse should have read data not 
from one or two partitions but from all of them.

A shard should have three or more replicas. Aggregators insert data into the first three ones.
The rest ones are read-only replicas and may be used to scale the reading load.

The number of shards can be any. To prevent incorrect configuration and inconsistent sharding, which may lead to a 
sharp increase in the amount of data due to weak aggregation, agents send the number of a replica shard to 
the aggregator. If the aggregator is not the right recipient for this data, it responds with an error. The same 
number helps the _ingress proxy_ to forward data to the right aggregator.

## Application programming interface (API)

Find StatsHouse [OpenAPI](../guides/openapi.md) specification.

The thin API client allows StatsHouse to send efficient queries to the database.
The service caches data to minimize a database load. We limit retrieving data directly from ClickHouse
as much as possible, since ineffective queries can negatively impact the ClickHouse cluster.

## User interface (UI)

A user interface retrieves data from `statshouse-api` and displays metric data in a graph view.

## Ingress proxy

An ingress proxy receives data from the agents that live outside the protected perimeter
(i.e., outside the data center) and sends it to the aggregators.

Agents and aggregators use the TL/RPC protocol with the data center encryption key. So, the agents outside the 
data center cannot connect to aggregators directly, because it would require disclosing or copying the key to the 
external systems.

The ingress proxy has a separate set of encryption keys for the external connections. To revoke the encryption 
key, one should delete it from the ingress proxy configuration.

Ingress proxy does not have a state. To reduce the likelihood of an attack, it proxies only the subset of TL/RPC 
request types used by the aggregators.

There should be exactly three ingress proxies. Each of them is a proxy to a corresponding replica of a shard. 
If the proxy is unavailable due to service or shutdown, it is equivalent to a breakdown of one shard's replica and 
does not affect the normal operation of the StatsHouse system.

<img src={IngressProxy} width="600"/>

### Cryptokeys

StatsHouse uses the VK RPC protocol with the (optional) encryption to connect the components.

According to the VK RPC protocol, the cryptokey is both the login for getting access and the secret for getting the 
ephemeral connection keys. To establish a connection, the client has to use one of the keys known to a server. 
The central system component is the aggregators. Upon startup, they get the single "major" data center cryptokey.

To connect to aggregators, the agents should get the parameters:
* `-agg-addr=X`—the addresses of the first aggregators' shards;
* `-aes-pwd-file=X`—the "major" data center cryptokey.

<img src={AgentParameters} width="300"/>

The mechanism above is secure only inside the protected perimeter. To connect from the outside, use the ingress proxy 
installed at the border.

This ingress proxy at the border has two parts:
* an RPC server for the agents to connect from the outside,
* an RPC client for the proxy to connect to the aggregators within the perimeter.

For the ingress proxy, one should configure the parameters:
* `-ingress-external-addr`—the proxies' external addresses the agents use for connection;
* `-ingress-addr=X`—the parameter to control the interfaces for connecting agents.
The `-ingress-addr=X` parameter is usually `:8128` that is the same as `0.0.0.0:8128`.
It also may contain the subnet address of the network adapter to make it the 
only gateway for connections. The port in the `-ingress-addr=X` parameter should match one of the ports in the 
`-ingress-external-addr` parameter. The "outer" part of the ingress proxy should be available to the agents via these ports. 
* `-aes-pwd-file=X`—the inner cryptokey for sending data to the aggregators,
* `-ingress-pwd-dir=X`—a set of the external keys for the agents from the remote sites.

<img src={IngressProxyParameters} width="500"/>

This file contains the cryptokey; the file name is ignored and regarded as a comment.  
The keys have random length—four bytes at least. The first four bytes are for key identification, so they must not 
be identical.

If the external keys in the directory are changed, restart the ingress proxy. The ingress proxy does not keep track 
of this directory, because the external keys in the set are changed rarely.

Each agent gets one of the keys from the ingress proxy's `-ingress-pwd-dir=X` directory as the `-aes-pwd-file=X` 
parameter.

### A proxy behind the proxy

Three ingress proxy instances simulate aggregators. One can set up one more ingress proxy level behind the existing 
proxies. This level will use the previous ingress proxies as the aggregators.

For the second proxy level, provide the parameters:
* `-agg-addr=X`—three addresses from the `-ingress-external-addr=X` parameter of the previous ingress proxy level,
* `-aes-pwd-file=X`—one of the cryptokeys from the `-ingress-pwd-dir=X` directory of the previous ingress proxy level.

### Kubernetes-related questions

With the VK RPC protocol, StatsHouse gets the ephemeral connection keys using the remote and local IP addresses 
of the connection—as they are shown to the client and the server.

Routing packets from one adapter to another via firewall makes establishing the connections impossible.

To connect the [Kubernetes](https://kubernetes.io)-like components, create the virtual network adapters and connect 
them using the [Linux network namespaces](https://lwn.net/Articles/580893/).

:::important
Please avoid deploying agents and the ingress proxies in the Kubernetes pods.
We strongly recommend deploying them on the server—make sure to specify the `13337` port.

The reason is that StatsHouse "does not like" the fluctuating number of agents. 
The agents send per-second reports to the aggregators. The permanent number of agents indicates that the agents are 
connected to the aggregators. The pods that stopped working reduce the number of agents and activate the main StatsHouse alert.
:::

## Metadata

A metadata service stores a list of metrics and their properties:
* the metric type (to show the metric correctly in the UI),
* the metric name, 
* tag names, 
* tags interpretation.

We usually deploy this service on the aggregators' first shards. 
The load on this service is not so high—there is no need to deploy it on a separate host.

### The budget for creating metrics

:::important
The users can create as many metrics as they wish as soon as they do it manually via the StatsHouse UI.

As a rule, the administrators cannot automate creating metrics. 
:::

The StatsHouse components rely on the idea that there are not so many different metrics—hundreds of thousands as a 
maximum. StatsHouse is not protected from the uncontrollable increase of the metrics' number.

:::tip
If you migrate to StatsHouse from the other monitoring solution, contact the StatsHouse administrators in your 
organization to enable the "Auto-create" mode (and to disable it upon migration).
:::

### Getting metric properties from metadata

Aggregators use the TL/RPC long polling to get metrics' information from metadata. Similarly, agents use 
long polling to get information from aggregators. So, all the agents become informed about the changes in the metrics' 
properties almost immediately (in a second).

### Deleting metrics

One cannot delete a metric, because there is no efficient way to do it in the ClickHouse database.
StatsHouse uses the `visible` flag to disable the metric, i.e., to hide the metric from the metric list 
(it is reversible). Disabling a metric stops writing data for it to the database.

### The mapping mechanism

StatsHouse is known for providing real-time data. To provide users with low latency, StatsHouse maps the 
`string` tag values (as well as metric names) to `int32` values:
```
    'iphone' <=> 12
    'null' <=> 26
```

This huge `string`↔`int32` map is global and common for all metrics. The elements of this map are never deleted.

Aggregators get information directly from the metadata service. Agents deal with the mappings via the aggregators.
Both the agents and the aggregators cache the mappings in memory or in files—for a month.

<img src={MappingCached} width="700"/>

Upon initial startup, the agents use the special bootstrap request to get the 100,000 most frequently used mappings.
Otherwise, while deploying the agents on the 10,000 hosts, StatsHouse should have downloaded a billion values one by 
one. It would take a lot of time, and StatsHouse would not be able to write metric data.

#### The budget for creating mappings

To prevent the uncontrollable increase of the `string`↔`int32` map, the budget for creating mappings is limited to 300.
Upon exceeding the budget, new mappings can be added once per hour (this rule is customizable).

**Mapping flood** appears when you exceed this budget. When the budget is over and new mappings are not allowed, 
StatsHouse inserts a service `mapping flood` value to a tag column not to lose the entire event.

There are options to use [tags with many different values](../guides/design-metric.md#how-many-tag-values) 
and to avoid the mapping flood: [String top](../guides/design-metric.md#string-top-tag) tag and 
[Raw](../guides/design-metric.md#raw-tags) tags.

<img src={Mapping} width="600"/>

If you need a tag with many different 32-bit integer values (such as `user_ID`), use the
[Raw](../guides/design-metric.md#raw-tags) tag values to avoid the mapping flood.

For many different string values (such as `search_request`), use a [String top tag](#string-top-tag).

#### _String top_ tag

The _String top tag_ stands apart from the other ones as its values are _not mapped to integers_. It is a separate 
`stag` column in the ClickHouse table:

| timestamp | metric           | tag_1                                                           | tag_2                                                         | counter | sum   | min | max  | <text className="orange-text">stag</text>                                   |
|-----------|------------------|-----------------------------------------------------------------|---------------------------------------------------------------|---------|-------|-----|------|-----------------------------------------------------------------------------|
| 13:45:05  | toy_packets_size | JSON<br/><text className="orange-text">mapped to `int32`</text> | ok<br/><text className="orange-text">mapped to `int32`</text> | 100     | 13000 | 20  | 1200 | my-tag-value<br/><text className="orange-text">NOT mapped to `int32`</text> |

As the non-mapped strings take up a lot of space and are longer to read, StatsHouse limits their number (e.g., to a 
hundred). This limit is not configurable for users.

:::important
For these _String top_ tag values, StatsHouse stores only the most frequently used ones—those with the highest 
_counter_. The other tag values for this metric become _empty_ and are aggregated.
:::

For example, the limitation for the non-mapped strings is 4, and we have the following metric data:

| timestamp | metric     | tag_1 | tag_2 | counter | sum | min | max | stag |
|-----------|------------|-------|-------|---------|-----|-----|-----|------|
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | a    |
| 13:45:05  | toy_metric | ...   | ...   | 3       | ... | ... | ... | b    |
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | c    |
| 13:45:05  | toy_metric | ...   | ...   | 88      | ... | ... | ... | d    |

The next piece of data adds one more row: with the `e` _String top_ tag value and the counter equal to `5`.
The _String top_ mechanism chooses the tag value with the lowest count (`b` is the less popular one) and makes it 
_empty_:

| timestamp | metric     | tag_1 | tag_2 | counter | sum | min | max | stag                   |
|-----------|------------|-------|-------|---------|-----|-----|-----|------------------------|
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | a                      |
| 13:45:05  | toy_metric | ...   | ...   | **3**   | ... | ... | ... | **b** → _empty string_ |
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | c                      |
| 13:45:05  | toy_metric | ...   | ...   | 88      | ... | ... | ... | d                      |
| 13:45:05  | toy_metric | ...   | ...   | **55**  | ... | ... | ... | **e**                  |

The next piece of data adds one more row: with the `f` tag value and the counter equal to `2`.

| timestamp | metric     | tag_1 | tag_2 | counter | sum | min | max | stag                    |
|-----------|------------|-------|-------|---------|-----|-----|-----|-------------------------|
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | a                       |
| 13:45:05  | toy_metric | ...   | ...   | 3       | ... | ... | ... | _empty string_          |
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | c                       |
| 13:45:05  | toy_metric | ...   | ...   | 88      | ... | ... | ... | d                       |
| 13:45:05  | toy_metric | ...   | ...   | 55      | ... | ... | ... | e                       |
| 13:45:05  | toy_metric | ...   | ...   | **2**   | ... | ... | ... | **f** → _empty string_  |

As the `f` tag value is not in the top of the frequently used ones (i.e., it has the low _count_), it becomes the 
empty string too and is aggregated with the previous _empty string_:

| timestamp | metric     | tag_1 | tag_2 | counter | sum | min | max | stag                    |
|-----------|------------|-------|-------|---------|-----|-----|-----|-------------------------|
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | a                       |
| 13:45:05  | toy_metric | ...   | ...   | **3+2** | ... | ... | ... | _empty string_          |
| 13:45:05  | toy_metric | ...   | ...   | 100     | ... | ... | ... | c                       |
| 13:45:05  | toy_metric | ...   | ...   | 88      | ... | ... | ... | d                       |
| 13:45:05  | toy_metric | ...   | ...   | 55      | ... | ... | ... | e                       |

#### _Raw_ tags

If tag values in your metric are originally 32-bit integer values, you can mark them as the _Raw_ ones 
to avoid the mapping flood. 
These _Raw_ tag values will be parsed as `(u)int32` (`-2^31..2^32-1` values are allowed) 
and inserted into the ClickHouse database as is.
Learn how to [set up _Raw_ tags](../guides/edit-metrics.md#set-up-raw-tags).
