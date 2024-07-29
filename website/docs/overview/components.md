---
sidebar_position: 3
---
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'
import Components from '../img/components.png'
import Agent from '../img/agent.png'
import Aggregator from '../img/aggregator.png'
import ShardsReplicas from '../img/shards-replicas.png'
import RealHist from '../img/real-hist.png'
import OddEven from '../img/odd-even.png'
import IngressProxy from '../img/ingress-proxy.png'
import AgentParameters from '../img/agent-parameters.png'
import Mapping from '../img/mapping.png'
import MappingCached from '../img/mapping-cached.png'

# Components

Find basic StatsHouse components in the picture:

<img src={Components} width="1000"/>

Check the descriptions below:
<!-- TOC -->
* [Agent](#agent)
  * [Receiving data via UDP](#receiving-data-via-udp)
  * [Deploying agents in the Kubernetes pods](#deploying-agents-in-the-kubernetes-pods)
* [Aggregator](#aggregator)
  * [Real-time and "historical" data](#real-time-and-historical-data)
  * [Handling aggregator's shutdown](#handling-aggregators-shutdown)
* [Database](#database)
* [Application programming interface (API)](#application-programming-interface-api)
* [User interface (UI)](#user-interface-ui)
* [Ingress proxy](#ingress-proxy)
* [Metadata](#metadata)
  * [The budget for creating tag values](#the-budget-for-creating-tag-values)
    * [_String top_ tag](#string-top-tag)
    * [_Raw_ tags](#raw-tags)
  * [The budget for creating metrics](#the-budget-for-creating-metrics)
<!-- TOC -->

## Agent

The agent 
* validates metric data (e.g., if a metric exists), 
* [aggregates](concepts.md#aggregation) data over a second, 
* determines how to shard data,
* and sends data to aggregators.

<img src={Agent} width="500"/>

If aggregators are unavailable, the agent stores data on a local disk within the quota and sends it later.

An agent receives metric data via [UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol). Supported formats are
* [JSON](https://www.json.org/json-en.html),
* [Protocol Buffers](https://protobuf.dev),
* [MessagePack](https://msgpack.org),
* [TL](https://core.telegram.org/mtproto/TL).

:::note
Agents support IPv6.
:::

### Receiving data via UDP

StatsHouse receives data via UDP in the MessagePack, Protocol Buffers, JSON, and TL formats—they are semantically 
identical. It automatically detects the format by the first bytes in the packet.

Find the schemes for [TL](https://github.com/VKCOM/statshouse/blob/master/internal/data_model/public.tl), MessagePack,
and [Protocol Buffers](https://github.com/VKCOM/statshouse/blob/master/internal/receiver/statshouse.proto):

<Tabs>

<TabItem value="TL" label="TL">

```
---types---

statshouse.metric#3325d884 fields_mask:#
name:    string
tags:    (dictionary string)
counter: fields_mask.0?double
ts:      fields_mask.4?#               // UNIX timestamp UTC
value:   fields_mask.1?(vector double)
unique:  fields_mask.2?(vector long)

= statshouse.Metric;

---functions---

// for smooth JSON interoperability, first byte of tag must not be 0x5b or 0x7b ("[" or "{")
// for smooth MessagePack interoperability, first byte of tag must be less than 0x80
// for smooth ProtoBuf interoperability, first byte must be as large as possible.

@write statshouse.addMetricsBatch#56580239 fields_mask:# metrics:(vector statshouse.metric) = True;
```
</TabItem>

<TabItem value="MessagePack" label="MessagePack">

```{
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

</TabItem>

<TabItem value="Protocol Buffers" label="Protocol Buffers">

```
syntax = "proto3";
package statshouse;

option go_package = "github.com/vkcom/statshouse/internal/receiver/pb";

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

// to compile
// sudo apt-get install libprotobuf-dev
// go install google.golang.org/protobuf/cmd/protoc-gen-go
// ~/go/src/github.com/vkcom/statshouse$ protoc -I=internal/receiver --go_out=../../../../.. statshouse.proto

// to compile if proto3 format not supported, for example by protocute

// 1. comment out line: option go_package = "github.com/vkcom/statshouse/internal/receiver/pb";
// 2. add
// message MapFieldEntry {
//   optional string key = 1;
//   optional string value = 2;
// }
// 3. replace Metric with
// message Metric {
// string              name    = 1;
// map<string, string> tags    = 2;
// double              counter = 3;
// uint32              ts      = 4;  // UNIX seconds UTC
// repeated double     value   = 5;
// repeated int64      unique  = 6;
// }
// 4. ./protocute --cpp_out=. ~/go/src/github.com/vkcom/statshouse/internal/receiver/statshouse.proto
```

</TabItem>

</Tabs>


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

Сheck the requirements for using formats.
* For TL: the packet body should be the Boxed-serialized `statshouse.addMetricsBatch` object.
* For JSON: the first character should be a curly bracket `{` (to detect the format correctly).
* For Protocol Buffers: do not add fields to the `MetricBatch` object (to detect the format correctly).

### Deploying agents in the Kubernetes pods

Please avoid deploying agents in the Kubernetes pods.
We strongly recommend deploying them on the real servers—make sure to specify the `13337` port.

The number of agents should not be fluctuating.
The agents send per-second reports to the aggregators. The permanent number of agents indicates that the agents are
connected to the aggregators. The pods that stopped working reduce the number of agents and activate the main StatsHouse alert.

<details>
    <summary>Details</summary>
  <p>With the VK RPC protocol, StatsHouse gets the ephemeral connection keys using the remote and local IP addresses
of the connection—as they are shown to the client and the server. 
Routing packets from one adapter to another via firewall makes establishing the connections impossible.
To connect the Kubernetes-like components, create the virtual network adapters and connect
them using the Linux network namespaces.</p>
</details>

## Aggregator

It aggregates per-second metric data from all the agents and inserts the resulting aggregation into the ClickHouse
database.

<img src={Aggregator} width="500"/>

There are as many aggregators as there are ClickHouse shards with replicas. Each aggregator inserts data to its 
local database replica deployed on the same machine. For example: 3 shards × 3 replicas = 9 aggregators.

<img src={ShardsReplicas} width="700"/>

### Real-time and "historical" data

The aggregator has two "workflows":
* for real-time data,
* for "historical" data. The data is "historical" if it has not been sent immediately upon creating.

Inserting real-time data is the top priority for the aggregator.

<img src={RealHist} width="800"/>

Imagine the breakdown situation: it was impossible to insert data for a long time, then the system recovered.
StatsHouse starts to insert real-time data immediately. As for the "historical" data, StatsHouse will insert it as 
soon as possible—if only it does not prevent real-time data from being inserted.

<details>
    <summary>Details</summary>
  <p>Prioritizing real-time data in StatsHouse is related to the ClickHouse database's way of inserting "historical" data, 
which is rather slow.</p>

<p>**Real-time data**</p>

<p>The aggregator allows agents to insert last 5-minute data—this is a "small" inserting window (customizable).
If an agent was not able to insert data in time, it is required to send data as the "historical" one.</p>

<p>The aggregator keeps a container with statistics per each second—such a container aggregates data from the agents.
As soon as the next second data arrives, the aggregator inserts this data (from the "small" window) into the database.
And the agents receive the response with the insert result.</p>

<p>The "small" window extends to the future for 2 seconds. It helps the agents to insert data correctly
if their clock is going too fast.</p>

<p>**"Historical" data**</p>

<p>The aggregator allows agents to insert last 48-hour data—this is a "large" inserting window (customizable).</p>

<p>If the data is older than 48 hours, StatsHouse records meta-statistics, and throws away this piece of data. The agent
receives the `OK` response.</p>

<p>The between-host aggregation is really important, so StatsHouse does its best to make it possible:
<li>each agent makes a request to insert a few tens of "historical" seconds—starting from the "oldest" one;</li>
<li>the aggregator receives these requests and chooses the "oldest" second;</li>
<li>it aggregates data, inserts it into the database and sends the response;</li>
<li>then it chooses the "oldest" second again, etc.</li></p>

<p>This algorithm helps the most distant ("oldest") seconds to come up with the "newest" ones. It makes aggregating
historical data possible and helps to insert data simultaneously.</p>
</details>

### Handling aggregator's shutdown

If the aggregator is unavailable or responds with an error, the agent stores data on a local disk.
Storing data on a disk is limited in bytes. It is also limited in time—within the "large"
(48-hour) inserting window.

<details>
    <summary>Details</summary>
  <p>**Distributing data between the replicas**</p>

  <p>If it is unacceptable or impossible to access the disk, one may run the agent with the empty `--cache-dir` argument.
StatsHouse will not use the disk. The "historical" data will be stored in memory—while the aggregators are
unavailable, i.e., for several minutes.</p>

<p>If the aggregator is unavailable, the agents send data to the rest of the replicas.
The data is distributed according to the seconds' ordinal numbers: the even seconds are sent to one of the replicas,
the odd seconds are sent to another. So the load for both increases by 50%. This is one of the reasons to support
writing data to exactly three ClickHouse replicas.</p>

  <p>**Handling double inserts**</p>

  <p>If the aggregator responds with an error, the agent sends data to another aggregator (another replica) on the other
host. For deduplication, we need a consensus algorithm, which is a rather complicated thing.</p>

  <p>In StatsHouse, the main and the back-up aggregators can insert data from the same agent at the same second.
For this rare case, we track the double inserts via the `__heartbeat_version` meta-metric, which is the _number of
agents sending data at the current second_. To make this meta-metric stable during normal aggregators' operation, 
the agents send data every second—even if there is no real user data at the moment.</p>
</details>

## Database

The [ClickHouse](https://clickhouse.com) database stores [aggregated](concepts.md#aggregation) metric data.

StatsHouse inserts metric data into the ClickHouse table having the following definition:

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

If writing [percentiles](../guides/edit-metrics.md#percentiles) is not enabled for a metric, or the metric is not a 
[_unique counter_](../guides/design-metric.md#unique-counters), the corresponding table columns (`percentiles` or `uniq_state`) are empty.

If a metric is a simple [counter](../guides/design-metric.md#counters), all the columns are empty except the `count` 
one.  The `stag` column is not empty only if a metric has a [String top tag](../guides/design-metric.md#string-top-tag).

To get data for time intervals longer than a second, StatsHouse aggregates data within them and produces per-minute
and per-hour aggregates.

<details>
    <summary>Details</summary>
  <p>Data is distributed across the ClickHouse shards using the hash of `metric, key0, … , key15`.
If a metric has multiple tags, its data related to a particular tag (i.e., `"protocol":"tcp"`) are usually stored on
different shards. To get the full statistics, one should always make _distributed queries_ to the whole set of shards.</p>

  <p>What is the reason for it?
The set of tag values has a certain cardinality: there is a finite number of possible tag value combinations for a
metric. If we reach this cardinality limit, i.e., we send all these tag value combinations, the amount of data stops
increasing due to aggregation—StatsHouse aggregates the events with the same tag value combination.</p>

  <p>To store the sample of data for the whole metric, each shard should store as many rows as there are tag value
combinations for a metric—not a part proportional to a number of shards.</p>

  <p>StatsHouse does not use buffer tables—each aggregator inserts data once per second. Data is inserted into the 
incoming table. The data is filtered by `time` within a receive window (48 hours) and copied via the meterialized 
view. It prevents StatsHouse from inserting the "garbage" data. Otherwise, ClickHouse should have read data not 
from one or two partitions but from all of them.</p>

  <p>A shard should have three or more replicas. Aggregators insert data into the first three replicas.
The rest ones are read-only replicas and may be used to scale the reading load.</p>

  <p>The number of shards can be any. To prevent incorrect configuration and inconsistent sharding, which may lead to a
sharp increase in the amount of data due to weak aggregation, agents send the number of a replica shard to
the aggregator. If the aggregator is not the right recipient for this data, it responds with an error. The same
number helps the _ingress proxy_ to forward data to the right aggregator.</p>
</details>

## Application programming interface (API)

Find StatsHouse [OpenAPI](../guides/openapi.md) specification.

The thin API client allows StatsHouse to send efficient queries to the database.
The service caches data to minimize a database load. We limit retrieving data directly from ClickHouse
as much as possible, since ineffective queries can negatively impact the ClickHouse cluster.

## User interface (UI)

A user interface retrieves data from the StatsHouse API and displays metric data in a graph view.

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

Three ingress proxy instances simulate aggregators. One can set up one more ingress proxy level behind the existing
proxies. This level will use the previous ingress proxies as the aggregators.

Please avoid deploying ingress proxies in the Kubernetes pods.

<details>
    <summary>Details</summary>
  <p>**Cryptokeys**</p>

  <p>StatsHouse uses the VK RPC protocol with the (optional) encryption to connect the components.</p>

  <p>According to the VK RPC protocol, the cryptokey is both the login for getting access and the secret for getting the 
ephemeral connection keys. To establish a connection, the client has to use one of the keys known to a server. 
The central system component is the aggregators. Upon startup, they get the single "major" data center cryptokey.</p>

  <p>To connect to aggregators, the agents should get the parameters:
<li>`-agg-addr`—the addresses of the first aggregators' shards;</li>
<li>`-aes-pwd-file`—the "major" data center cryptokey.</li></p>

  <p>The mechanism above is secure only inside the protected perimeter. To connect from the outside, use the ingress proxy 
installed at the border.</p>

  <p>This ingress proxy at the border has two parts:
<li>an RPC server for the agents to connect from the outside,</li>
<li>an RPC client for the proxy to connect to the aggregators within the perimeter.</li></p>

  <p>For the ingress proxy, one should configure the parameters:
<li>`-ingress-external-addr`—the proxies' external addresses the agents use for connection;</li>
<li>`-ingress-addr`—the parameter to control the interfaces for connecting agents.</li>
<li>`-aes-pwd-file`—the inner cryptokey for sending data to the aggregators,</li>
<li>`-ingress-pwd-dir`—a set of the external keys for the agents from the remote sites.</li></p>

  <p>The `-ingress-addr` parameter is usually `:8128` that is the same as `0.0.0.0:8128`.
It also may contain the subnet address of the network adapter to make it the
only gateway for connections. The port in the `-ingress-addr` parameter should match one of the ports in the
`-ingress-external-addr` parameter. The "outer" part of the ingress proxy should be available to the agents via 
these ports.</p>

  <p>Each of these files contains the cryptokey; the file name is ignored and regarded as a comment.  
The keys have random length—four bytes at least. The first four bytes are for key identification, so they must not 
be identical.</p>

  <p>If the external keys in the directory are changed, restart the ingress proxy. The ingress proxy does not keep track 
of this directory, because the external keys in the set are changed rarely.</p>

  <p>Each agent gets one of the keys from the ingress proxy's `-ingress-pwd-dir` directory as the `-aes-pwd-file` 
parameter.</p>
</details>

## Metadata

The metadata component stores the global `string`↔`int32` map—it maps the metric names and 
the tag values, which are strings, to integers.

StatsHouse is known for providing real-time data. To provide users with low latency, StatsHouse maps the 
`string` tag values (as well as metric names) to `int32` values:
```
    'iphone' <=> 12
    'null' <=> 26
```

This huge `string`↔`int32` map is common for all metrics. The elements of this map are never deleted.

To prevent the uncontrollable increase of the `string`↔`int32` map, the budgets for 
[creating metrics](#the-budget-for-creating-metrics) and [tag values](#the-budget-for-creating-tag-values) are limited.

### The budget for creating tag values

To prevent the uncontrollable increase of the `string`↔`int32` map, the budget for creating tag values is limited to 
300 per day. Upon exceeding the budget, new mappings can be added twice per hour (this rule is customizable).

**Mapping flood** appears when you exceed this budget. When the budget is over and new mappings are not allowed, 
StatsHouse inserts a service `mapping flood` value to a tag column not to lose the entire event.

There are options to use [tags with too many different values](../guides/design-metric.md#how-many-tag-values) 
and to avoid the mapping flood: [String top](../guides/design-metric.md#string-top-tag) tag and 
[Raw](../guides/design-metric.md#raw-tags) tags.

<img src={Mapping} width="600"/>

If you need a tag with many different 32-bit integer values (such as `user_ID`), use the
[Raw](../guides/design-metric.md#raw-tags) tag values to avoid the mapping flood.

For many different string values (such as `search_request`), use a [String top tag](#string-top-tag).

#### _String top_ tag

The _String top tag_ stands apart from the other ones as its values are _not mapped to integers_. It is a separate 
`stag` column in the ClickHouse table:

| timestamp | metric           | tag_1                                                           | tag_2                                                         | <text className="orange-text">tag_s</text>                                  | counter   | sum    | min   | max   | 
|-----------|------------------|-----------------------------------------------------------------|---------------------------------------------------------------|-----------------------------------------------------------------------------|-----------|--------|-------|-------|
| 13:45:05  | toy_packets_size | JSON<br/><text className="orange-text">mapped to `int32`</text> | ok<br/><text className="orange-text">mapped to `int32`</text> | my-tag-value<br/><text className="orange-text">NOT mapped to `int32`</text> | 100       | 1300   | 20    | 1200  | 

As the non-mapped strings take up a lot of space and are longer to read, StatsHouse limits their number (e.g., to a 
hundred). This limit is not configurable for users.

:::important
For these _String top_ tag values, StatsHouse stores only the most frequently used ones—those with the highest 
_counter_. The other tag values for this metric become _empty_ and are aggregated.
:::

For example, the limitation for the non-mapped strings is 4, and we have the following metric data:

| timestamp | metric     | tag_1 | tag_2 | <text className="orange-text">tag_s</text> | counter | sum | min | max |
|-----------|------------|-------|-------|--------------------------------------------|---------|-----|-----|-----|
| 13:45:05  | toy_metric | ...   | ...   | a                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | b                                          | 3       | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | c                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | d                                          | 88      | ... | ... | ... |

The next piece of data adds one more row: with the `e` _String top_ tag value and the counter equal to `55`.
The _String top_ mechanism chooses the tag value with the lowest count (`b` is the less popular one) and makes it 
_empty_:

| timestamp | metric     | tag_1 | tag_2 | <text className="orange-text">tag_s</text> | counter | sum | min | max |
|-----------|------------|-------|-------|--------------------------------------------|---------|-----|-----|-----|
| 13:45:05  | toy_metric | ...   | ...   | a                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | **b** → _empty string_                     | **3**   | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | c                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | d                                          | 88      | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | **e**                                      | **55**  | ... | ... | ... |

The next piece of data adds one more row: with the `f` tag value and the counter equal to `2`.

| timestamp | metric     | tag_1 | tag_2 | <text className="orange-text">tag_s</text> | counter | sum | min | max |
|-----------|------------|-------|-------|--------------------------------------------|---------|-----|-----|-----|
| 13:45:05  | toy_metric | ...   | ...   | a                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | _empty string_                             | 3       | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | c                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | d                                          | 88      | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | e                                          | 55      | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | **f** → _empty string_                     | **2**   | ... | ... | ... |

As the `f` tag value is not in the top of the frequently used ones (i.e., it has the low _count_), it becomes the 
empty string too and is aggregated with the previous _empty string_:

| timestamp | metric     | tag_1 | tag_2 | <text className="orange-text">tag_s</text> | counter | sum | min | max |
|-----------|------------|-------|-------|--------------------------------------------|---------|-----|-----|-----|
| 13:45:05  | toy_metric | ...   | ...   | a                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | _empty string_                             | **3+2** | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | c                                          | 100     | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | d                                          | 88      | ... | ... | ... |
| 13:45:05  | toy_metric | ...   | ...   | e                                          | 55      | ... | ... | ... |

#### _Raw_ tags

If tag values in your metric are originally 32-bit integer values, you can mark them as the _Raw_ ones 
to avoid the mapping flood. 
These _Raw_ tag values will be parsed as `(u)int32` (`-2^31..2^32-1` values are allowed) 
and inserted into the ClickHouse database as is.
Learn how to [set up _Raw_ tags](../guides/edit-metrics.md#set-up-raw-tags).

### The budget for creating metrics

Users can create as many metrics as they wish as soon as they do it manually via the StatsHouse UI.
As a rule, administrators cannot automate creating metrics.

The StatsHouse components rely on the idea that there are not so many different metrics—hundreds of thousands as a
maximum. StatsHouse is not protected from the uncontrollable increase of the metrics' number.

:::tip
If you migrate to StatsHouse from the other monitoring solution, contact the StatsHouse administrators in your
organization to enable the "Auto-create" mode (and to disable it upon migration).
:::

<details>
    <summary>Details</summary>
  <p>**Getting metric properties from metadata**</p>

  <p>Aggregators get information directly from the metadata service. Agents deal with the mappings via the aggregators.
Both the agents and the aggregators cache the mappings in memory or in files—for a month.</p>

  <p>Upon initial startup, the agents use the special bootstrap request to get the 100,000 most frequently used mappings.
Otherwise, while deploying the agents on the 10,000 hosts, StatsHouse should have downloaded a billion values one by
one. It would take a lot of time, and StatsHouse would not be able to write metric data.</p>

  <p>Aggregators use the TL/RPC long polling to get metrics' information from metadata. Similarly, agents use
long polling to get information from aggregators. So, all the agents become informed about the changes in the metrics'
properties almost immediately (in a second).</p>

  <p>**Deleting metrics**</p>

  <p>One cannot delete a metric, because there is no efficient way to do it in the ClickHouse database.
StatsHouse uses the `visible` flag to disable the metric, i.e., to hide the metric from the metric list
(it is reversible). Disabling a metric stops writing data for it to the database.</p>
</details>
