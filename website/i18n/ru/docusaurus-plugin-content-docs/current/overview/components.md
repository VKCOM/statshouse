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

# Компоненты

Основные компоненты StatsHouse изображены на рисунке:

<img src={Components} width="1000"/>

А вот их описания:
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

## Агент

Агент
* валидирует метрику (например, проверяет, существует ли она),
* [агрегирует](concepts.md#aggregation) данные в пределах секунды,
* шардирует данные
* и отправляет их на агрегаторы.

<img src={Agent} width="500"/>

Если агрегаторы недоступны, агент хранит данные на локальном диске в пределах квоты и отправляет их позже.

An agent receives metric data via [UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol). Supported formats are
* [JSON](https://www.json.org/json-en.html),
* [Protocol Buffers](https://protobuf.dev),
* [MessagePack](https://msgpack.org),
* [TL](https://core.telegram.org/mtproto/TL).


Агент получает данные по протоколу [UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol). Поддерживаются 
следующие форматы:
* [JSON](https://www.json.org/json-en.html),
* [Protocol Buffers](https://protobuf.dev),
* [MessagePack](https://msgpack.org),
* [TL](https://core.telegram.org/mtproto/TL).

:::note
Агенты поддерживают IPv6.
:::

### Получение данных по UDP

StatsHouse receives data via UDP in the MessagePack, Protocol Buffers, JSON, and TL formats—they are semantically 
identical. It automatically detects the format by the first bytes in the packet.

StatsHouse получает данные по UDP в форматах MessagePack, Protocol Buffers, JSON и TL — они семантически
идентичны. Формат определяется автоматически по первым байтам в пакете.

Посмотрите схемы для [TL](https://github.com/VKCOM/statshouse/blob/master/internal/data_model/public.tl), 
MessagePack и [Protocol Buffers](https://github.com/VKCOM/statshouse/blob/master/internal/receiver/statshouse.proto):

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

Пакет — это объект, содержащий массив метрик:

```
    {"metrics":[ ... ]}

```

Каждый элемент этого массива представляет собой объект с полями:

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

Например, можно отправить такой пакет:

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

Обратите внимание на требования к использованию форматов.
* Для TL: тело пакета должно быть Boxed-сериализацией объекта `statshouse.addMetricsBatch`.
* Для JSON: первым символом должна быть фигурная скобка `{` (для корректного определения формата).
* Для Protocol Buffers: не добавляйте поля в объект `MetricBatch` (для корректного определения формата).

### Разворачивание агентов в подах Kubernetes

Не устанавливайте агенты в подах Kubernetes.
Мы настоятельно рекомендуем устанавливать их только на реальных серверах (обязательно указывайте порт `13337`).

The reason is that StatsHouse "does not like" the fluctuating number of agents.
The agents send per-second reports to the aggregators. The permanent number of agents indicates that the agents are
connected to the aggregators. The pods that stopped working reduce the number of agents and activate the main StatsHouse alert.

StatsHouse "не любит", когда число агентов колеблется.
Агенты отправляют агрегаторам посекундные отчёты. Если число агентов постоянно, значит, они подключены к 
агрегаторам. Из-за остановки подов число агентов уменьшается, и срабатываюет главный алерт StatsHouse.

<details>
    <summary>Details</summary>
  <p>При шифровании в VK RPC для вывода эфемерных ключей используются удалённый и локальный IP-адреса соединений (как их 
видят клиент и сервер).
Маршрутизация пакетов от одного адаптера к другому через брандмауэр делает установление соединений невозможным.
Для соединения компонентов в таком случае понадобится создать виртуальные сетевые адаптеры и соединить
их средствами Linux network namespaces.</p>
</details>

## Агрегатор

Он агрегирует посекундные данные метрик от всех агентов и вставляет результат в базу данных ClickHouse.

<img src={Aggregator} width="500"/>

There are as many aggregators as there are ClickHouse shards with replicas. Each aggregator inserts data to its 
local database replica deployed on the same machine. For example: 3 shards × 3 replicas = 9 aggregators.

Агрегаторов должно быть столько, сколько имеется шардов ClickHouse с репликами. Каждый агрегатор вставляет данные в 
реплику базы данных, развёрнутую на той же машине. Например: 3 шарда × 3 реплики = 9 агрегаторов.

<img src={ShardsReplicas} width="700"/>

### Актуальные и "исторические" данные

У агрегатора есть два режима работы:
* работа с актуальными данными,
* работа с "историческими" данными. Данные считаются "историческими", если их не удалось отправить сразу после создания.

Вставка актуальных данных — приоритет для агрегатора.

<img src={RealHist} width="800"/>

Представьте: возник сбой. Долгое время вставить данные было невозможно, затем система восстановилась.
StatsHouse немедленно начинает вставлять актуальные данные. А вот "исторические" данные StatsHouse вставит при 
первой возможности, если только это не помешает вставке актуальных данных.

<details>
    <summary>Подробнее</summary>
  <p>Вставка актуальных данных имеет приоритет, поскольку "исторические" данные вставляются в базу ClickHouse 
довольно медленно.</p>

<p>**Актуальные данные**</p>

<p>Агрегатор позволяет агентам вставлять данные за последние 5 минут — это "короткое" окно вставки (его можно 
настроить). Если агент не успел вставить данные вовремя, он отправит данные как "исторические".</p>

<p>Для каждой актуальной секунды агрегатор хранит контейнер со статистикой. В этот контейнер агрегируются данные от 
агентов. Как только наступает следующая секунда, агрегатор вставляет данные для неё (из "короткого" окна) 
в базу. А агенты получают ответ с результатом вставки.</p>

<p>"Короткое" окно распространяется на две секунды в будущее, чтобы нормально работали агенты, у которых часы немного 
спешат.</p>

<p>**"Исторические" данные**</p>

<p>Агрегатор позволяет агентам вставлять данные за последние 48 часов — это "длинное" окно вставки (его можно
настроить).</p>

<p>Если данные старше 48 часов, StatsHouse записывает метастатистику и выбрасывает этот фрагмент данных. Агент получает ответ `OK`.</p>

<p>Агрегация между хостами очень важн, поэтому StatsHouse делает всё возможное, чтобы она состоялась:
<li>каждый агент делает запрос на вставку нескольких десятков "исторических" секунд, начиная с самой "старой";</li>
<li>агрегатор получает эти запросы и выбирает самую "старую" секунду;</li>
<li>он агрегирует данные, вставляет их в базу данных и отправляет ответ;</li>
<li>затем снова выбирает "самую старую" секунду и т.д.</li></p>

<p>Этот алгоритм помогает наиболее удалённым ("старым") секундам оказаться рядом с самыми "новыми". Он делает 
возможным агрегирование исторических данных и помогает вставлять данные одновременно.</p>
</details>

### Работа при отказе агрегатора

Если агрегатор недоступен или отвечает с ошибкой, агент хранит данные на локальном диске.
Хранение данных на диске ограничено в байтах. Оно также ограничено по времени — в пределах "длинного"
(48-часового) окна вставки.

<details>
    <summary>Подробнее</summary>
  <p>**Распределение данных между репликами**</p>

  <p>Если доступ к диску нежелателен или невозможен, можно запустить агент с пустым аргументом `--cache-dir`.
StatsHouse не будет использовать диск. "Исторические" данные будут храниться в памяти, пока агрегаторы
недоступны, т. е. в течение нескольких минут.</p>

<p>Если агрегатор недоступен, агенты отправляют данные репликам.
Данные распределяются в соответствии с порядковыми номерами секунд: чётные секунды отправляются в одну из реплик,
нечётные — в другую. Таким образом, нагрузка на обе реплики увеличивается на 50 %. Это одна из причин, по которой 
StatsHouse записывает данные ровно в три реплики ClickHouse.</p>

  <p>**Предотвращение двойной вставки**</p>

  <p>Если агрегатор отвечает с ошибкой, агент отправляет данные другому агрегатору (другой реплике) на другом
хосте. Для дедупликации нужно использовать алгоритм консенсуса, а это довольно сложно.</p>

  <p>В StatsHouse основной и резервный агрегаторы могут вставлять данные от одного и того же агента в одну и ту же секунду.
Это происходит редко, и для такого случая мы отслеживаем двойные вставки с помощью метаметрики `__heartbeat_version`, 
которая показывает _количество агентов, отправляющих данные в текущую секунду_. Чтобы эта метаметрика была 
стабильной при нормальной работе агрегаторов, агенты отправляют данные каждую секунду, даже если в данный момент нет 
реальных пользовательских данных.</p>
</details>

## База данных

В базе данных [ClickHouse](https://clickhouse.com) хранятся [агрегированные](concepts.md#aggregation) данные метрик.

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

The next piece of data adds one more row: with the `e` _String top_ tag value and the counter equal to `5`.
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
