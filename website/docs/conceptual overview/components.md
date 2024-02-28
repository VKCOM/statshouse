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

The [ClickHouse](https://clickhouse.com) database stores aggregated metric data.

31:50


репликация
downsampling секунлы - минуты - часы (TTL разный)

только три таблицы
только 16 тегов
числа вместо строк

  

### ClickHouse table structure

Вся статистика всех типов для всех метрик исходно сохраняется в одну таблицу clickhouse,
один ряд соответствует одному агрегату. Примерное определение таблицы такое.

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

Если метрик не является перцентилем или счётчиком уникумов, то значение в соответствующей колонке будет пустым.
Также, для обычного счётчика все колонки, кроме count будут нулевыми/пустыми. Аналогично колонка строкового тэга
будет непустой только если использован топ строк.

Данные из этой таблицы агрегируются за некоторые временные интервалы (например, 60 секунд, 3600 секунд) и сохраняются
в идентичные таблицы, но с другим именем, для поддержки быстрой выборки за временные интервалы,
значительно превышающие секунду.

Данные шардируются между шардами clickhouse по хэшу от (metric, key0, … , key15), поэтому части данных метрики, с
оответствующие, например, метке `"protocol":"tcp"` обычно оказываются на разных шардах из-за разных значений других
тэгов (на всех шардах, если комбинаций тэгов много), и для получения полной картины нужно всегда делать
Distributed Query ко всем шардам. Это так, потому что набор тэгов имеет ограниченную кардинальность, и при её
достижении объём данных при агрегации перестаёт расти, поэтому если бы каждый шард хранил “сэмпл” всей статистики,
то при достаточном объёме данных, фактически каждому шарду пришлось бы хранить число рядов равное кардинальности
статистики, а не долю, пропорциональную числу шардов.

Буферные таблицы не используются, так как обычно каждый агрегатор делает 1 вставку в секунду. Вставка делается в
incoming-таблицу, а копирование оттуда с помощью материализованного представления с фильтрацией значений по time в
пределах окна приёма данных (двое суток), это защита от ошибочной вставки мусорных данных, которая приведёт к тому,
что при запросах clickhouse придётся читать данные из всех партов, а не 1-2.

Количество реплик каждого шарда должно быть больше или ровно 3, первые три будут использоваться для вставки
агрегаторами, остальные считаются readonly-репликами и могут разворачиваться для масштабирования нагрузки чтений.
Количество шардов может быть каким угодно (мы используем 8, в планах увеличить до 16). Для предотвращения неправильной
конфигурации агентов и разного шардирования, которое бы привело к взрывному росту объёма данных из-за плохой агрегации,
агенты присылают номер шарда-реплики с каждым пакетом данных а агрегатор, и если агрегатор видит, что данные
предназначаются не ему, отвечает ошибкой. Этот же номер шарда-реплики используется ingress proxy для того, чтобы
направить данные на правильный агрегатор.

## Data access service (API)

A data access service allows StatsHouse to send efficient queries to the database using a thin API.
The service caches data to minimize a database load. We limit retrieving data directly from ClickHouse
as much as possible, since ineffective queries can negatively impact the ClickHouse cluster.

Все клиенты работают через АПИ

контролирует доступ,
контролирует нагрузку,
пытается кешировать в памяти все что можно - все данные идут через него


## User interface

A user interface retrieves data from `statshouse-api` and displays metric data in a graph view.

## Ingress proxy

An ingress proxy receives data from the agents that live outside the protected perimeter
(i.e., outside the data center) and sends it to the aggregators.

Поскольку агенты и агрегаторы используют протокол TL RPC с ключом шифрования датацентра, агенты вне датацентра не могут
напрямую присоединяться к агрегаторам, так как это бы потребовало копирования/раскрытия ключа датацентра
на внешние площадки.

Поэтому ingress proxy имеет отдельный набор ключей шифрования для подключения извне. Любой ключ может отзываться
простым удалением из конфигурации ingress proxy.

Ingress proxy не имеет состояния, и для уменьшения вероятности атаки проксирует только подмножество типов запросов
TL RPC, используемых агрегаторами.

Ingress proxy должно быть ровно 3 штуки, каждая проксирует в соответсвующую реплику каждого шарда.
Поэтому отказ или остановка для обслуживания одной из ingress proxy эквивалентен отказу одной из реплик каждого шарда,
что не мешает нормальной работе системы.

### Cryptokeys

Статсхаус использует VK RPC с (опциональным) шифрованием для общения всех компонентов. В VK RPC криптоключ является
и паролем для входа,
и секретом для вывода эфемерных ключей соединения.

Для того, чтобы установить связь, клиент должен использовать при установлении соединения один из ключей,
известных серверу.

Центральным компонентам системы являются Агрегаторы, они при запуске получают единственный "главный" криптоключ
датацентра.

Все агенты для подключения к Агрегаторам должны получить адреса первого шарда Агрегаторов в параметре `-agg-addr=X`,
а также "главный" криптоключ датацентра в параметре `-aes-pwd-file=X`.

Однако это безопасно только внутри защищённого периметра. Если есть необходимость подключаться извне,
используется ingress proxy, установленные как раз на границе периметра.

Ингресс прокси стоит на границе, и у неё есть 2 половинки - RPC сервер для того, чтобы агенты подключались снаружи,
и RPC клиент для подключения самой прокси к Агрегаторам внутри.

Ингресс прокси обязаны знать свои внешние адреса, по которым к ним будут подключаться агенты, эти адреса даются
в аргументе `-ingress-external-addr`.
Для управления тем, на каких интерфейсах могут подключаться агенты используется аргумент (`-ingress-addr=X`,
обычно `:8128` что эквивалентно `0.0.0.0:8128`, либо адрес подсети сетевого адаптера, чтобы ограничить подключение
только через него).
Порт в нём должен совпадать с портами в `-ingress-external-addr`.

По этим адресам внешняя половинка ингресс прокси должна быть доступна для агентов.

В настройках ingress proxy указывается как внутренний криптоключ для отправки данных Агрегаторам `-aes-pwd-file=X`,
так и множество внешних ключей для агентов,
установленных на удалённых площадках `-ingress-pwd-dir=X`, содержимое каждого файла это криптоключ,
имя файла игнорируется, и считается комментарием. Длина ключей произвольна, но не меньше 4 байтов, первые 4 байта
используются для идентификации ключей и не могут совпадать.
При изменении содержимого папки нужно перезапустить ingress proxy, механизма слежения за папкой нет,
так как набор ключей меняется крайне редко.

Соответственно каждый агент получает в `-aes-pwd-file=X` один из ключей указанных у прокси в папке `-ingress-pwd-dir=X`.

### A proxy behind the proxy

Тройка Ingress proxy имитирует агрегаторы, так что можно установить ещё один уровень ingress proxy,
который будет в качестве агрегаторов использовать предыдущую прокси.

Тогда для следующей ingress proxy в аргументе `-agg-addr=X` даётся тройка адресов из `-ingress-external-addr=X`
предыдущей прокси, а в качестве криптоключа в `-aes-pwd-file=X` один из криптоключей в папке `-ingress-pwd-dir=X`
предыдущей прокси.

### Kubernetes-related questions

При шифровании в VK RPC для вывода эфемерных ключей используются
remote- и local- IP-адреса соединений, как их видят клиент и сервер, так что трюки типа перекладывания пакетов
между адаптерами с помощью средств firewall приведут к невозможности установления соединений.
Если соединяются, например, kubernetes-подобные компоненты, то придётся создать
виртуальные сетевые адаптеры и подключить их друг к другу средствами linux network namespaces.

Однако крайне не рекомендуется разворачивать агенты и ingress proxy внутри подов. Лучше устанавливать агенты
на контейнеровозы, прокидывая в поды порт 13337.

Эта рекомендация потому, что статсхаусу не нравится, когда количество агентов флуктуирует.
Агенты отчитываются на агрегаторы строго каждую секунду,
и неизменное число агентов являесят важным индикатором того, что все агенты имеют связь до Агрегаторов.
Остановка подов не должна приводить к уменьшению этого числа, так как сработает главный алерт.

## Metadata

A metadata system stores a list of metrics and their settings.
It also holds a global `string`↔`int32` mapping:
StatsHouse maps tag values (which are string values) to `int32` values for higher efficiency.
This huge map is common for all metrics. The metadata system prevents it from being overloaded.

Свойства каждой метрики хранятся в специальном сервисе метаданных, развёрнутом обычно на машинах первого
шарда агрегаторов. Поскольку нагрузка на сервис данных невелика, нет смысла разворачивать этот сервис на отдельных
машинах.

Для каждой метрики хранится её тип (для правильного отображения), имя метрики, имена и способ интерпретации тэгов.

Метрики создаются через UI, автоматического создания метрик не происходит. Это важно, так как
все компоненты предполагают, что разных метрик немного (максимум десятки тысяч)
и не имеют защиты от неконтролируемого роста числа разных метрик.

При миграции с существующего решения можно включить режим “автосоздания”, который создаст все реально используемые
метрики, затем мы рекомендуем отключит автосоздание, так как иначе может произойти создание слишком большого числа
метрик, например если кто-то запишет в имя метрики rand.

Агрегаторы находятся в постоянном TL RPC long poll к сервису метаданных на изменение метаданных метрик,
а агенты аналогично в long poll к агрегаторам, поэтому изменения свойств метрики уже через секунду
отражаются на всех агентах.

Удалять метрики нельзя, так как нет способа сделать это эффективно в базе данных ClickHouse.
Поэтому используется скрытие метрики установкой флага visible (это действие обратимо).
При этом статистика по этой метрике перестаёт записываться в базу данных.

### Mapping budget

Tag values are usually `string` values and appear repeatedly. For higher efficiency, StatsHouse maps all of them to
`int32` values. This huge `string`↔`int32` map is common for all metrics. The elements of this map are never deleted.
To prevent the uncontrollable increase of the map, the budget for creating new mappings is limited to 300.
Upon exceeding the budget, new mappings can be added once per hour (this rule is customizable).

Mapping flood appears when you exceed this budget.

Then the budget is over and new mappings are not allowed, StatsHouse inserts a service `mapping flood` value to a
tag column not to lose the entire event.

Mapping string values

Сервис метаданных хранит взаимно однозначные отображение в своей базе.

```
    'iphone' <=> 12
    'null' <=> 26
```

Flood-лимиты на создание хранятся в той же базе данных.

Агрегаторы работают с сервисом данных напрямую, и кэшируют отображения в памяти и файлах (на месяц),
агенты работают с отображениями через агрегаторы, и также кэшируют отображения в памяти и файлах (тоже на месяц).
Также агенты при старте с чистого листа используют специальный boostrap запрос из примерно 100000 самых
распространённых отображений, это нужно так как при разворачивании на 10000 машинах пришлось бы скачать с агрегаторов
примерно миллиард значений по-одному, что заняло бы долгое время, в течение которого метрики бы не могли писаться.
