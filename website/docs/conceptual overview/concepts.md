---
sidebar_position: 2
---

import PerSecAggr from '../img/per-sec-aggr.png'
import MetricFormula from '../img/metric-formula.png'
import AggregationComponents from '../img/aggregation-components.png'
import Bottlenecks from '../img/bottlenecks.png'
import CardinalitySamplingNoise from '../img/cardinality-sampling-noise.png'
import HigherSamplingCoef from '../img/higher-sampling-coef.png'
import Lod from '../img/lod.png'
import MetricFormulaType from '../img/metric-formula-type.png'


# Concepts

To understand StatsHouse deeply, learn the basic metric-related concepts:
* [aggregation](#aggregation),
* [sampling](#sampling).

Then find details about the [metrics' implementation](#metrics) in StatsHouse.

## Aggregation

When we talk about monitoring, we mean getting statistical data.

A _metric_ is the minimal unit for setting up, collecting, and viewing statistics. When sending metric data, you may 
label each measurement (i.e., event) with tags. You can also assign a timestamp to the event.

<img src={MetricFormula} width="700"/>

StatsHouse aggregates the events with the same tag sets—both within the time period and between the hosts. For 
example, this is what an aggregate within one second looks like:

<img src={PerSecAggr} width="700"/>

:::important
StatsHouse does not store an exact metric value per each moment.
Instead, it stores aggregates associated with time intervals.

An aggregate is the result of aggregation. 
It is the minimal set of descriptive statistics such as _count_, _sum_, _min_, _max_. StatsHouse uses them to 
reconstruct the rest of statistics if necessary.
:::

Imagine a hypothetical product. For this product, we need to get the number of received packets per second.
The packets may have different 
* formats: `TL`, `JSON`;
* statuses: "correct" (`ok`) or "incorrect" (`error_too_short`, `error_too_long`, etc.).

When the user-defined code receives a packet, it sends an event to StatsHouse, specifically, 
to an [agent](components.md#agent).
For example, let this event have a JSON format and a "correct" status:

```
    {"metrics":[ {"name": "toy_packets_count",
     "tags":{"format": "JSON", "status": "ok"},
     "counter": 1}] }
```

Formats and statuses may vary for the packets:

```
    {"metrics":[ {"name": "toy_packets_count",
     "tags":{"format": "TL", "status": "error_too_short"},
     "counter": 1} ]}
```

Let's represent an event as a row in a conventional database. Upon per-second aggregation, 
we'll get the table below—for each tag value combination received, we get the row with the corresponding count:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | JSON   | ok              | 100     |
| 13:45:05  | toy_packets_count | TL     | ok              | 200     |
| 13:45:05  | toy_packets_count | TL     | error_too_short | 5       |

The number of rows in such a table is a metric's cardinality.

### Cardinality

:::important
Cardinality is how many unique tag value combinations you send for a metric.
:::

In the example above, the metric's cardinality for the current second is _three_ as we have three tag value combinations.

The amount of inserted data does not depend on the number of events. It depends on the number of unique tag 
value combinations. StatsHouse "collapses" the rows with the same tag value combinations and summarizes the counters. 

StatsHouse collects data from many hosts ([agents](components.md#agent)) simultaneously. Upon collecting and 
aggregating data within a second, it sends data to [aggregators](components.md#aggregator).

<img src={AggregationComponents} width="1000"/>

For our hypothetical metric, the between-host aggregation per second leads to the following:

| timestamp | metric            | format  | status          | counter |
|-----------|-------------------|---------|-----------------|---------|
| 13:45:05  | toy_packets_count | JSON    | ok              | 1100    |
| 13:45:05  | toy_packets_count | JSON    | error_too_short | 40      |
| 13:45:05  | toy_packets_count | JSON    | error_too_long  | 20      |
| 13:45:05  | toy_packets_count | TL      | ok              | 30      |
| 13:45:05  | toy_packets_count | TL      | error_too_short | 2400    |
| 13:45:05  | toy_packets_count | msgpack | ok              | 1       |

Cardinality may increase due to between-host aggregation: the sets of tag value combinations for the hosts may vary.
In the example above, the total cardinality for the current second is _six_.

Upon aggregation, StatsHouse inserts data into the ClickHouse database—specifically, into a per-second table.
The amount of per-second data is huge, so StatsHouse downsamples data: per-second data is available for two days.

Meanwhile, StatsHouse resets seconds in each timestamp to zero. It aggregates data within a minute and inserts it to 
a per-minute ClickHouse table. Similarly, StatsHouse aggregates per-minute data to per-hour one. 

* Per-second aggregated data is stored for the first two days.
* Per-minute aggregated data is stored for a month.
* Per-hour aggregated data is available forever (if not deleted manually).

The total [hour cardinality](../guides/view-graph.md#cardinality) for a metric determines how many 
rows for a metric can be stored in a database for a long time.

When retrieving data from a database, we have to iterate over the rows for the chosen time interval. It is 
the cardinality that determines the number of these rows and the time we spend on doing this.

## Sampling

StatsHouse has two bottlenecks:
* sending data from agents to aggregators,
* inserting data into the ClickHouse database.

<img src={Bottlenecks} width="1000"/>

If the amount of sent or inserted data exceeded the aggregator's or database's capacity, it could lead to a
processing delay. This delay could increase indefinitely or disappear if the amount of data was reduced.
To ensure minimal delay, StatsHouse samples data.

:::important
Sampling means that StatsHouse throws away pieces of data to reduce its overall amount. 
To keep aggregates and statistics the same, StatsHouse multiplies the rest of data by a sampling coefficient.
:::

Suppose we have three data rows per second for a single metric:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | JSON   | ok              | 100     |
| 13:45:05  | toy_packets_count | TL     | ok              | 200     |
| 13:45:05  | toy_packets_count | TL     | error_too_short | 5       |

Suppose also that the channel width allows us to send only two rows to the aggregator. StatsHouse will set 
the sampling coefficient to `1.5`, then randomize the rows, and send only the first two rows multiplied by `1.5`.

The data sent will look like this:

| timestamp | metric            | format | status | counter |
|-----------|-------------------|--------|--------|---------|
| 13:45:05  | toy_packets_count | TL     | ok     | 300     |
| 13:45:05  | toy_packets_count | JSON   | ok     | 150     |

or like this:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | TL     | ok              | 300     |
| 13:45:05  | toy_packets_count | TL     | error_too_short | 7.5     |

or like that:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | TL     | error_too_short | 7.5     |
| 13:45:05  | toy_packets_count | JSON   | ok              | 150     |

If each agent samples data in this way, it will throw away some rows. Upon aggregating data between the agents, 
the counters for the rows will be close to their original values (as if data was not sampled at all).
The more agents aggregate their sampled data, the more accurate the resulting counters are.

The same is true for downsampling data: when StatsHouse aggregates 60-second rows to a one-minute row, or
60-minute rows—to a one-hour row.

The aggregates' averages will stay the same but will get the high-frequency noise.

<img src={CardinalitySamplingNoise} width="900"/>

:::important
We strongly [recommend reducing metrics' cardinality](../guides/send-data.md#how-many-tag-values).
:::

The same algorithm applies both when the agents send data to the aggregator and when the aggregator inserts data into 
the database.

### Non-integer sampling coefficients

Sampling coefficients should sometimes be non-integer to keep aggregates and statistics the same. 
This leads to [non-integer values for counters](../guides/view-graph.md#3--descriptive-statistics), 
though each counter is an integer number at its core.

In StatsHouse, counters are floating-point by default. 
For a particular metric, you can choose the option to randomly round the sampling coefficients:
if the desired sampling coefficient is `1.1`, it will be rounded to `1` nine times out of ten—and it will be rounded to 
`2` only once.

### Fair resource sharing

Now assume we have more than one metric, and they share the same channel of a given width.
Ideally, the metrics should not affect each other.
If a metric starts generating a lot more rows than the others, it should get the higher sampling coefficient, so that 
the other metrics are not affected.

<img src={HigherSamplingCoef} width="700"/>

The algorithm's logic is the following:

1. StatsHouse sorts all the metrics in ascending order by a number of occupied bytes. Then it 
   refers to them one by one.
2. StatsHouse calculates the number of bytes to grant to each metric. The rest of the budget is shared 
   between the rest of the metrics.
3. If a metric does not spend its budget, its data is not sampled at all.
4. If a metric exceeds its budget, StatsHouse samples it so that the metric data fits in the budget:
for a metric with 2000 rows and the budget of 500 rows, the sampling coefficient is set to 4.
5. StatsHouse reduces the rest of the budget by the number of bytes spent. 

The number of occupied bytes depends on the [metric type](../guides/send-data.md#how-to-choose-a-metric-type) 
and the number of tag values. _Counter_ metrics require less space than _value_ metrics.

In practice, some metrics are more important than the other. StatsHouse administrators can set up 
[weights](../guides/edit-metrics.md#admin-settings) for the particular metrics.
A metric having a weight of 2 gets the channel two times as broad as the channel for a 1-weight metric.

### Sampling "mainstays"

The above-mentioned algorithm works fine when the original counters for the rows are close to each other.
Often, a metric has one or several "mainstays" that are dominating rows.
For example, if we successfully process 1000 packets per second, 
but we also get one error of each type, the first "ok" row becomes the "mainstay":

| timestamp | metric            | format | status          | counter                                                         |
|-----------|-------------------|--------|-----------------|-----------------------------------------------------------------|
| 13:45:05  | toy_packets_count | JSON   | ok              | 1000 <text className="orange-text">← will not be sampled</text> |
| 13:45:05  | toy_packets_count | TL     | error_too_short | 1                                                               |
| 13:45:05  | toy_packets_count | TL     | error_too_long  | 1                                                               |
| 13:45:05  | toy_packets_count | TL     | error_too_bad   | 1                                                               |
| 13:45:05  | toy_packets_count | TL     | error_too_good  | 1                                                               |

When we display the sum of counters on a graph, we get the flat graph for the `1004` value.

Imagine, we have to insert only four rows out of five because of a budget.
We will throw away one row per each second:
* for four seconds out of five the value will be `1003 * (5 / 4) ~ 1203`,
* for one second out of five the value will be `4 * (5 / 4) = 5`.

On average, these values look valid: if we summarize them, we'll get `1004`. But the graph will be located higher than 
the average value—around `1200`—and will have the ravines down to 0.

When StatsHouse has to sample such data (with "mainstays"), it divides the budget in two. The first part of the 
budget (two rows in our example) is granted for the non-sampled rows with the maximum counter values.
The second part of the budget is spent for the rest of the rows: the random rows are inserted while the others are 
thrown away. Unlike "mainstays," these "weak" rows get the respectively higher sampling rate.  

In our example, StatsHouse will insert the row with the `1000` counter as is, i.e., will not sample this row. So, 
we'll get the graph with the average value of `1004`.

### Resolution

The highest available resolution of data to show on a graph depends on the currently available
[aggregate](#aggregation): you can get per-second data for the last two days, per-minute data for the last month,
and you can get per-hour data for any period you want.

If getting the highest available resolution is not crucial for you, but it is important for you
to reduce [sampling](#sampling), [reduce your metric resolution](../guides/edit-metrics.md#resolution).

For example, you may choose a custom resolution to make the [agent](../conceptual%20overview/components.md#agent)
send data once per five seconds instead of sending per-second data.
StatsHouse will send data five times more rarely and grant five times more rows for the metric.
The processing delay will increase by **ten** seconds:
* StatsHouse will be collecting data for **five** seconds,
* then it will shard data into five partitions,
* and will be sending data for the next **five** seconds—one shard per second.

This way of sending data ensures fair channel sharing for the metrics with differing resolution.

The resolution level may be a divisor of 60. To avoid jitter on a graph, use the "native" resolution levels: 
1, 5, 15, 60 seconds. These levels correspond to levels of details (LOD) in the UI.
If you choose a 2-second resolution level, the events will be distributed between 5-second LODs unevenly—two or 
three events per LOD:

<img src={Lod} width="300"/>

This uneven distribution leads to a jitter on a graph.

### Metric types

#### Value metrics

Кроме метрик-счётчиков, есть метрики-значения. Например вместо счётчика принятых пакетов мы бы могли захотеть
записывать размер принятых пакетов.

```
    {"metrics":[ {"name": "toy_packets_size",
     "tags":{"format": "JSON", "status": "ok"},
     "value": [150]} ]}
```
или

```
    {"metrics":[ {"name": "toy_packets_size",
     "tags":{"format": "JSON", "status": "error_too_short"},
     "value": [0]} ]}
```
Значение является массивом, чтобы можно было отправлять сразу несколько значений пачкой, что более эффективно.

Тогда кроме счётчика будут вычисляться агрегаты значений - сумма, минимальное и максимальное значение.

| timestamp | metric           | format | status          | counter | sum   | min | max  |
| --------- | ---------------- | ------ | --------------- | ------- | ----- | --- | ---- |
| 13:45:05  | toy_packets_size | JSON   | ok              | 100     | 13000 | 20  | 1200 |
| 13:45:05  | toy_packets_size | TL     | ok              | 200     | 7000  | 4   | 800  |
| 13:45:05  | toy_packets_size | TL     | error_too_short | 5       | 10    | 0   | 8    |

Среднее вычисляется при выборке данных путём деления суммы на сумму счётчика.

##### Receiving regular values

Детали приёма регулярных значений
Агенты финализируют секунду для отправки синхронно с календарной секундой. Поэтому если у клиента есть какое-то
значение, которое должно фиксироваться каждую секунду (условный “уровень воды”), клиентские библиотеки стараются присылать это
значение в районе середины календарной секунды. Таким образом увеличивается вероятность того, что каждая секунда будет
содержать ровно 1 измерений, однако гарантий этого система не предоставляет. Клиенты, которым это важно могут
явно указать timestamp.

#### Percentiles

Если в описании метрики установлена галочка “с перцентилями”, то кроме агрегатов значений система будет
считать перцентили на агентах, пересылать на агрегаторы, агрегировать там и записывать в clickhouse.
Объём данных для такой метрики значительно возрастет, поэтому система скорее всего выберет высокие факторы сэмплирования.
В таком случае может помочь уменьшение кардинальности или временного разрешения.

#### Unique counters

Используются, чтобы оценить число разных уникальных целых значений (если значения не целые, а например строки,
то можно взять hash строк) Предположим, что пакеты содержат id пользователя, отправляющего пакеты. Мы можем
посчитать сколько разных пользователей отправляло пакеты.

```
    {"metrics":[ {"name": "toy_packets_user",
     "tags":{"format": "JSON", "status": "ok"},
     "unique": [17]} ]}
```

Уникальное значение является массивом, чтобы можно было отправлять сразу несколько значений пачкой, что более эффективно.

Множества хранятся в сжатом виде и использованием функции, подобной Hyper Log Log, так что сами значения недоступны,
можно узнать только оценку кадинальности множеств.

| timestamp | metric           | format | status          | counter | unique                    |
| --------- | ---------------- | ------ | --------------- | ------- | ------------------------- |
| 13:45:05  | toy_packets_user | JSON   | ok              | 100     | uniq(17,  19, 13, 15)     |
| 13:45:05  | toy_packets_user | TL     | ok              | 200     | uniq(17,  19, 13, 15, 11) |
| 13:45:05  | toy_packets_user | TL     | error_too_short | 5       | uniq(51)                  |

Кроме того для уникумов хранятся обычные агрегаты, как для значений (минимальное, максимальное, среднее,
стандартное отклонение), интерпретированных, как int64 и округлённых до float64.
Знание диапазона значений часто помогает в отладке.

### Tags

#### Tag names

Хранение меток - имена

Все метрики хранятся в одной таблице, где есть 15 колонок для меток, названных tag1..tag15. Когда в примере выше
мы использовали метки с именем “format”, “status” на самом деле система выбирала одну из колонок на основании
описания метрики, например можно отредактировать описание так, чтобы format направлялся в tag1,
а статус - в tag2 или наоборот. Также можно использовать системные имена 1..15 напрямую для выбора нужной колонки,
они не пересекаются с пользовательскими и всегда доступны для записи.

Дополнительная колонка tag0 имеет специальную интерпретацию, и служит для задания окружения (environment),
в котором собирается статистика. Например production или staging. В принципе, могут использоваться любые значения.
Например, если на подмножестве машин экспериментальная версия, может быть задана строчка для этого эксперимента.
Задаётся в клиентских библиотеках один раз при инициализации. В остальном с точки зрения системы tag0 ничем
не отличается от остальных колонок.

#### Tag values

Хранение меток - значения

Так как значения меток постоянно повторяются, для компактности хранения и скорости записи и выборки tag0..tag15
имеют тип int32 и там хранятся не строчки, а значения, которые берутся из гигантского отображения строчек string ↔ int32.
Отображение общее на все метрики, а его элементы никогда не удаляются, а чтобы предотвратить его бесконтрольный рост,
на создание элементов отображения установлен бюджет с лимитом в 300, пополняющийся на 1 значения в час (настраивается).
Когда бюджет исчерпывается и отображение создать не удаётся, в колонку записывается  специальное служебное значение
(mapping flood), чтобы не терять событие целиком.

Максимальная длина значения тэга 128 байтов, если больше - обрезается. Также делается нормализация - с обеих сторон
делается TRIM, а все последовательности юникодных пробелов внутри заменяются на 1 ASCII пробел,
а все непечатные символы заменяются на дорожный знак. Это делает метки более регулярными и уменьшает удивление
при отображении в UI, копировании и вставке в чаты, и т.д.

Иногда значения меток уже имеют подходящий тип, а количество значений велико, например номера приложений или
каких-нибудь других объектов. Тогда можно отредактировать описание метрики, указав, что определённые тэги
являются “сырыми”, тогда для них вместо отображения строчки-значения, она будет просто парситься, как (u)int32
(принимаем значения в диапазоне -2^31..2^32-1) и вставляться в таблицу как есть. При отображении в UI можно попросить
показывать такие значения в каком-то формате, например timestamp, беззнаковое целое, hex, ip адрес, и т.д.

must be UTF-8 string
// 1. trim unicode whitespaces to the left and right
// 2. replace all consecutive unicode whitespaces inside with single ASCII space
// 3. trim to MaxStringLen (if last utf symbol fits only partly, we remove it completely)
// 4. non-printable characters are replaced by roadsign (invalid rune)

тэги с невалидным UTF-8 приводят к тому, что всё событие выбрасывается и загорается красная плашка над графиком.
Это так в основном потому, что у нас есть много текста в формате vk1251, если такой текст забудут сконвертить
(в PHP это делает обёртка наша, а в других языках мало ли) , то мы хотим, чтобы это заметили).
А так можно было тоже невалидные байты заменять на дорожный знак.
#### String tag and _Top N_

Топ строк

Иногда бывает ситуация, когда число разных значений метки огромно и неограниченно, например referrer
или слово в поисковом запросе. Если использовать обычные метки, то новые значения очень быстро выберут
бюджет на создание элементов отображения, да ещё и засорят его огромным количеством одноразовых значений.
Для подобных случаев система поддерживает специальную метку с именем _s, что означает буквально строковый тэг.
Буквально в дополнение к 16 целочисленным колонкам тэгов сделана дополнительная строковая колонка.
Для каждой комбинации обычных тэгов  на агенте создаётся специальная структура данных, которая хранит
некоторое количество популярных за эту секунду значений строки (например, 100), когда структура наполняется,
применяется вероятностное вытеснение. На агрегатор отправляется топ этих значений, например 10.
При сэмплировании выбрасываются либо все строки из набора, либо ни одной, таким образом распределение самих наборов
по пространству кардинальности обычных меток сохраняется, это важно для большинства пользователей.
Аггрегатор собирает все строки для набора во всех агентов, и в свою очередь вставляет в базу данных топ этих строк
(например, 20), а для “остальных” не попавших в топ, используется пустая строка.
Таким образом обычные счётчики (и метрики других типов) являются на самом деле частным случаем топа строк,
когда все строки пустые.

#### Max host tag

Метка машины-агента и механизм max_host

Большинство пользователей интуитивно хотят использовать имя машины-агента в качестве метки, чтобы иметь возможность
просматривать статистику с каждой машины независимо. Однако, что может оказаться неожиданным, добавление такой метки
прекращает агрегацию данных между машинами, а значит увеличивает кардинальности и объём данных в соответствующее число
раз. Например, если машин-агентов 100, то объём данных увеличится в 100 раз, и системой могут быть выбраны гигантские
коэффициенты сэмплирования, например 10, 50 или 100, при этом качество данных сильно ухудшается из-за шума.

Поэтому в StatsHouse для всех метрик включен очень дешёвый механизм, когда в специальную колонку max_host при агрегации
данных записывается имя машины, ответственной за максимальное значение (либо внёсшей максимальный вклад в счётчик для
счётчиков). Такая колонка увеличивает объём данных менее, чем на 10%, при этом позволяет ответить на вопросы “на каком
хосте максимальный объём занятого дискового пространства” или “на каком хосте максимальное количество ошибок каждого
типа”.

Например, после агрегации следующих строк от двух агентов

| timestamp | metric      | format | … | min | max  | max_host |
| --------- | ----------- | ------ | - | --- | ---- | -------- |
| 13:45:05  | toy_latency | JSON   |   | 200 | 1200 | nginx001 |

| timestamp | metric      | format | … | min | max | max_host |
| --------- | ----------- | ------ | - | --- | --- | -------- |
| 13:45:05  | toy_latency | JSON   |   | 4   | 80  | nginx003 |

ясно, что максимальное значение latency 1200 было именно на хосте nginx001

| timestamp | metric      | format | … | min | max  | max_host |
| --------- | ----------- | ------ | - | --- | ---- | -------- |
| 13:45:05  | toy_latency | JSON   |   | 4   | 1200 | nginx001 |


Прежде чем добавлять метку с именем машины-агента мы рекомендуем попробовать посмотреть в UI функцию max_host для
вашей метрики. Чаще всего, зная имя лишь одной проблемной машины, можно посмотреть логи и решить проблему.

Также советуем, где возможно, использовать разбивку не по отдельным машинам, а по их группам, используя метку
environment или собственную метку. Например, запуская экспериментальную версию на одной или нескольких машинах,
можно установить environment staging или dev для того, чтобы отделить статистику с этих машин от остальных.

Значение максимум в колонке max_host имеет типа Float32, а не Float64 для лучшего сжатия, так как высокая точность
здесь не нужна. Имя хоста хранится в виде строкового отображения (Int32), как тэги.

### Timestamps and a receive window

Хранение времени и окно приёма

Обычно время события назначается системой автоматически по времени приёма события, однако если нужно писать старую
статистику, можно указать конкретное значение времени события, но принимается только статистика за последние
полтора часа, если более старая - время будет установлено в текущее время минус полтора часа. Это сделано потому,
что система может работать только при эффективной агрегации между хостами, а для этого все хосты должны присылать
данные за конкретную секунду максимально вместе и слажено. А также система хранения (сейчас это clickhouse) работает
гораздо медленнее, если значение primary ключа оказывается в разных партах на диске. Для тех метрик, которые явно
указывают время событий можно ожидать выбора системой более высоких факторов сэмплирования, так как ряды
с разным временем не могут быть агрегированы между собой.

### Meta-metrics

Для получения сведений о работе самой системы на разных этапах собираются и записываются метаметрики.
Все они имеют префикс два подчерка. Самые главные из них вынесены в UI для каждой метрики - это ошибки приёма данных
(например отрицательное значение счётчика или значение-NaN), факторы сэмплирования, выбранные агентом и агрегатором,
а также оценка часовой кардинальности метрики и количество созданных элементов отображения.

Многие метаметрики по разным причинам не подчиняются общим правилам, например не сэмплируются.
Некоторые метаметрики, например статус приёма данных и факторы сэмлирования агентом пересылаются в специальной
компактной форме для экономии трафика.


