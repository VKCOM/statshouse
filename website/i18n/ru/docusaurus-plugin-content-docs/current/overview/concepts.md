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
import MinAvailableAggregation from '../img/min-available-aggregation.png'

# Основные понятия

Чтобы лучше понять устройство StatsHouse, познакомьтесь с основными понятиями:
* [агрегацией](#aggregation),
* [кардинальностью](#cardinality),
* [семплированием](#sampling).

:::tip
Краткое описание можно почитать [**здесь**](../guides/design-metric.md#how-many-tag-values).
:::

## Агрегация

StatsHouse агрегирует, то есть "схлопывает", измерения с одинаковыми наборами тегов — как в пределах временного 
интервала, так и между машинами.

### Агрегат

**Агрегат** — это результат агрегации.
Это минимальный набор описательных статистик: _count_, _sum_, _min_, _max_. На основе них StatsHouse при 
необходимости восстанавливает остальные статистики. Например, вот как выглядит секундный агрегат:

<img src={PerSecAggr} width="700"/>

:::important
StatsHouse не хранит точное значение метрики за каждый момент времени. Вместо этого в системе хранятся агрегаты, 
относящиеся к временным интервалам
(см. [минимальный интервал агрегации из доступных](#minimal-available-aggregation-interval)).
:::

StatsHouse вставляет агрегированные данные в базу данных ClickHouse, а именно в секундную таблицу. Посекундных 
данных очень много, поэтому с течением времени StatsHouse уменьшает их разрешение: посекундные данные доступны в 
течение двух дней. Затем StatsHouse агрегирует данные в пределах каждой минуты и вставляет их в минутную таблицу.
Затем данные агрегируются в пределах каждого часа.

### Минимальный интервал агрегации из доступных

То, какой агрегат доступен в данный момент времени, зависит от "возраста" данных:
* данные, агрегированные по секундам, хранятся в течение первых двух дней,
* данные, агрегированные по минутам, хранятся в течение месяца,
* данные, агрегированные по часам, хранятся как угодно долго (или до тех пор, пока их не удалят вручную).

<img src={MinAvailableAggregation} width="700"/>

Представим гипотетический продукт. Для него нам нужно получить количество принятых пакетов в секунду.
Пакеты могут иметь различные
* форматы: `TL`, `JSON`;
* статусы: "правильный" (`ok`) или "неправильный" (`error_too_short`, `error_too_long` и т.д.).

Когда пользовательский код получает пакет, он отправляет событие в StatsHouse, а именно в [агент](components.md#agent).
Например, пусть это событие имеет формат JSON и статус "правильно":

```
    {"metrics":[ {"name": "toy_packets_count",
     "tags":{"format": "JSON", "status": "ok"},
     "counter": 1}] }
```

Форматы и статусы пакетов могут различаться:
```
    {"metrics":[ {"name": "toy_packets_count",
     "tags":{"format": "TL", "status": "error_too_short"},
     "counter": 1} ]}
```

Представим событие в виде строки в обычной базе данных. При посекундной агрегации мы получим таблицу ниже — для 
каждой полученной комбинации значений тегов мы получим строку с соответствующим счётчиком:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | JSON   | ok              | 100     |
| 13:45:05  | toy_packets_count | TL     | ok              | 200     |
| 13:45:05  | toy_packets_count | TL     | error_too_short | 5       |

Количество рядов в такой таблице - это кардинальность метрики.

### Кардинальность

:::important
Кардинальность - это количество уникальных комбинаций значений тегов, которые вы отправляете для метрики.
:::

В приведённом выше примере кардинальность метрики для текущей секунды равна _трём_, поскольку у нас есть три комбинации 
значений тегов.

Объём вставляемых данных не зависит от количества событий. Он зависит от количества уникальных комбинаций значений 
тегов. StatsHouse "схлопывает" строки с одинаковыми комбинациями значений тегов и суммирует счётчики.

StatsHouse собирает данные со многих хостов ([агентов](components.md#agent)) одновременно. После сбора и
агрегирования данных в пределах секунды, он отправляет данные на [агрегаторы](components.md#aggregator).

<img src={AggregationComponents} width="1000"/>

Для нашей гипотетической метрики в результате агрегации между хостами в пределах секунды мы получим следующее:

| timestamp | metric            | format  | status          | counter |
|-----------|-------------------|---------|-----------------|---------|
| 13:45:05  | toy_packets_count | JSON    | ok              | 1100    |
| 13:45:05  | toy_packets_count | JSON    | error_too_short | 40      |
| 13:45:05  | toy_packets_count | JSON    | error_too_long  | 20      |
| 13:45:05  | toy_packets_count | TL      | ok              | 30      |
| 13:45:05  | toy_packets_count | TL      | error_too_short | 2400    |
| 13:45:05  | toy_packets_count | msgpack | ok              | 1       |

Кардинальность может увеличиваться из-за агрегации между хостами: наборы комбинаций значений тегов для хостов могут 
различаться. В приведенном примере общая кардинальность для текущей секунды составляет _шесть_.

Общая [часовая кардинальность](../guides/view-graph.md#cardinality) для метрики определяет, сколько
рядов для метрики может храниться в базе данных в течение длительного времени.

Чтобы извлечь данные из базы, нам придётся перебирать строки для выбранного временного интервала. Именно
кардинальность определяет количество этих рядов и время, которое придётся потратить на такой перебор.

## Семплирование

У StatsHouse есть два узких места:
* отправка данных от агентов к агрегаторам,
* вставка данных в базу ClickHouse.

<img src={Bottlenecks} width="1000"/>

Если объём отправленных или вставляемых данных превышает возможности агрегатора или базы данных, может возникать 
задержка. В принципе, эта задержка может увеличиваться бесконечно, а может и исчезнуть, когда объём данных уменьшится.
Чтобы обеспечить минимальную задержку, StatsHouse семплирует данные.

:::important
Семплирование означает, что StatsHouse выбрасывает часть данных, чтобы уменьшить их общий объём.
Чтобы сохранить значения агрегатов и статистик, StatsHouse домножает оставшиеся данные на 
коэффициент семплирования.
:::

Предположим, что за секунду собраны три строки данных, причём все они принадлежат одной метрике:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | JSON   | ok              | 100     |
| 13:45:05  | toy_packets_count | TL     | ok              | 200     |
| 13:45:05  | toy_packets_count | TL     | error_too_short | 5       |

Suppose also that the channel width allows us to send only two rows to the aggregator. StatsHouse will set 
the sampling coefficient to `1.5`, then randomize the rows, and send only the first two rows multiplied by `1.5`.

Предположим также, что ширина канала позволяет нам отправить агрегатору только две строки. StatsHouse выберет
коэффициент семплирования, равный `1,5`, затем перемешает строки случайным образом и отправит только первые две, 
домноженные на `1,5`.

Отправленные данные будут выглядеть следующим образом:

| timestamp | metric            | format | status | counter |
|-----------|-------------------|--------|--------|---------|
| 13:45:05  | toy_packets_count | TL     | ok     | 300     |
| 13:45:05  | toy_packets_count | JSON   | ok     | 150     |

или так:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | TL     | ok              | 300     |
| 13:45:05  | toy_packets_count | TL     | error_too_short | 7.5     |

или вот так:

| timestamp | metric            | format | status          | counter |
|-----------|-------------------|--------|-----------------|---------|
| 13:45:05  | toy_packets_count | TL     | error_too_short | 7.5     |
| 13:45:05  | toy_packets_count | JSON   | ok              | 150     |

Если каждый агент семплирует данные таким образом, он выбрасывает часть рядов. Однако при агрегации данных между 
агентами значения счётчиков каждого ряда будут близки к исходным значениям (как будто данные вообще не семплировались).
Чем больше агентов участвует в агрегации, тем точнее получаются результирующие счётчики.

То же самое произойдёт при агрегации рядов за 60 секунд в один минутный ряд, и, далее, в часовой.
Все средние значения агрегатов сохранятся, но к ним добавится высокочастотный шум.

<img src={CardinalitySamplingNoise} width="900"/>

:::important
Мы настоятельно рекомендуем следить за тем, чтобы 
[кардинальность метрик была минимальной](../guides/design-metric.md#how-many-tag-values).
:::

Одинаковый алгоритм применяется как перед отправкой агентами данных на агрегатор, так и перед вставкой агрегатором в 
базу данных.

### Нецелые коэффициенты семплирования

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

The number of occupied bytes depends on the [metric type](../guides/design-metric.md#metric-types) 
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

### User-guided sampling

Though it is better to let StatsHouse [sample](#sampling) data for you,
you may want to sample your data before sending them to StatsHouse.
Use this kind of sampling to control the memory footprint.

In this case, you can explicitly specify `counter` for the `value` metric:
```bash
`{"metrics":[{"name":"my_metric","tags":{},"counter":6, "value":[1, 2, 3]}]}`
```
This means that the number of events is 6, and the values are sampled—as if the original `value`
was `[1, 1, 2, 2, 3, 3]`

### Resolution

The highest available resolution of data to show on a graph depends on the currently available
[aggregate](#aggregation): you can get per-second data for the last two days, per-minute data for the last month,
and you can get per-hour data for any period you want.

If getting the highest available resolution is not crucial for you, but it is important for you
to reduce [sampling](#sampling), [reduce your metric resolution](../guides/edit-metrics.md#resolution).

For example, you may choose a custom resolution to make the [agent](components.md#agent)
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



