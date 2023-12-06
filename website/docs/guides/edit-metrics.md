---
sidebar_position: 3
description: Get to know how to set up metric types, tags, resolution, and more.
---

import MetricTypes from '../img/metric-types.png'

# Edit metrics


You have just created a metric—it now has a name and nothing more.

:::info 
For a created metric, you have to choose the [metric type](#metric-type). This is the minimal configuration needed 
to start sending data. 
Setting up other metric parameters is optional.
:::

Before you start configuring metrics, you may have a question:

> "Can I skip configuration for my metric?"

No. As soon as your metric has a name, you have to specify a [metric type](#metric-type) in your sending requests.
Then, you can configure more parameters for advanced usage, or start sending data right away.

For a full list of configuration options, see the upper-right navigation menu on this page.

:::warning
Do not commit configuration changes or send data to someone else's metric as you can spoil the metric or the related data.
:::

## Metric type

> "What is a metric type? Why do I have to specify it?"

A metric is a [system for measuring something](https://dictionary.cambridge.org/dictionary/english/metric)—it 
is _how you measure_ the things you are interested in.

You can measure same things in different ways. To evaluate service availability, you can count the 
number of handled requests, which is just a number of events; or you can measure processing time for these requests, 
which is the value accompanying each event (each request).

StatsHouse is a powerful tool, but it cannot decide for you, so you should clearly understand what you want to 
measure, and how you will measure it.

:::important
A metric type affects the range of
[descriptive statistics](view-graph#desriptive-statistics-available-for-a-metric) available for your metric to view 
and analyze. For example, you cannot calculate the sum for unique metrics (you can, but this sum is meaningless); 
and percentiles are available for values only.

See more about [enabling percentiles](#enable-percentiles) 
and [showing the proper descriptive statistics](#aggregation) in the UI. 
:::

> "Which metric types are available in StatsHouse?"

With StatsHouse, you can use three basic metric types.

<img src={MetricTypes} width="800"/>

See the table below for definitions and examples:

| Metric type | What does it measure?                                                                     | Examples                                                                                                                                        |
|-------------|-------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Counter     | It counts the number of times an event has occurred.                                      | The number of API method calls<br/>The number of requests to a server<br/>The number of errors received while sending messages                  |
| Value       | It measures magnitude of a parameter.<br/>A measurement event itself is counted as well.  | How long does it take <br/>for a service to generate a newsfeed?<br/>What is CPU usage for this host?<br/>What is the response size (in bytes)? |
| Unique      | It counts the number of unique events.<br/>The total number of events is counted as well. | The number of unique users who sent packages to a service                                                                                       |

Value and unique metrics have an ordinary counter inside, so you should not implement your own counters 
for these metric types. Imagine you measuring a value metric (e.g. the response size in bytes) once in a second:
* You get the "value level" that is your parameter magnitude:
1024 bytes, then 2048 bytes, etc. Please note that this "level" is [aggregation], not an exact value for a 
  particular moment in time.
* You also get the "counter" for your value metric that shows the number of times you performed 
your measurements: +1 for the first second, +1 for the next one, etc. 

The same applies to unique metrics: they provide you with the number of unique events and the total number of events.
See more on 
[changing or combining metric types](#changing-or-combining-metric-types)
and [user-guided sampling](#user-guided-sampling).

:::note
Metric types should not be confused with [data types](https://en.wikipedia.org/wiki/Data_type) in programming
languages. You should not specify the type of your data: whether it is `int`, `float`, or `double`, etc.
:::

> "OK. How can I configure my metric type?"

You should specify a [metric type](#metric-type) in your sending requests.

For a toy example, you may send your metric data with one of these `bash` scripts:
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```
```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"value":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```
where `counter` or `value` are the metric types.

You can also use one of the client libraries. For example, with the
[StatsHouse client library for Python](https://github.com/VKCOM/statshouse-py/tree/master), you have 
to choose one of these methods according to your metric type:
```Python
statshouse.count("my_metric", {}, 0.42)
```
```Python
statshouse.value("my_metric", {}, 0.42)
```
```Python
statshouse.unique("my_metric", {}, 0.42)
```
Having done this, you may send your data, or continue configuring metric parameters.

## Tags


Why to use tags for your metrics

Человек группирует по тегам, например, ищет причину провала

Хороший дизайн метрики - хорошие теги. Теги позволяют категоризовать данные

Ограничения: число тегов. Но много чего можно сделать

ЧТо лучше: создавать несколько метрик или одну метрику переиспользовать с кучей тегов






https://prometheus.io/docs/practices/naming/

Use labels to differentiate the characteristics of the thing that is being measured:

api_http_requests_total - differentiate request types: operation="create|update|delete"
api_request_duration_seconds - differentiate request stages: stage="extract|transform|load"
Do not put the label names in the metric name, as this introduces redundancy and will cause confusion if the respective labels are aggregated away.

Remember that every unique combination of key-value label pairs represents a new time series, which can dramatically
increase the amount of data stored. Do not use labels to store dimensions with high cardinality (many different
label values), such as user IDs, email addresses, or other unbounded sets of values.

https://www.robustperception.io/cardinality-is-key/

> "How many tags are allowed?"
 

> "How many tag _values_ are allowed?"




You can [start sending data](send-data.md) to your metric with no additional configuration.
StatsHouse does everything for you, but it may lead to increased sampling. In most cases, you do not need
to worry about sampling, but if you are not sure, check the [conceptual overview].

:::info
Sampling prevents StatsHouse from database overload. If you send too much, StatsHouse _samples_
data: it removes random data rows and multiplies the remaining ones so that the resulting aggregation and
digests stay the same. Find more about [sampling], [aggregation], and [digests].
:::




> "What are tag ID, name, and description?"


> "What are _Raw_ tags and value comments"


Mapping - в БД храним числа (меньше места занимают, экономия на чтении и записи)
Строки переводятся в числа 32-разрядные - ограничиваем создание mapping-а
Необходима правильная разметка данных - не разрешаем автоматически создавать метрики
16 тегов (мб будет больше) - в Кликхаусе надо сразу сказать, сколько будет колонок
Если в качестве тега user id   - получаем mapping error
Receive status загорается, когда мы не смогли распарсить данные
Sampling source/aggregator - на чем семплируется
Mapping status - руками создаем метрики
Если много создаем, если исчерпали бюджет, то строку (тег) выкидываем - получаем empty
Загорелось: если писать в качестве тега user id - исчерпается бюджет строк
(про теги) Raw  - когда прислали строку - перевести в число. Raw - можно использовать как есть
Если шлем строку, а стоит Raw - получаем ошибки Receive status
С помощью Raw экономим бюджет на строки
Если тег - метрика, то что??
Вначале пользователи вообще не знали про mapping - фигачили подряд - быстро исчерпывали бюджет
Сделали Raw
неудобно смотреть числа
Сделали comment - хранятся не там, где маппинг (он хранится в метаданных)

Host (комп): вместо того, чтобы писать тег "host", нужно использовать готовую фичу — с какого компа чаще всего приходит максимальная строчка

Кардинальность — сколько уникальных комбинаций значений тегов. Каждому сочетанию соответствует строчка в базе.


Пользователи часто хотят добавить как можно больше тегов в метрики
Динамическое возрастание объема данных: например, сервис создает ключи, например, ID. Представим, что сервис стал популярен...
Можно писать одну и ту же метрику — много тегов, кардинальность высокая — она семплируется, запросы будут медленные
Для постоянного использования всего пара основных ключей, но семплироваться не будет


Ряд - последовательность значений с тегаами (ключ - значение)
Имя метрики - тоже тег
Бюджет - сколько в секунду передать данных (разные бюджеты на агенте и агрегаторе)
Нужно сохранить статистику: случайным образом выкидываем, а потом счетчики и сумму домножаем - семплирование

Наши разные либы по-разному отправляют пакеты на события
Сейчас семплирование работает только на уровне метрик
Например, пришли 10 комбинаций тегов - это 10 рядов
Если низкая кардинальность - легко агрегируем, не надо семплировать. Высокая кардинальность - надо семплировать
Хуже всего, когда в тег пишут ID пользователя - не сжимается, надо семплировать

Создать метрику - назвать ее и проименовать теги (добавляется алиас - говорящее имя)
тег0 - всегда  env
(Кликхаус хранит данные по столбикам, теги - столбики, данные группируются по тегам)
Хорошо, когда вначале идут теги с меньшей кардинальностью (env-тов у нас мало)

tag_1 - alias - description
Value comment - mapping



**Как  выглядит настройка тегов в ЮИ и в коде**



## Description
какие символы использовать

какой длины текст
кто читает и зачем

Description is for UI only. New lines are respected, no other formatting supported yet.

## Aggregation
CounterValueUniqueMixed

## Percentiles

Aggregation defines which functions (count, avg, sum, etc.) are available in UI. Mixed allows all functions. Enabling percentiles greatly increase data volume collected.
Resolution
1 second (native, default)2 seconds3 seconds4 seconds5 seconds (native)6 seconds10 seconds12 seconds15 seconds (native)20 seconds30 seconds60 seconds (native)
If your metric is heavily sampled, you can trade time resolution for reduced sampling. Selecting non-native resolution might render with surprises in UI.
Unit
no unitsecondmillisecondmicrosecondnanosecondbyte
The unit in which the metric is written
Weight

Important metrics can have their data budget proportionally increased. Will reduce other metrics budgets so can be enabled only by administrator.
Tags


## Resolution

## Units
no unitsecondmillisecondmicrosecondnanosecondbyte
The unit in which the metric is written

## Weight

Important metrics can have their data budget proportionally increased. Will reduce other metrics budgets so can be enabled only by administrator.




## Metric groups



## Advanced metric options

### Changing or combining metric types

### User-guided sampling



## Disabling a metric

Disabling metric stops data recording for this metric and removes it from all lists.
This is most close thing to deleting metric (which statshouse does not support).
You will need a direct link to enable metric again.

If you disable a metric, you stop recording data for it and remove it from the metric
list. You can enable the metric only via the direct link.
