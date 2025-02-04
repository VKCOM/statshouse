---
sidebar_position: 7
---

# Query with PromQL

This section tells you about using PromQL with StatsHouse:
<!-- TOC -->
* [What is PromQL?](#what-is-promql)
* [How to switch to a PromQL query editor](#how-to-switch-to-a-promql-query-editor)
* [What is specific about PromQL in StatsHouse?](#what-is-specific-about-promql-in-statshouse)
  * [The query result is an aggregate](#the-query-result-is-an-aggregate)
  * [`__what__` for choosing the aggregate components](#what-for-choosing-the-aggregate-components)
  * [Histograms are _t-digests_](#histograms-are-t-digests)
  * [No data grouping by default](#no-data-grouping-by-default)
* [PromQL extensions in StatsHouse](#promql-extensions-in-statshouse)
  * [`__what__` and `__by__`](#what-and-by)
  * [Applying dashboard variables](#applying-dashboard-variables)
  * [Range vectors and instant vectors](#range-vectors-and-instant-vectors)
  * [`prefix_sum`](#prefix-sum)
  * [`default`](#default)
<!-- TOC -->

## What is PromQL?

PromQL is the Prometheus Query Language that lets the user select time series data. 

Why have we decided to support querying with PromQL in StatsHouse? 
We wanted to broaden the range of things the users could do with data in StatsHouse.
PromQL provides users with the necessary operations, it is widely used and well-documented.

Find the original [PromQL documentation](https://prometheus.io/docs/prometheus/latest/querying/basics/).

## How to switch to a PromQL query editor

To switch to the PromQL query editor in StatsHouse, press the `< >` button near the _Metric name_ field.
Find more about the [PromQL query editor in the UI](view-graph.md#18--promql-query-editor).

## What is specific about PromQL in StatsHouse?

If you have been using PromQL before, you may be confused with some PromQL implementation details in StatsHouse. 
Let's make them clear.

* [The query result is an aggregate](#the-query-result-is-an-aggregate)—not an exact metric value per moment. 
* You can [choose the aggregate components](#what-for-choosing-the-aggregate-components) using the `__what__` selector.
* StatsHouse [histograms are _t-digests_](#histograms-are-t-digests).
* StatsHouse [does not group data by default](#no-data-grouping-by-default).

### The query result is an aggregate

Prometheus stores `timestamp—value` pairs. Instead, StatsHouse stores aggregated data per time intervals, or 
[_aggregates_](../overview/concepts.md#aggregate).

So, the query result in StatsHouse is an aggregate, and it depends on
* the [_minimal available aggregation interval_](../overview/concepts.md#minimal-available-aggregation-interval)
(i.e., on the "age" of the data),
* the [_requested aggregation interval_](view-graph.md#6--aggregation-interval),
* the metric [_resolution_](../overview/concepts.md#resolution).

An [aggregate](../overview/concepts.md#aggregate) contains the _count_, _sum_, _min_, _max_ 
statistics, and, optionally, the [_String top_](../overview/components.md#string-top-tag) tag (`tag_s`)
and [percentiles](edit-metrics.md#percentiles) (if enabled). 
They are _aggregate components_:

| timestamp | metric     | tag_1 | tag_2 | tag_s | count | sum | min | max | percentiles |
|-----------|------------|-------|-------|-------|-------|-----|-----|-----|-------------|
| 13:45:05  | toy_metric | ...   | ...   | ...   | ...   | ... | ... | ... | ...         |

Read more about [aggregation](../overview/concepts.md#aggregation) in StatsHouse.

### ___what___ for choosing the aggregate components

* In Prometheus, you can query the exact values. In Prometheus `timestamp—value` pairs, the 
value is the floating-point number associated with a moment in time.
* In StatsHouse, you can query the aggregates associated with time intervals.

To query the aggregate components, use the  `__what__` selector. The possible values are:
```
"avg"
"count"
"countsec"
"max"
"min"
"sum"
"sumsec"
"stddev"
"stdvar"
"p25"
"p50"
"p75"
"p90"
"p95"
"p99"
"p999"
"cardinality"
"cardinalitysec"
"unique"
"uniquesec"
```

They are the [descriptive statistics](view-graph.md#3--descriptive-statistics) you see in the StatsHouse UI.
The "sec" postfix means that the value is normalized—divided by the aggregation interval in seconds.

For example, this selector returns the counter for the `api_requests` metric associated with the aggregation interval:
```
api_requests{__what__="count"}
```

If the  `__what__` selector is not specified, StatsHouse tries to guess based on the PromQL functions you use in your 
query:

| PromQL functions                                                         | StatsHouse interpretation |
|--------------------------------------------------------------------------|---------------------------|
| "increase"<br/>"irate"<br/>"rate"<br/>"resets"                           | `__what__="count"`        |
| "delta"<br/>"deriv"<br/>"holt_winters"<br/>"idelta"<br/>"predict_linear" | `__what__="avg"`          |

For example, this query returns the `api_requests` metric's counter rate for five minutes:

```
rate(api_requests[5m])
```

If StatsHouse fails to guess, it returns the counter for the [_counter_ metrics](design-metric.md#counters) 
and the average (the sum divided by the counter) for the [_value_ metrics](design-metric.md#value-metrics).

### Histograms are _t-digests_

Prometheus provides you with "traditional" and "native" histograms. StatsHouse will soon support the 
"traditional" ones. More details will be provided when the 
[scraping](../admin/migrating.md#how-to-migrate-from-prometheus) feature is implemented. Now it is recommended to 
use StatsHouse histograms.

StatsHouse stores histograms in the _t-digest_ structure but does not provide them by default—you should 
[enable writing percentiles](edit-metrics.md#percentiles).

To get access to percentiles (if enabled), specify the necessary one in the `__what__` selector:
```
"p25"
"p50"
"p75"
"p90"
"p95"
"p99"
"p999"
```

For example, this expression returns the 99th percentile:

```
api_requests{__what__="p99"}
```

### No data grouping by default

If you query data in Prometheus by a metric name, it returns all the data rows for this metric—all label combinations.

On the contrary, StatsHouse returns the result of aggregation. For example, in StatsHouse, the "api_requests" query 
returns the single row.
To group data by tags, specify the necessary ones using the `__by__` PromQL operator.

## PromQL extensions in StatsHouse

Find PromQL extensions implemented in StatsHouse.

### ___what___ and ___by___

The [`__what__`](#what-for-choosing-the-aggregate-components) and [`__by__`](#no-data-grouping-by-default)
selectors help to express any standard query in StatsHouse.

### Applying dashboard variables

To bind the tag to the previously created variable in your PromQL query, use the following syntax:

`tag_name:$variable_name`

The resulting query may look like this:

`topk(5,api_requests{@what="countsec",0:$env})`

Find more about [setting up variables for PromQL-based graphs and dashboards](dashboards.md#set-up-promql-based-dashboards).

### Range vectors and instant vectors

Functions for the range vectors receive instant vectors too. But the converse is false.

### _Prefix sum_

The `prefix_sum` function allows you to calculate a prefix sum. For example,
for a `1, 2, 3, 4, 5, 6` sequence, it returns the following: `1, 3, 6, 10, 15, 21`.

### _default_

It is a binary operator. It has an array on the left, and an array or a literal on the right.
* If it has the literal on the right, the `NaN` values on the left are replaced with the literal.
* If it has the array on the right, the logic of mapping the arrays is the same as for the `or` operator. The `NaN` 
  values on the left are replaced with the corresponding values on the right.
