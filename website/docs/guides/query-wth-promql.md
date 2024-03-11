---
sidebar_position: 7
---

# Query with PromQL

This section tells you about using PromQL with StatsHouse:
<!-- TOC -->
* [What is PromQL?](#what-is-promql)
* [How to switch to PromQL mode](#how-to-switch-to-promql-mode)
* [What is specific about PromQL in StatsHouse?](#what-is-specific-about-promql-in-statshouse)
  * [The query result is an aggregate](#the-query-result-is-an-aggregate)
  * [The `__what__` selector: choosing the aggregate components](#the-what-selector-choosing-the-aggregate-components)
  * [Histograms are _t-digests_](#histograms-are-t-digests)
  * [No data grouping by default](#no-data-grouping-by-default)
<!-- TOC -->

## What is PromQL?

PromQL is the Prometheus Query Language that lets the user select time series data. 

Why have we decided to support querying with PromQL in StatsHouse? 
We wanted to broaden the range of things the users could do with data in StatsHouse.
PromQL provides users with the necessary operations, it is widely used and well-documented.

Find the original [PromQL documentation](https://prometheus.io/docs/prometheus/latest/querying/basics/).

## How to switch to PromQL mode

To switch to PromQL queries in StatsHouse, press the `< >` button near the _Metric name_ field.
Find more about the [PromQL mode in the UI](view-graph.md#18--query-with-promql).

## What is specific about PromQL in StatsHouse?

If you have been using PromQL before, you may be confused with some PromQL implementation details in StatsHouse. 
Let's make them clear.

* [The query result is an aggregate](#the-query-result-is-aggregation)—not an exact metric value per moment. 
* You can [choose the aggregate components](#the-what-selector-choosing-the-aggregate-components) using the `__what__` 
  selector.
* StatsHouse [histograms are t-digests](#histograms-are-t-digests).
* StatsHouse [does not group data by default](#no-data-grouping-by-default).

### The query result is an aggregate

The PromQL implementation in StatsHouse is related to the way of storing data in StatsHouse. Instead of 
storing `timestamp—value` pairs as in Prometheus, StatsHouse stores aggregated data per time intervals, or 
[_aggregates_](../conceptual%20overview/concepts.md#aggregation).

So, the query result in StatsHouse is an aggregate, and it depends on
* the _minimum available aggregation interval_ (how "old" the data is),
* the _requested aggregation interval_,
* the metric _resolution_.

An [aggregate](../conceptual%20overview/concepts.md#aggregation) contains the _count_, _sum_, _min_, _max_ 
statistics, and, optionally, the [_String top_](../conceptual%20overview/components.md#string-top-tag) tag (`tag_s`)
and [percentiles](edit-metrics.md#percentiles) (if enabled). 
They are _aggregate components_. For example:

| timestamp | metric     | tag_1 | tag_2 | tag_s | count | sum | min | max | percentiles |
|-----------|------------|-------|-------|-------|-------|-----|-----|-----|-------------|
| 13:45:05  | toy_metric | ...   | ...   | ...   | ...   | ... | ... | ... | ...         |


### The `__what__` selector: choosing the aggregate components

### Histograms are _t-digests_

### No data grouping by default
