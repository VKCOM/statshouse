---
sidebar_position: 2
title: TLDR
---

import TldrAggregation from './img/t-aggregation.png'
import TldrCardinality from './img/t-cardinality.png'
import TldrSf from './img/t-sf.png'
import TldrSfRes from './img/t-sf-result.png'

# TLDR

The main StatsHouse feature is the ability to "digest" as much data as you need and to display it in real time.
It is possible due to **aggregation** and **sampling**: StatsHouse has to "compress" and (sometimes) to discard data.

:::warning
You can't get rid of sampling in general. And most of the time you don't need to.
:::

To see whether StatsHouse suits you well, have a quick glance at its basic concepts and mechanisms. They are
described below — in a couple of words and pictures:

<!-- TOC -->
* [Aggregation](#aggregation)
* [Cardinality](#cardinality)
* [Sampling](#sampling)
    * [StatsHouse does not guarantee the absence of sampling](#statshouse-does-not-guarantee-the-absence-of-sampling)
    * [How to minimize sampling](#how-to-minimize-sampling)
    * [Things that do not minimize sampling](#things-that-do-not-minimize-sampling)
<!-- TOC -->

## Aggregation

> Learn more about [**aggregation**](overview/concepts.md#aggregation) in the conceptual overview. This is just the
> TLDR.

StatsHouse "collapses" rows of data **with the same tag values**. The counters are summed up.

<img src={TldrAggregation} width="800"/>

StatsHouse does NOT store an exact metric value per each moment. It stores the general statistics (_aggregate_ =
_count_, _sum_, _min_, _max_) for a time interval.

The maximum resolution is one second. Per-second data is available for two days, then the resolution decreases (to
minutes and hours).

## Cardinality

> Learn more about [**cardinality**](overview/concepts.md#cardinality) in the conceptual overview. This is just the
> TLDR.

_High cardinality_ data is data with many different tag values. It is poorly aggregated, takes up a lot of space —
StatsHouse has to sample it.

_Low cardinality_ data is aggregated (compressed) well, and StatsHouse usually does not have to sample it.

<img src={TldrCardinality} width="800"/>

## Sampling

> Learn more about [**sampling**](overview/concepts.md#sampling) in the conceptual overview. This is just the TLDR.

When the loads are too high, StatsHouse samples data — throws away random data **rows**. It multiplies the sums and
counts in the remaining rows by a sampling coefficient to preserve the average statistics.

:::warning
**StatsHouse does not guarantee the absence of sampling.**
:::

<img src={TldrSf} width="800"/>

<img src={TldrSfRes} width="800"/>

In most cases, users do not worry about sampling. And it still protects StatsHouse from overload.

### StatsHouse does not guarantee the absence of sampling

You can't get rid of sampling in general. StatsHouse is designed to be a communal cluster: the resource is shared
fairly between the tenants.

The tenant's resource is the share of total resource. If the total resource has already been shared between the
tenants in your organization and there are no more new ones, the tenants' metrics will not interfere with each
other, so you can try to [minimize sampling](#how-to-minimize-sampling).

When scaling the system (adding new demanding tenants) the actual budget in bytes may decrease. In real
organizations, one should increase the total resource by scaling the physical cluster.

### How to minimize sampling

StatsHouse samples data when you send more metric data than allowed. You can either decrease the amount of data sent
(1–3), or increase the budget (4).
1. _Minimize cardinality_ —
   [the number of tag values](guides/design-metric.md#how-many-tag-values).
   It is not the number of tags that matters. The number of tag values is what you should care about.

:::tip
Do not use `userID` or `hostname` as tag values. Use larger categories.

Categorize users by a _region_, _platform_, etc. — not by their personal IDs.

Categorize hosts by a _datacenter_, a _cluster_, etc. — not by their names.
:::

2. _Consider the metric type_ — it influences the amount of data sent:
    * [_counter_ metrics](guides/design-metric.md#counters) takes up less space than
      [_value_ metrics](guides/design-metric.md#value-metrics);
    * [enabling _percentiles_](guides/edit-metrics.md#percentiles) increases the amount of data sent — it may lead to
      increased sampling.
3. _Reduce the metric resolution_. Learn [how to customize resolution](guides/edit-metrics.md#resolution).
4. _Increase the budget_. Only administrators are allowed to [manage budgets](admin/manage-budgets.md) for
   specific metrics (as well as groups and namespaces). Use this option sparingly. 

:::tip
You can [enable the "Fair key tags" feature](./guides/edit-metrics.md#fair-key-tags) to share the budget fairly 
between the services sending data to the same metric.
Read more about [tag-level budgeting](./overview/concepts.md#tag-level-budgeting-fair-key-tags).
:::

### Things that do not minimize sampling

If you send a lot of rows and start writing less data to the same rows, the sampling rate will not reduce.

Imagine you send two data rows: `[a=1, b=2]` и `[a=1, b=3]` — they are different rows because they contain
different tag values. It does not matter if you write four values or one million values to these rows (it works for
_counter_ and _value_ types). But if your metric generates **two million rows per second**, they **will likely be
sampled**.

### Why StatsHouse cannot guarantee the absence of sampling

You can't get rid of sampling in general. StatsHouse is designed to work as a communal cluster: the resource is 
fairly distributed among tenants.

The resource allocated to a tenant is the fixed percentage of the total resource.
If the resource in your organization has already been distributed among tenants and there are no new ones, then 
tenants will not interfere with each other, and it is even possible to empirically [minimize sampling](#how-to-minimize-sampling).

Upon scaling (when new tenants appear), the actual budget in bytes may decrease. To solve this problem in the real 
organization, one should increase the total budget, i.e., physically scale up the cluster.
