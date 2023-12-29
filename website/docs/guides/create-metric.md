---
sidebar_position: 2
description: Learn how to create a metric via the StatsHouse UI.
---
import CreateMetric from '../img/create-metric.png'
import NameMetric from '../img/name-new-metric.png'

# Create a metric

[Create a metric via the StatsHouse UI](#how-to-create-a-metric-in-the-ui). If you have questions, please [check the 
typical ones](#frequent-questions).

## How to create a metric in the UI

To create a metric, give it a name.

In the StatsHouse UI, go to the main **⚡** menu in the upper-left corner and select **Create metric**:

<img src={CreateMetric} width="300"/>

Please [avoid creating "one big metric"](send-data.md#can-i-change-or-combine-metric-types) for the whole system. 
Instead, create several metrics, named consistently. For such a group of metrics, related to a particular system (a 
product or a service), use prefixes or other specifications:

<img src={NameMetric} width="600"/>

Please use these characters:
* Latin alphabet
* integers
* underscores

:::note
Do not start metric names with underscores. They are for StatsHouse internal use only.
:::

As soon as your metric has a name, choose the [metric type](edit-metrics.md#metric-type) and start [sending data](send-data.md).

## FAQs about creating metrics

Before you get your first metric in StatsHouse, you may have questions:

#### "Can I skip creating a metric and start sending data to it right away?"

No. _Before_ sending data to a metric, you have to create a metric _manually via the StatsHouse UI_.
Otherwise, you will not be able to see it.

#### "Can I automate creating metrics?"

No. You cannot automate creating metrics: the data you send should be properly formatted. Also, there are limits to
creating metrics related to the StatsHouse mapping mechanism.
See more about [mapping and budgets for creating metrics](../conceptual-overview.md#mapping-and-budgets-for-creating-metrics).

#### "How many metrics can I create?"

In most cases, you should not worry about limits if you create metrics manually via the UI. If you are not sure, see
more about [mapping and budgets for creating metrics](../conceptual-overview.md#mapping-and-budgets-for-creating-metrics).

#### "What if I send too much data? Can I overload StatsHouse or spoil other metrics?"

Most likely, you cannot do anything wrong to StatsHouse or other users with your metrics. It is almost
impossible to overload StatsHouse due to [aggregation](../conceptual-overview.md#aggregation)
and [sampling](../conceptual-overview.md#sampling).
StatsHouse also provides users with [fair resource sharing](../conceptual-overview.md#fair-resource-sharing),
so no metric can steal budget from the other one.

:::info
To learn more about mechanisms that make StatsHouse highly available and scalable, and how they may affect the
resulting data, refer to the [conceptual overview](../conceptual-overview.md).
:::

#### "Can I delete a metric?"

No, but you can disable it. See [how to disable or enable a metric](edit-metrics.md#disabling-a-metric).

#### "I want to re-design my metric. Should I refactor it or create a new one?"

If you refactor your existing metric, the data may become confusing or uninformative.
If you create a new metric, please note that you cannot reuse the name of
an existing (even disabled) metric.
