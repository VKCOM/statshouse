---
sidebar_position: 2
description: Learn how to create a metric via the StatsHouse UI.
---
import CreateMetric from '../img/create-metric.png'
import NameMetric from '../img/name-new-metric.png'

# Create a metric

[Check the typical questions](#frequent-questions) and [create a metric via the StatsHouse UI](#how-to-create-a-metric-in-the-ui).

## Frequent questions

Before you get your first metric in StatsHouse, you may have questions:

> "Can I skip creating a metric and start sending data to it right away?"

No. _Before_ sending data to a metric, you have to create a metric _manually via the StatsHouse UI_.
Otherwise, you will not be able to see it.

> "Can I automate creating metrics?"

No. You cannot automate creating metrics: the data you send should be properly formatted. Also, there are limits to 
creating metrics related to the StatsHouse mapping mechanism. 
See more about [mapping and budgets for creating metrics](../conceptual-overview/draft.md).

> "How many metrics can I create?"
 
In most cases, you should not worry about limits if you create metrics manually via the UI. If you are not sure, see 
more about [mapping and budgets for creating metrics](../conceptual-overview/draft.md).

> "What if I send too much data? Can I overload StatsHouse or spoil other metrics?"

Most likely, you cannot do anything wrong to StatsHouse or other users with your metrics. It is almost 
impossible to overload StatsHouse due to [aggregation] and [sampling]. StatsHouse also provides users with [fair 
resource sharing], so no metric can steal budget from the other one.

:::info
To learn more about mechanisms that make StatsHouse highly available and scalable, and how they may affect the 
resulting data, refer to the [conceptual overview].
:::

> "Can I delete a metric?"

No, but you can disable it. See [how to disable or enable a metric](configure-metric.md#disabling-a-metric).

## How to create a metric in the UI

To create a metric, give it a name:

1. In the StatsHouse UI, go to the main **⚡** menu in the upper-left corner and select **Create metric**:

<img src={CreateMetric} width="300"/>

2. In StatsHouse, there may be more than 10.000 metrics, so naming your metrics properly will help you and other 
users to find the necessary one.
If you have a group of metrics related to a particular product or a service, you can use prefixes or other 
specifications to name them consistently:

<img src={NameMetric} width="600"/>

As soon as your metric has a name, you should understand the [metric type](configure-metric.md#metric-type) and specify it in 
your sending requests. 
Then, you can [configure](configure-metric.md) more parameters for your metric, or start [sending data](send-data.md) 
right away.

