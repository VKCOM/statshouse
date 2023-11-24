---
sidebar_position: 2
---
import CreateMetric from '../img/create-metric.png'
import NameMetric from '../img/name-new-metric.png'

# Create a metric

:::info
- _**Before** sending data to a metric, create a metric manually via the StatsHouse UI_.
- You cannot automate creating metrics. See more about
  [mapping and budgets for creating metrics](../conceptual-overview/draft.md).
- _It is possible to [disable](configure-metric.md#disable-a-metric) a metric but not to delete._
:::

To create a metric, give it a name:

1. In the StatsHouse UI, go to the main **⚡** menu in the upper-left corner and select **Create metric**:

<img src={CreateMetric} width="300"/>

2. In StatsHouse, there may be more than 10.000 metrics, so naming your metrics properly will help you and other 
users to find the necessary one.
If you have a group of metrics related to a particular product or a service, you may use prefixes or other 
specifications to name them consistently:

<img src={NameMetric} width="600"/>

You can proceed to [configure the metric](configure-metric.md) via the UI, or [start sending data](send-data.md) 
to it—with minimal configuration right from your code.





