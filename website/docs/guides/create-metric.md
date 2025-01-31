---
sidebar_position: 3
---
import CreateMetric from '../img/create-metric.png'
import NameMetric from '../img/name-new-metric.png'

# Create a metric

In this section, you will find:
<!-- TOC -->
* [How to create a metric in the UI](#how-to-create-a-metric-in-the-ui)
* [FAQs about creating metrics](#faqs-about-creating-metrics)
    * ["Can I skip creating a metric and start sending data to it right away?"](#can-i-skip-creating-a-metric-and-start-sending-data-to-it-right-away)
    * ["Can I automate creating metrics?"](#can-i-automate-creating-metrics)
    * ["How many metrics can I create?"](#how-many-metrics-can-i-create)
    * ["What if I send too much data? Can I overload StatsHouse or spoil other metrics?"](#what-if-i-send-too-much-data-can-i-overload-statshouse-or-spoil-other-metrics)
    * ["Can I rename a metric?"](#can-i-rename-a-metric)
    * ["I want to re-design my metric. Should I refactor it or create a new one?"](#i-want-to-re-design-my-metric-should-i-refactor-it-or-create-a-new-one)
    * ["Can I delete a metric?"](#can-i-delete-a-metric)
<!-- TOC -->

## How to create a metric in the UI

To create a metric, give it a name.

In the StatsHouse UI, go to the main **⚡** menu in the upper-left corner and select **Create metric**:

<img src={CreateMetric} width="300"/>

Please avoid creating "one big metric" for the whole system. 
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

As soon as your metric has a name, you can start [sending data](send-data.md).

## FAQs about creating metrics

Before you get your first metric in StatsHouse, you may have questions:

#### "Can I skip creating a metric and start sending data to it right away?"

Yes, but... you will not be able to see your data! _Before_ sending data to a metric, you have to create a metric 
_manually via 
the StatsHouse UI_.

#### "Can I automate creating metrics?"

No. You cannot automate creating metrics. We want each metric in StatsHouse to have its owner and to be created 
deliberately.

#### "How many metrics can I create?"

You can create as many metrics as you wish as soon as you do it manually via the StatsHouse UI.

#### "What if I send too much data? Can I overload StatsHouse or spoil other metrics?"

Most likely, you cannot do anything wrong to StatsHouse or other users with your metrics. It is almost
impossible to overload StatsHouse due to [aggregation](../overview/concepts.md#aggregation)
and [sampling](../overview/concepts.md#sampling).
StatsHouse provides users with [fair resource sharing](../overview/concepts.md#fair-resource-sharing),
so no metric can steal budget from the other one.

:::info
To learn more about mechanisms that make StatsHouse highly available and scalable, and how they may affect the
resulting data, refer to the [conceptual overview](../overview/concepts.md).
:::

The rare case of losing or spoiling metric data is related to 
[UDP socket buffer overflow](send-data.md#how-to-send-data-without-client-libraries). 
Most likely, you should not worry about it.

#### "Can I rename a metric?"

You cannot rename an existing metric, so please create a new one with the name you want. You can
[edit the graph name](view-graph.md#2--graph-name-and-description) so that the metric name remains the same.
[Disable](edit-metrics.md#disabling-a-metric) an unused metric.

#### "I want to re-design my metric. Should I refactor it or create a new one?"

If you refactor your existing metric, i.e., switch between different metric types for a single metric, the data may
become confusing or uninformative.

If you create a new metric, please note that you cannot reuse the name of
an existing (even disabled) metric.

#### "Can I delete a metric?"

No, but you can disable it. Disabling a metric stops writing data for it and removes it from the metric list.
See [how to disable or enable a metric](edit-metrics.md#disabling-a-metric).
