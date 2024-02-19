---
slug: /
sidebar_position: 1 
---

import Intro from './img/intro.png'
import WhatIsSh from './img/what-is-sh.png'

# What is StatsHouse?

StatsHouse is a monitoring system that is highly available, scalable, and multitenant.
One can hardly overload it—due to [aggregation](conceptual%20overview/concepts.md#aggregation) and 
[sampling](conceptual%20overview/concepts.md#sampling).
Read more about StatsHouse [features](conceptual%20overview/features.md).

It allows your applications to send metric data without limitations, and you can view statistics on a graph in a live 
mode.

<img src={WhatIsSh} width="1000"/>

## What you can do with StatsHouse

Here is [what the StatsHouse user interface looks like](guides/view-graph.md) and what it basically allows you to do:

<img src={Intro} width="900"/>

## What you can find in the docs

Use the **🔍 Search** bar in the upper-right corner, or check the table of contents below.

### [Quick start](quick-start.md)

* [Run StatsHouse locally](quick-start.md#run-statshouse-locally) or 
[access the deployed one](quick-start.md#get-internal-permissions)
* [Send metrics from a demo web server](quick-start.md#send-metrics-from-a-demo-web-server)
* [Create your own metric](quick-start.md#create-your-metric)
* [Send data to your metric](quick-start.md#send-data-to-your-metric)
* [Check basic viewing options](quick-start.md#check-basic-viewing-options)

### [User guide](guides/access-cluster.md)

* [Get access to StatsHouse](guides/access-cluster.md)
* [Design your metric](guides/design-metric.md) <text className="orange-text">← _understand what you want from 
  your metric_</text>
* [Create a metric](guides/create-metric.md)
* [Send metric data](guides/send-data.md)
* [View data on a graph](guides/view-graph.md) <text className="orange-text">← _the detailed UI description is 
  here_</text>
* [Edit a metric](guides/edit-metrics.md)
* [Create and view dashboards](guides/dashboards.md)
* [Query with PromQL](guides/query-wth-promql.md)

### [Conceptual overview](conceptual%20overview/features.md)

* Find more about StatsHouse [features](conceptual%20overview/features.md).
* Understand the underlying [concepts](conceptual%20overview/concepts.md).
* Learn how the StatsHouse [components](conceptual%20overview/components.md) implement all of the above.

### [FAQ](faq.md)

Find answers to [frequently asked questions](faq.md).
