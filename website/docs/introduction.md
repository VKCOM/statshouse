---
slug: /
sidebar_position: 1 
---

import Intro from './img/intro.png'
import WhatIsSh from './img/what-is-sh.png'

# What is StatsHouse?

StatsHouse is a monitoring system that is highly available, scalable, and multitenant.
It is robust, and one can hardly overload it—due to [aggregation](conceptual-overview.md#aggregation) and 
[sampling](conceptual-overview.md#sampling).
Read more about StatsHouse [features](conceptual-overview.md#features).

It allows your applications to send metric data without limitations, and you can view statistics on a graph in a live 
mode.

<img src={WhatIsSh} width="1000"/>

## What you can do with StatsHouse

Here is what the StatsHouse user interface looks like and what it basically allows you to do:

<img src={Intro} width="900"/>

Check the links below to get the most out of StatsHouse.

### Try StatsHouse basics

Walk through the ten-minute [**Quick start**](quick-start.md) guide:
* get access to StatsHouse (run it locally or access the deployed one),
* create your own metric,
* send data to your metric,
* and [check basic viewing options](quick-start.md#check-basic-viewing-options).

### Find the guide for your task

Check the detailed **User guide** to perform popular tasks:

* [Get access to StatsHouse](guides/access-cluster.md)
* [Create a metric](guides/create-metric.md)
* [Send metric data](guides/send-data.md)
* [View data on a graph](guides/view-graph.md) ← <text className="orange-text">_the detailed UI description is 
  here_</text>
* [Edit a metric](guides/edit-metrics.md)
* [Create dashboards](guides/dashboards.md)
* [Query with PromQL](guides/query-wth-promql.md)

### Dive into details

Refer to the [**Conceptual overview**](conceptual%20overview/features.md) to handle complicated monitoring tasks:
* find more about StatsHouse [features](conceptual%20overview/features.md),
* understand the underlying [concepts](conceptual%20overview/concepts.md), 
* learn how the StatsHouse [components](conceptual%20overview/components.md) implement all of the above.

### Check FAQs

Find answers to [**Frequently asked questions**](faq.md).
