---
slug: /
sidebar_position: 1
toc_max_heading_level: 2
---

import Intro from './img/intro.png'
import WhatIsSh from './img/what-is-sh.png'
import { Icon } from '@iconify/react';

# What is StatsHouse?

StatsHouse is an [open-source](https://github.com/VKCOM/statshouse) monitoring system that is highly available, scalable, and multitenant.
One can hardly overload it—due to [aggregation](overview/concepts.md#aggregation) and
[sampling](overview/concepts.md#sampling).
It allows your applications to send metric data without limitations, and you can view statistics on a graph in a live
mode.

<img src={WhatIsSh} width="1000"/>

:::tip
* [View the conceptual **TLDR**](tldr.md).
* [Start using **StatsHouse in 10 minutes**](quick-start.md).
* Proceed to [**User guide**](guides/access-cluster.md) or [**Administrator guide**](admin/sys-req.md).
  :::

Find StatsHouse on [<Icon icon="octicon:mark-github-24" /> GitHub](https://github.com/VKCOM/statshouse).

## What you can do with StatsHouse

Here is [what the StatsHouse user interface looks like](guides/view-graph.md) and what it basically allows you to do:

<img src={Intro} width="900"/>

You have three options to use StatsHouse:
- [run StatsHouse locally](quick-start.md),
- [get access to a StatsHouse cluster deployed in your organization](guides/access-cluster.md),
- or [deploy your own StatsHouse cluster](admin/install.md).

## What you can find in the docs

Use the **🔍 Search** bar in the upper-left corner, or check the table of contents below.

### [TLDR](tldr.md)

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
* [Find OpenAPI specification](guides/openapi.md)

### [Administrator guide](admin/sys-req.md)

* [Check system requirements](admin/sys-req.md)
* [Find packages](admin/packages.md)
* [Install components](admin/install.md)
* [Monitor StatsHouse](admin/monitor.md)
* [Ensure security](admin/security.md)
* [Use host metrics](admin/host-metrics.md)
* [Manage budgets](admin/manage-budgets.md)
* [Maintain and upgrade](admin/maintain-upgrade.md)
* [Migrate from the other monitoring system](admin/maintain-upgrade.md)
* [Integrate the external systems](admin/integrations.md)

### [Overview](overview/features.md)

* Find more about StatsHouse [features](overview/features.md).
* Understand the underlying [concepts](overview/concepts.md).
* Learn how the StatsHouse [components](overview/components.md) implement all of the above.

### [Support](support.md)
