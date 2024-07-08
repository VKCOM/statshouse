---
slug: /
sidebar_position: 1
toc_max_heading_level: 2
---

import Intro from './img/intro.png'
import WhatIsSh from './img/what-is-sh.png'
import { Icon } from '@iconify/react';


# Что такое StatsHouse?

StatsHouse — система мониторинга с [открытым исходным кодом](https://github.com/VKCOM/statshouse). Что в ней 
особенного? 

* Высокая доступность
* Лёгкое масштабирование
* Мультитенантность

StatsHouse не сломается из-за слишком 
высокой нагрузки, потому что использует [агрегацию](overview/concepts.md#агрегация) и 
[семплирование](overview/concepts.md#семплирование).
Так что отправляйте сколько угодно данных и просматривайте графики в режиме реального времени.

Почитайте о [преимуществах](overview/features.md) StatsHouse или перейдите в наш репозиторий на
[<Icon icon="octicon:mark-github-24" /> GitHub](https://github.com/VKCOM/statshouse).

<img src={WhatIsSh} width="1000"/>

## Что можно делать в StatsHouse

Вот [как выглядит интерфейс StatsHouse](guides/view-graph.md). И вот что он позволяет (в общих чертах):

<img src={Intro} width="900"/>

Чтобы работать со StatsHouse, можно
- [запустить StatsHouse локально](quick-start.md),
- [получить доступ к уже развёрнутому кластеру StatsHouse](guides/access-cluster.md),
- или [развернуть свой кластер](admin/install.md).

## Про что написано в документации

Чтобы найти нужную информацию, используйте **🔍 Поиск** справа вверху.

А вот список разделов:

### [Quick start](quick-start.md)

* [Run StatsHouse locally](quick-start.md#запустите-statshouse-локально) or 
[access the deployed one](quick-start.md#получите-доступ)
* [Send metrics from a demo web server](quick-start.md#отправьте-данные-с-демосервера)
* [Create your own metric](quick-start.md#создайте-свою-метрику)
* [Send data to your metric](quick-start.md#отправьте-данные-в-свою-метрику)
* [Check basic viewing options](quick-start.md#что-посмотреть-на-графике)

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
