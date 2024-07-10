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

### [Краткое руководство](quick-start.md)

* [Запустите StatsHouse локально](quick-start.md#запустите-statshouse-локально) or 
[получите доступ к готовой инсталляции](quick-start.md#получите-доступ)
* [Отправьте данные с демосервера](quick-start.md#отправьте-данные-с-демосервера)
* [Создайте свою метрику](quick-start.md#создайте-свою-метрику)
* [Отправьте данные в свою метрику](quick-start.md#отправьте-данные-в-свою-метрику)
* [Что посмотреть на графике](quick-start.md#что-посмотреть-на-графике)

### [Пользователям](guides/access-cluster.md)

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

### [Администраторам](admin/sys-req.md)

* [Требования к системе](admin/sys-req.md)
* [Пакеты](admin/packages.md)
* [Установка компонентов](admin/install.md)
* [Мониторинг StatsHouse](admin/monitor.md)
* [Безопасность подключений](admin/security.md)
* [Хостовые метрики](admin/host-metrics.md)
* [Управление бюджетом](admin/manage-budgets.md)
* [Поддержка и обновление](admin/maintain-upgrade.md)
* [Переход с других систем](admin/migrating.md)
* [Подключение других систем](admin/integrations.md)

### [Обзор](overview/features.md)

* [Преимущества](overview/features.md)
* [Основные понятия](overview/concepts.md)
* [Компоненты](overview/components.md)

### [Поддержка](support.md)
