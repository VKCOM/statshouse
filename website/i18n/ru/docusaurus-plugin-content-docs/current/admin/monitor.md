---
sidebar_position: 4
title: Мониторинг StatsHouse
---

import HeartbeatVersion from '../img/heartbeat-version.png'
import AggMetrics from '../img/agg-metrics.png'
import AggInsertMetrics from '../img/agg-insert-metrics.png'

# Мониторинг StatsHouse

Чтобы следить за состоянием StatsHouse, используйте готовые [служебные метрики](#служебные-метрики). Также можно
получить [логи](#логи).

## Служебные метрики

Метрика `__heartbeat_version` показывает количество запущенных компонентов. Сгруппируйте данные по тегу `component`:

<img src={HeartbeatVersion} width="700"/>

:::tip
Узнайте, как [проверить версию агента](maintain-upgrade.md).

Прочитайте, как [следить за состоянием агента](install.md#как-следить-за-состоянием-агентов).
:::

Чтобы найти метрики агрегаторов, введите префикс `__agg` в поле поиска:

<img src={AggMetrics} width="700"/>

Чтобы следить за вставкой данных в базу ClickHouse, используйте префикс `__agg_insert`:

<img src={AggInsertMetrics} width="150"/>

## Логи

По умолчанию StatsHouse выводит логи в `/dev/stdout`.

Рекомендуем использовать `Systemd Unit` при запуске агента и работать с логами при помощи сервиса `journal`.
