---
sidebar_position: 2
title: System requirements
---

import ShardReplicas from '../img/shard-replicas.png'
import LocalDisks from '../img/local-disks.png'

# System requirements

**Software:** you can install StatsHouse components on the Linux systems.

**Hardware:** please check the recommendations below regarding the [ClickHouse machines](#clickhouse-machines),
[backup disks](#backup-disks), and [cloud installations](#cloud-installations).

## ClickHouse machines

1. Your hardware should comply with the
   [requirements for running ClickHouse database](https://clickhouse.com/docs/ru/operations/requirements).
2. Each shard should have at least three replicas. You can have any number of shards (one or more).

<img src={ShardReplicas} width="500"/>

Read more about
[distributing data between the replicas](conceptual%20overview/components.md#handling-aggregators-shutdown).

## Backup disks

If the aggregator is unavailable, responds with an error, or cannot insert data into the ClickHouse database,
StatsHouse stores data locally — the local disks prevent you from losing data:

<img src={LocalDisks} width="500"/>

:::important
Make sure there is enough space on a disk to store data resulting from **six hours** of the agent or the aggregator
working.
:::

Read more about [handling aggregator's shutdown](conceptual%20overview/components.md#handling-aggregators-shutdown).

## Сloud installations

StatsHouse is not a cloud-native product. We recommend you to install StatsHouse components on the hardware, not in
the pods or the short-lived virtual machines. If you use virtual machines, they should be full analogues of the
physical hardware.

Read more about
[deploying agents in the Kubernetes pods](conceptual%20overview/components.md#deploying-agents-in-the-kubernetes-pods).
