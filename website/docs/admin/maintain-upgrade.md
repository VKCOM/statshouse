---
sidebar_position: 8
title: Maintain and upgrade
---

import AgentVersion from '../img/agent-version.png'

# Maintain and upgrade

As for maintaining the system, you should regularly upgrade the agents on your machines.
In the nearest future, we plan to prevent the agents with the outdated version from sending data to StatsHouse.

:::important
We assume that upgrading should be done within **one month** since the [new version](packages.md) is available.
:::

To check the current version of your agents, view the `__heartbeat_version` [service metric](monitor.md#service-metrics).

The `commit_hash` tag is for the build versions (the `Build Commit` tag is the full hash), see the 
`commit_timestamp` or the `commit_date` tag for the build date:

<img src={AgentVersion} width="700"/>

Read more about [finding the packages](packages.md).
