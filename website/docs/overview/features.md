---
sidebar_position: 1
---

# Features

StatsHouse is a **highly available**, **scalable**, **multitenant** monitoring system:
* it delivers uninterrupted service even in the face of failures,
* it handles a growing amount of incoming data,
* it serves multiple groups of users while providing them with fair resource sharing.

Watch the [StatsHouse presentation](https://youtu.be/gs2_PGgPVwU) at the HighLoad 2022 conference (in Russian)
to learn more about StatsHouse features and implementation details.   

### Ready for production on a large scale

StatsHouse is the [vk.com](https://vk.com) main monitoring system. As of June 2024, the main StatsHouse cluster
receives 1.6 billion measurements per second from 28,000 servers and stores 5 years of data.

### Providing high-resolution, low-latency data

With the default one-second resolution and five-second latency, you can view almost real-time data in great detail.

### Ensuring long-term metric storage

StatsHouse automatically downsamples high-resolution data: per-second aggregated data is stored for the first two
days, per-minute data is stored for a month, per-hour data is available forever.

### Easy to operate

StatsHouse works around network unavailability and individual machine failure.
It operates correctly when the amount of inserted data increases sharply,
or when the errors in the client code appear.
Most StatsHouse components are quasi-stateless. The main stateful component is the
[ClickHouse](https://clickhouse.com) cluster, which is a primary StatsHouse data store.

### Overload-resistant

StatsHouse uses the explicitly configured per-second budgets for network and disk usage.
To avoid the tragedy of commons, StatsHouse provides users with fair resource sharing,
so no metric can steal budget from the other one.
If a metric sends too much data and exceeds the budget, StatsHouse automatically samples metric data.

### Offering a built-in user interface

With the built-in user interface (UI), you can view your metric data interactively—there is no need for
a query language. Get use of the StatsHouse dashboard system or a Grafana data source plugin.

To understand how these features are implemented, read more about the underlying [concepts](concepts.md) and 
architectural [components](components.md).
