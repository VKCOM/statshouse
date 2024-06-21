---
sidebar_position: 4
title: Administrator guide ⭐
---

import LocalOrCluster from './img/local-or-cluster.png'
import ShardReplicas from './img/shard-replicas.png'
import LocalDisks from './img/local-disks.png'
import ComponentsOrder from './img/components-order.png'
import HeartbeatVersion from './img/heartbeat-version.png'
import AggMetrics from './img/agg-metrics.png'
import AggInsertMetrics from './img/agg-insert-metrics.png'
import Aes from './img/aes.png'
import NoProxyEncryption from './img/no-proxy-encryption.png'
import WithProxyEncryption from './img/with-proxy-encryption.png'
import Namespace from './img/namespace.png'
import NamespaceAdd from './img/namespace-add.png'
import GroupName from './img/group-name.png'
import GroupAdd from './img/group-add.png'
import EditWeight1 from './img/edit-weight-1.png'
import EditWeight2 from './img/edit-weight-2.png'
import Weight from './img/weight.png'


# Administrator guide

You have three options to use StatsHouse:
- [run StatsHouse locally](quick-start.md),
- [get access to a StatsHouse cluster deployed in your organization](guides/access-cluster.md),
- or deploy your own StatsHouse cluster.

<img src={LocalOrCluster} width="900"/>

This guide will help you to deploy and maintain your own StatsHouse cluster on your machines.

**Before you begin:**

* [System requirements](#system-requirements)

**Basic how-tos:**

* [How to find packages](#how-to-find-packages)
* [How to install](#how-to-install)
* [How to ensure security](#how-to-ensure-security)
* [How to configure](#how-to-configure)
* [How to monitor](#how-to-monitor)
* [How to manage users](#how-to-manage-users)
* [How to maintain and upgrade](#how-to-maintain-and-upgrade)

**Additional:**

* [Troubleshooting](#troubleshooting)
* [Integrations](#integrations)
* [Migrating from the other monitoring system](#migrating-from-the-other-monitoring-system)
* [Support and resources](#support-and-resources)

## System requirements

**Software:** you can install StatsHouse components on the Linux systems.

**Hardware:** please check the recommendations below regarding the [ClickHouse machines](#clickhouse-machines), 
[backup disks](#backup-disks), and [cloud installations](#cloud-installations).

### ClickHouse machines

1. Your hardware should comply with the
[requirements for running ClickHouse database](https://clickhouse.com/docs/ru/operations/requirements).
2. Each shard should have at least three replicas. You can have any number of shards (one or more).

<img src={ShardReplicas} width="500"/>

Read more about 
[distributing data between the replicas](conceptual%20overview/components.md#handling-aggregators-shutdown).

### Backup disks

If the aggregator is unavailable, responds with an error, or cannot insert data into the ClickHouse database, 
StatsHouse stores data locally — the local disks prevent you from losing data:

<img src={LocalDisks} width="500"/>

:::important
Make sure there is enough space on a disk to store data resulting from **six hours** of the agent or the aggregator 
working.
:::

Read more about [handling aggregator's shutdown](conceptual%20overview/components.md#handling-aggregators-shutdown).

### Сloud installations

StatsHouse is not a cloud-native product. We recommend you to install StatsHouse components on the hardware, not in 
the pods or the short-lived virtual machines. If you use virtual machines, they should be full analogues of the 
physical hardware.

Read more about 
[deploying agents in the Kubernetes pods](conceptual%20overview/components.md#deploying-agents-in-the-kubernetes-pods).

## How to find packages

Find the StatsHouse packages in the public _vkpartner_ repositories:
* [for the DEB packages](https://artifactory-external.vkpartner.ru/ui/repos/tree/General/debian-statshouse),
* [for the RPM packages](https://artifactory-external.vkpartner.ru/ui/repos/tree/General/rpm-statshouse),

or in the [GitHub Releases](https://github.com/VKCOM/statshouse/releases) section.

## How to install

Here is the flow to install StatsHouse components (1 → 5):

<img src={ComponentsOrder} width="500"/>

The [local StatsHouse installation](quick-start.md) is almost the same. The only difference is that 
the [local installation script](https://github.com/VKCOM/statshouse/blob/master/localrun.sh) puts the ClickHouse 
database and aggregator on different virtual machines. To install StatsHouse on the real servers, use the same 
machine for the ClickHouse database and the aggregator.

Read more about the StatsHouse [components](conceptual%20overview/components.md).

### ClickHouse database

Read more about the StatsHouse [database](conceptual%20overview/components.md#database) component in the conceptual 
overview.

Check the [system requirements for the ClickHouse machines](#system-requirements).
We recommend using fast SSDs for storing per-second data, so that StatsHouse is able to provide you with the live mode.
See more about the ClickHouse [configuration (storage policies)](#cluster-configuration-storage-policies).

#### Cluster scheme

The ClickHouse cluster must have three replicas per shard. You can have any number of shards (one or more).

Find the [scheme](https://github.com/VKCOM/statshouse/blob/master/build/clickhouse-cluster.sql)
to create the necessary ClickHouse tables.

#### Cluster configuration (storage policies)

Find the example of the ClickHouse cluster configuration:

```
<storage_configuration>
<disks>
  <clickhouse_fast>
    <path>/var/lib/clickhouse-fast/</path>
  </clickhouse_fast>
</disks>
<policies>
  <ssd_then_hdd>
    <volumes>
      <ssd>
        <disk>clickhouse_fast</disk>
      </ssd>
      <hdd>
        <disk>default</disk>
      </hdd>
    </volumes>
  </ssd_then_hdd>
</policies>
</storage_configuration>
```

This policy means that ClickHouse should initially insert data into the fast SSDs, and then move it to the slower 
HDDs.

This configuration is applied to a created table: 
see the above-mentioned [scheme](https://github.com/VKCOM/statshouse/blob/master/build/clickhouse-cluster.sql).

### Metadata service

As soon as you have created the ClickHouse tables, proceed to metadata service installation.

Read more about the StatsHouse [metadata](conceptual%20overview/components.md#metadata) component in the conceptual 
overview.

#### How to install

You may install the metadata service on any machine. You have one metadata service instance.

To handle the possible failure of the metadata service, back up it manually. Now, one can copy the 
database binlog once a day using the `cron` job to restart the service if necessary.

:::info
We are now developing our own consensus mechanism to make the metadata service distributed.
:::

The metadata service needs the following parameters:
* `--db-path` that is the place to store data;
* `--binlog-prefix` that is the place to store the binlog.

Find the example of the metadata service installation script:

```shell
statshouse-metadata --db-path=/var/lib/statshouse/metadata/db --binlog-prefix=/var/lib/statshouse/metadata/binlog/bl
```

The aggregator, the API/UI component, and the agents need to know the metadata address. The agents get this 
information from the aggregators.

The metadata service has its agent too.

#### In case of metadata service failure

If the metadata service is unreachable, StatsHouse cannot create new metrics and tags, and use tag information (see 
more about the [mapping mechanism](conceptual%20overview/components.md#metadata)).

Each StatsHouse component has its metadata copy and continues working even in case of metadata failure. 
But if the component fails too, it will not be able to restore.

:::important
In case of aggregator failure, restore the metadata service with the same IP address as soon as possible.
:::

### Aggregators

Read more about the StatsHouse [aggregator](conceptual%20overview/components.md#aggregator) component in the 
conceptual overview.

#### How to install

The aggregator needs to know where to find the cluster, so please specify the following parameters:
* `--cluster` that is the cluster name,
* `--kh` that is the database address (the database may contain many clusters),
* `--agg-addr` that is the port to listen to,
* `--aes-pwd-file` that is the directory with the encryption key (the obligatory start parameter: `/etc/engine/pass` 
  by default), 
* `--cache-dir` that is the directory to store data in case the database does not insert data,
* `-u`, `-g` that are the group and the user (the obligatory start parameters).

On each ClickHouse replica, install the StatsHouse aggregator. Find the example of the aggregator installation script:
```
statshouse aggregator --cluster=test_shard_localhost --agg-addr=':13336' --aes-pwd-file=/etc/engine/pass \
--kh=XXX.X.X.X:XXXX --cache-dir=/var/lib/statshouse/cache/aggregator -u=root -g=root
```

As the aggregators get the ClickHouse cluster address, they get info about the shard and the replica they are 
installed on. 

The agents need all the aggregators' addresses. Each agent scans these addresses successively and tries to get the 
necessary configuration from the first available one.
The agent sends the data to the aggregators in a pseudorandom order (read more about [distributing data 
across the shards and replicas](conceptual%20overview/components.md#database)).

The aggregator starts with the `--aes-pwd-file` parameter that is the directory containing a key to decrypt the
incoming traffic. Read more about the `--aes-pwd-file` parameter and the encryption keys in the
[security](#how-to-ensure-security) section.

The size of the `--cache-dir` directory should be enough to store data resulting from **six hours** of the aggregator
working.

### Agents

:::important
Make sure an agent is installed on each machine that sends metrics to StatsHouse:
* the aggregators' machines,
* the metadata service machine,
* the API/UI machine,
* the ClickHouse machines.
:::

The agent opens the local port (13337, by default) and gets data from the application. Then it sends the data to the 
aggregators.

Read more about the StatsHouse [agent](conceptual%20overview/components.md#agent) component in the conceptual overview.

#### How to install

The agents are available as the RPM or DEB packages, for example:

`statshouse-2024.05.1-1.almalinux9.x86_64.rpm` or `statshouse_2024.05.1-focal_amd64.deb`.

The agent needs the following parameters:
* `--agg-addr` that is the aggregators' (or the proxies') addresses,
* `--aes-pwd-file` that is the directory with the encryption key (encrypting the data the agent sends to the aggregator 
  or the ingress proxy),
* `--cache-dir` that is the directory to store data in case the aggregator is unavailable,
* `--env-file-path` (optional) that is the file to configure tags for the host (hardware) metrics.

Find the example of the agent installation script:
```
statshouse agent --agg-addr=XX.XXX.XXX.XXX:XXXX,YY.YYY.YYY.YYY:YYYY,ZZ.ZZZ.ZZZ.ZZZ:ZZZZ \ 
--aes-pwd-file=/etc/engine/pass --cache-dir=/var/lib/statshouse/
```

The size of the `--cache-dir` directory should be enough to store data resulting from **six hours** of the agent working.
If the aggregators are unavailable for more than six hours, the older data is deleted from the disk.

Read more about the `--aes-pwd-file` parameter and the encryption keys in the [security](#how-to-ensure-security) section.

Read more about [using tags for the host (hardware) metrics](#how-to-use-tags-for-the-hardware-host-metrics) 
and the `--env-file-path` parameter.

### API/UI

The [API component](conceptual%20overview/components.md#application-programming-interface-api) has the StatsHouse 
user interface as its part. As soon as you start the API/UI component, you can view 
the [service metrics](guides/view-graph.md#service-metrics) that help to monitor StatsHouse.

#### How to install

You can install the API component on any machine.

The API component needs the following parameters:
* `--clickhouse-v2-addrs` that is the ClickHouse cluster address,
* `--listen-addr` that is the port to listen to,
* `--disk-cache` that is the place to store the cached 
[global `string↔int32` map](conceptual%20overview/components.md#the-budget-for-creating-tag-values),
* `--static-dir` that is the place where the UI static files live.

Find the example of the API/UI installation script:

```
statshouse-api --clickhouse-v2-addrs=XXX.X.X.X:XXXX \
--listen-addr=:YYYYY --disk-cache=/var/lib/statshouse/cache/api/mapping_cache.sqlite3 \
--static-dir=/usr/lib/statshouse-api/statshouse-ui/
```

#### Authentication

You may use an authentication mechanism you need, for example, an _nginx_ server using JSON Web Tokens (JWT).

To use the API with no authentication, enable the `--insecure-mode` option.

### Ingress proxy

Find information about the StatsHouse [ingress proxy](conceptual%20overview/components.md#ingress-proxy) component in 
the conceptual overview. Read more about [ensuring security](#how-to-ensure-security) 
with the ingress proxies and the cryptokeys.

## How to ensure security

StatsHouse encrypts data between the agent and the aggregator even if they are located in the same
data center. The AES encryption is used by default.

<img src={Aes} width="700"/>

If your cluster receives data from the agents that live outside the protected perimeter (i.e., outside the data center),
use the ingress proxies. The ingress proxy has a separate set of encryption keys for the external connections.
Read more about the StatsHouse [ingress proxy](conceptual%20overview/components.md#ingress-proxy) component 
in the conceptual overview.

### Without the ingress proxy

Check the flow for encrypting data between the agent and the aggregator without the ingress proxy.

<img src={NoProxyEncryption} width="300"/>

The agents send encrypted data to aggregators.
They use the key from the `--aes-pwd-file` directory.

The aggregators decrypt the incoming data.
They use the same key from the `--aes-pwd-file` directory.

### With the ingress proxy

Check the flow for encrypting data between the agent and the aggregator with the ingress proxy in the middle.

<img src={WithProxyEncryption} width="600"/>

The agents send encrypted data to the ingress proxies.
Each agent gets one of the keys from the ingress proxy's `-ingress-pwd-dir` directory as the `-aes-pwd-file` parameter.

The ingress proxies decrypt the incoming data using the keys from the `--ingress-pwd-dir` directory.

The ingress proxies send encrypted data to the aggregators.
They use the key from the `--aes-pwd-file` directory to encrypt the outgoing traffic.

The aggregator starts with the parameter `--aes-pwd-file` to decrypt the incoming traffic.

## How to configure

You can customize StatsHouse to fit the needs of your organization:
* manage budgets for multiple groups of users;
* manage tags for the hardware metrics.

### Budgets for namespaces, groups, and metrics

StatsHouse provides multiple groups of users with fair resource sharing. 
If you need to manage the resources manually, you can change the _weight_ for namespaces, groups, or individual metrics.

#### Namespaces

A namespace is the named metric container.
Namespaces help to
* [manage budgets](#manage-budgets) for StatsHouse tenants,
* [manage access](#manage-access) to metric data.

The metric's name defines if a metric relates to a namespace or not.
To assign a namespace, use a colon in a metric name:

<img src={Namespace} width="400"/>

The namespace does not appear by itself. The StatsHouse administrators create namespaces explicitly.
Use the full metric name, i.e. `foo:bar`, while sending data to StatsHouse.

The metric belongs to a default namespace
* if you do not specify the namespace in the metric name,
* if a metric name contains the nonexistent namespace.

Find the examples below:

| If                                                               | Then                                                                                                      |
|------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| The administrators have created the `foo` namespace for you.     | The `foo:bar` metric belongs to the `foo` namespace.<br/>The `abc` metric belongs to a default namespace. |
| There is **no** `buz` namespace in your StatsHouse installation. | The `buz:bar` metric belongs to a default namespace.                                                      |

##### Manage budgets

Namespaces allow you to budget the cluster resources. You can allocate the disk space for your namespace so that you 
can send at least N bytes per second and nobody interferes with your data. This kind of budgeting may be crucial when 
you need minimum or zero sampling.

As an administrator, create a namespace using the admin panel in the left StatsHouse menu:

<img src={NamespaceAdd} width="800"/>

By default, the resources are shared fairly between the namespaces. To allocate more or less resources to a namespace, 
configure the _weight_ parameter.

**What is _weight_?**

Weight is the percentage of resources allocated for a tenant (which can be a namespace, a group, or a metric).
Changing weight works similarly for namespaces, groups, and individual metrics. See the example of changing weight 
for the group below:

<img src={Weight} width="800"/>

:::important
While increasing the weight (and the budget) for a tenant, you automatically reduce the budget for the other ones. 
This may lead to higher [sampling](conceptual%20overview/concepts.md#sampling) rates for them. See the picture above.
:::

##### Manage access

StatsHouse allows you to integrate an access management system so that you can grant access to a particular namespace.
Usually, each business unit has its namespace.

#### Groups

A group is a prefix for the metric name (the first part of it):

<img src={GroupName} width="400"/>

You can create groups that belong or do not belong to a namespace. Specify the group name (as well as the namespace) 
when naming your metric or sending the metric data.

Groups allow you to manage budgets within the namespace. By default, the resources are shared fairly 
between the groups. To allocate more or less resources to a group, configure the _weight_ parameter.

<img src={GroupAdd} width="900"/>

#### Editing weight for a metric

To allocate more or less resources to individual metrics, change the weight for them.

Go to the **Edit** section on a metric page:

<img src={EditWeight1} width="700"/>

Then scroll down to change the weight for the metric:

<img src={EditWeight2} width="700"/>

This option is for administrators only.

### Hardware (host) metrics




Как пользоваться хостовыми метриками? Надо ли специально настраивать что-то?
Описание хостовых метрик и все вопросы по ним (из чатов)
Hardware metrics: описать, что собирается

#### How to use tags for the hardware (host) metrics

Как заполнить теги для хостовых метрик?
хост-метрики — не встроенные, а "изкоробочные" обычные скорее

"встроенные" — это именно про работоспособность самого статсхауса в первую очередь

/etc/statshouse_env.yml - тут ищем файл по умолчанию
А что по умолчанию пишется в теги хостовых метрик? Если не менять файл `--env-file-path`

И можно указать его параметром командной строки --env-file-path my_env.yml

* `--env-file-path` (optional) that is the file to configure tags for the host (hardware) metrics.

Надо ли менять что-то в файле `config.go` для агента
env тег для хостовых метрик - какие значения возможны, как заполнять?

В StatsHouse есть набор "предопределенных" названий стандартных тэгов.
StatsHouse  agent на старте читает (+вотчит) YAML файл /etc/statshouse_env.yml 
(другой путь можно задать аргументом env-file-path) формата:
hostname: myhost
env: production
group: dzen
dc: data_center_name
region: moscow
Там можно указать значения которые будут переданы тегами (env, dc, group и region) в хардварные метрики, 
что позволит получать их значения с необходимой группировкой
Стандартизация
Соответственно, чтобы разные команды не писали одни и те же по смыслу значения в разном формате 
(prod, Production, production etc.), предлагается определить формат заполнения данных тэгов командами



## How to monitor

To monitor StatsHouse, use the predefined service [metrics](#service-metrics). Additionally, find [logs](#logs).

### Service metrics

The `__heartbeat_version` metric shows the number of running components. Group the data by the "component" tag:

<img src={HeartbeatVersion} width="700"/>

To monitor the aggregator's metrics, use the `__agg` prefix:

<img src={AggMetrics} width="700"/>

To monitor the process of inserting data into the ClickHouse database, use the `__agg_insert` prefix:

<img src={AggInsertMetrics} width="150"/>

### Logs

By default, StatsHouse exports logs to `/dev/stdout`.
We recommend wrapping the process of running the agent into the `Systemd Unit` and managing logs via its `journal`
service.

## How to manage users

Instructions on how to create, modify, and delete user accounts, as well as manage user permissions and access levels.

Как управлять бюджетом: сначала на уровне неймспейса, потом на уровне групп, потом на уровне отдельных метрик.

Авторизация?

Метаинформация?


## How to maintain and upgrade

Information on regular maintenance tasks, such as backups and updates, as well as instructions for upgrading to new versions of the system.
Как безопасно обновлять систему?

У нас появились внутренние репозитории с которых можно забирать пакеты. Теперь новые версии стоит брать оттуда или ваших зеркал на эти репозитории
https://artifactory-external.vkpartner.ru/ui/repos/tree/General/debian-statshouse
https://artifactory-external.vkpartner.ru/ui/repos/tree/General/rpm-statshouse

❗Убедительная просьба настроить себе регулярный процесс обновления агентов. Мы планируем в ближайший месяц запретить старым агентам подключаться и отправлять данные.
В будущем при выпуске новых версий у вас так же будет месяц на то чтобы на них переключиться.

Посмотреть, какая версия стоит на агенте
__heartbeat_version
Там SHA в качестве commit_hash и время сборки в commit_timestamp



## Troubleshooting

Guidance on identifying and resolving common issues or errors that administrators may encounter while using the system.

## Integrations

* Какие инструменты нам понадобятся дополнительно? Алертинг? Авторизация? Метаинформация?

## Migrating from the other monitoring system

* How to migrate from Grafana

* How to migrate from Prometheus
  Подскажите, пожалуйста, statshouse поддерживает pull режим?
  Можно ли забирать метрики с экспортеров прометея?

Пока нет UI для скрейпа

## Support and resources

Contact information for technical support, links to online resources, and references to additional documentation or training materials.

* как задать вопрос
* как завести баг
* как завести фича-реквест


| Metric name                  | Description                                                                                                                 |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| host_cpu_usage               | The number of seconds the CPU has spent performing different kinds of work                                                  |
| host_softirq                 | Total number of software interrupts in the system                                                                           |
| host_irq                     | Total number of interrupts in the system                                                                                    |
| host_context_switch          | Total number of context switch in the system                                                                                |
| host_mem_usage               | Amount of free and used memory in the system                                                                                |
| host_mem_writeback           | Writeback/Dirty memory                                                                                                      |
| host_block_io_time           | The amount of time to transfer data to and from disk. Count - number of operations, Value - wait time for handle operations |
| host_block_io_size           | The amount of data transferred to and from disk. Count - number of operations, Value - size                                 |
| host_disk_usage              | Disk space utilization                                                                                                      |
| host_inode_usage             |                                                                                                                             |
| host_system_uptime           | The amount of time the system has been running                                                                              |
| host_system_process_created  | Number of processes and threads created                                                                                     |
| host_system_process_status   | Number of processes currently blocked, waiting IO or running on CPUs                                                        |
| host_system_psi_cpu          | PSI of CPU", // todo fix                                                                                                    |
| host_system_psi_mem          | PSI of memory                                                                                                               |
| host_system_psi_io           | PSI of IO                                                                                                                   |
| host_net_packet              | Number of transferred packets grouped by protocol                                                                           |
| host_net_error               | Number of network errors                                                                                                    |
| host_net_bandwidth" // total | Total bandwidth of all physical network interfaces. Count - number of packets, Value - number of bytes                      |
| host_net_dev_bandwidth       | Total bandwidth of all physical network interfaces. Count - number of packets, Value - number of bytes                      |
| host_net_dev_error           | Count of receive/transmit errors                                                                                            |
| host_net_dev_drop            | Count of packets dropped while receiving/transmitting                                                                       |
| host_socket_memory           | The amount of memory used by TCP sockets in all states                                                                      |
| host_tcp_socket_status       | The number of TCP socket grouped by state                                                                                   |
| host_tcp_socket_memory       | The amount of memory used by sockets                                                                                        |
| host_socket_used             | The number of socket in inuse state grouped by protocol                                                                     |
| host_page_fault              | The number of page fault                                                                                                    |
| host_paged_memory            | The amount of memory paged from/to disk                                                                                     |
| host_oom_kill                | The number of OOM                                                                                                           |
| host_numa_events             | NUMA events                                                                                                                 |
| host_dmesg_events            | dmesg events                                                                                                                |
| host_oom_kill_detailed       |                                                                                                                             |
