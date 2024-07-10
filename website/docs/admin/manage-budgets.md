---
sidebar_position: 7
title: Manage budgets
---

import Namespace from '../img/namespace.png'
import NamespaceAdd from '../img/namespace-add.png'
import GroupName from '../img/group-name.png'
import GroupAdd from '../img/group-add.png'
import EditWeight1 from '../img/edit-weight-1.png'
import EditWeight2 from '../img/edit-weight-2.png'
import Weight from '../img/weight.png'

# Manage budgets

You can manage budgets for multiple groups of users to fit the needs of your organization.

StatsHouse provides multiple groups of users (tenants) with fair resource sharing.
If you need to manage the resources manually, you can change the _weight_ for namespaces, groups, or 
individual metrics.

## Namespaces

A namespace is the named metric container.
Namespaces help to
* manage budgets for StatsHouse tenants by changing their [weight](#what-is-weight),
* [manage access](#manage-access-for-namespaces) to metric data.

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

Namespaces allow you to budget the cluster resources. You can allocate the disk space for your namespace so that you
can send at least N bytes per second and nobody interferes with your data. This kind of budgeting may be crucial when
you need minimum sampling.

As an administrator, create a namespace using the admin panel in the left StatsHouse menu:

<img src={NamespaceAdd} width="800"/>

By default, the resources are shared fairly between the namespaces. To allocate more or less resources to a namespace,
configure the _weight_ parameter.

### What is _weight_?

Weight is the percentage of resources allocated for a tenant (which can be a namespace, a group, or a metric).
Changing weight works similarly for namespaces, groups, and individual metrics. See the example of changing weight
for the group below:

<img src={Weight} width="800"/>

:::important
While increasing the weight (and the budget) for a tenant, you automatically reduce the budget for the other ones.
This may lead to higher [sampling](overview/concepts.md#sampling) rates for them. See the picture above.
:::

### Manage access for namespaces

StatsHouse allows you to integrate an access management system so that you can grant access to a particular namespace.
Usually, each business unit has its namespace.

## Groups

A group is a prefix for the metric name (the first part of it):

<img src={GroupName} width="400"/>

You can create groups that belong or do not belong to a namespace. Specify the group name (as well as the namespace)
when naming your metric or sending the metric data.

Groups allow you to manage budgets within the namespace. By default, the resources are shared fairly
between the groups. To allocate more or less resources to a group, configure the _weight_ parameter.

<img src={GroupAdd} width="900"/>

## Editing weight for a metric

To allocate more or less resources to individual metrics, change the weight for them.

Go to the **Edit** section on a metric page:

<img src={EditWeight1} width="700"/>

Then scroll down to change the weight for the metric:

<img src={EditWeight2} width="700"/>

This option is for administrators only.

