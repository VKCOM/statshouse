---
sidebar_position: 5
---

import Dash from '../img/dash.png'
import DashOpen from '../img/dash-open.png'
import DashList from '../img/dash-list.png'
import DashEdit from '../img/dash-edit.png'
import DashView from '../img/dash-view.png'
import DashName from '../img/dash-name.png'
import DashVar1 from '../img/dash-var-1.png'
import DashVar2 from '../img/dash-var-2.png'
import DashVar3 from '../img/dash-var-3.png'
import DashVar4 from '../img/dash-var-4.png'
import DashGroupByDefault from '../img/dash-group-by-default.png'
import DashAddVariables from '../img/dash-add-variables.png'
import DashLayout from '../img/dash-layout.png'
import DashRemove from '../img/dash-remove.png'
import PqlDash1 from '../img/pql-dash-1.png'
import PqlDash2 from '../img/pql-dash-2.png'
import PqlDash3 from '../img/pql-dash-3.png'
import PqlDash4 from '../img/pql-dash-4.png'

# Create and view dashboards

A dashboard is a graphical report of data relevant to a particular system (a product or a service):

<img src={Dash} width="900"/>

In this section, you will find information on how to:
<!-- TOC -->
* [Refer to existing dashboards](#refer-to-existing-dashboards)
* [Create a dashboard](#create-a-dashboard)
* [Edit a dashboard](#edit-a-dashboard)
  * [Settings](#settings)
    * [Dashboard info](#dashboard-info)
    * [Variables](#variables)
  * [Layout](#layout)
    * [Graph width](#graph-width)
    * [Graph groups](#graph-groups)
* [View a dashboard](#view-a-dashboard)
* [Remove a dashboard](#remove-a-dashboard)
* [Set up PromQL-based dashboards](#set-up-promql-based-dashboards)
<!-- TOC -->

## Refer to existing dashboards

Choose an existing dashboard from a list:

<img src={DashList} width="700"/>

## Create a dashboard

StatsHouse opens a dashboard for a currently viewed metric graph automatically.
To add more graphs on a dashboard, create a new [metric tab](view-graph.md#19--metric-tabs) 
and choose the other necessary metric:

<img src={DashOpen} width="400"/>

To finish creating the dashboard, [add a name and description](#dashboard-info) to it 
so that it appears in the dashboard list.

## Edit a dashboard

Edit a dashboard to customize its name, description, layout, 
and behavior—press the button with the "gear" icon:

<img src={DashEdit} width="800"/>

Upon editing, please remember to save your changes.

### Settings

In the _Setting_ section, set up the key dashboard characteristics, such as [general info](#dashboard-info) 
or [variables](#variables).

#### Dashboard info

Go to the _Setting_ section to set up a name and description for your dashboard:

<img src={DashName} width="800"/>

To finish creating the dashboard, press the _Create_ button. The button becomes active only when you add
a dashboard name.

:::note
Please note that you cannot reuse the name of the [removed dashboard](#remove-a-dashboard).
:::

#### Variables

With variables, you can apply the same tag filters to several metrics simultaneously.
A variable binds the tags of two metrics together even if the sets of values for the tags do not 
intersect. It is also useful if the same tag has different names such as `production` or `prod` in different metrics.

Imagine you want to view two metrics on a dashboard. Each metric has the `environment` tag. What if you would like to 
bind these tags together to filter data for both metrics simultaneously?

The first metric's `environment` tag has two values (`production`, `dev`):

<img src={DashVar1} width="900"/>

The second metric's `environment` tag has three values (`production`, `staging`, `testing`):

<img src={DashVar2} width="900"/>

In the _Setting_ section, _Add variable_ manually (remember to _Save_ changes):

<img src={DashAddVariables} width="800"/>

Bind the `environment` tags from two metrics to the `env` variable (remember to _Apply_ changes):

<img src={DashVar3} width="900"/>

In the _Layout_ section, see the option to filter data with the `environment` tag for both metrics simultaneously—with 
the full list of tag values
(`production`, `staging`, `testing`, `dev`):

<img src={DashVar4} width="900"/>

If two metrics have tags with the same names (such as the `environment` tag in our example), 
use the _Autofilter_ option to generate variables for them automatically.

### Layout

In the _Layout_ section, set up options to filter, sort, or group data by default for all the metrics on a dashboard:

<img src={DashGroupByDefault} width="900"/>

Customize the layout so that the dashboard could help you recognize the meaningful patterns in data:

<img src={DashLayout} width="900"/>

#### Graph width

_S_, _M_, _L_ options are fixed graph widths, which are _small_, _medium_, and _large_ ones.
_Autowidth_ and the number of graphs _per row_ are the ways to arrange graphs in a group horizontally.

Play with these options to arrange the compared graphs one above or near the other.

#### Graph groups

Group the graphs by subcategories, describe and sort the groups for your convenience:

* In the _Layout_ editing mode, drag and drop graphs to move them on the dashboard.
* Add a group before or after the current one. 
* Collapse the infrequently used groups to save resources: the collapsed graphs are not updated upon 
  refreshing or opening the page.

## View a dashboard

Use the same [viewing options](view-graph.md) as for the graphs. Reset to a saved state, if necessary:

<img src={DashView} width="800"/>

## Remove a dashboard

To get rid of an unused dashboard, proceed to [editing](#edit-a-dashboard) the dashboard. In the _Setting_ section, 
press the _Remove_ button:

<img src={DashRemove} width="800"/>

:::note
The removed dashboard disappears from the dashboard list but still exists, so you cannot use the same dashboard name
later.
:::

## Set up PromQL-based dashboards

To filter data with tags for a PromQL-based graph, [create variables](#variables).

Imagine you have a PromQL-based graph with tags:

<img src={PqlDash1} width="1000"/>

While creating the [variable](#variables) for the PromQL-based graph (`new_var` in our example), you will not see tag 
names:

<img src={PqlDash2} width="1000"/>

To enable filtering or grouping, get back to the graph view and use the PromQL editor. Bind the tag to the previously 
created variable in your PromQL query and press _Run_:

<img src={PqlDash3} width="1000"/>

In this example, the initial query was:

`topk(5,metric{@what="countsec"})`, 

and we bound the `0` tag (that means `environment`) with the `new_var` variable: 

`topk(5,metric{@what="countsec",0:$new_var})`.

Then the filtering controls appear in the _Layout_ section and in the _Dashboard_ view.
To filter data with the particular tag value, enter the value name manually:

<img src={PqlDash4} width="1000"/>

When adding more PromQL-based graphs to a dashboard, make sure you bind the necessary tags to the corresponding 
variables for each graph.
