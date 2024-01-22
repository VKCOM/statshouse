---
sidebar_position: 6
---

import Dash from '../img/dash.png'
import DashOpen from '../img/dash-open.png'
import DashList from '../img/dash-list.png'
import DashEdit from '../img/dash-edit.png'
import DashView from '../img/dash-view.png'
import DashName from '../img/dash-name.png'
import DashGrouping from '../img/dash-grouping.png'
import DashGroupByTags from '../img/dash-group-by-tags.png'
import DashLayout from '../img/dash-layout.png'
import DashRemove from '../img/dash-remove.png'

# Create and view dashboards

A dashboard is a graphical report of data relevant to a particular system (a product or a service):

<img src={Dash} width="900"/>

Check the main StatsHouse menu to [refer to existing dashboards](#refer-to-existing-dashboards),
or [create](#create-a-dashboard) and [edit](#edit-a-dashboard) your ones.

## Refer to existing dashboards

Choose an existing dashboard from a list:

<img src={DashList} width="700"/>

## Create a dashboard

StatsHouse creates a dashboard for a currently viewed metric graph automatically.
To add more graphs on a dashboard, create a new [metric tab](view-graph.md#19--metric-tabs) 
and choose the other necessary metric:

<img src={DashOpen} width="400"/>

## Edit a dashboard

Edit a dashboard to customize its name, description, layout, and behavior—press the button with the "gear" icon:

<img src={DashEdit} width="800"/>

Upon editing, please remember to save your changes.

### Set a name and description

Go to _Setting_ to set up a name and description for your dashboard:

<img src={DashName} width="800"/>

### Filter with common tags

In the _Setting_ section, enable filtering with tags that are common for all the metrics on a dashboard.
To enable filtering with tags, _Add variable_ manually for each tag, or generate variables for all the common tags with 
the _Autofilter_ option:

<img src={DashGrouping} width="500"/>

### Customize the layout

In the _Layout_ section, use [filtering options](view-graph.md#7--tags) for the common tags (variables):

<img src={DashGroupByTags} width="300"/>

Group metrics by a subcategory, change the group order, and customize the layout:

<img src={DashLayout} width="900"/>

## View a dashboard

Use the same [viewing options](view-graph.md) as for the graphs. Reset to a saved state, if necessary:

<img src={DashView} width="800"/>

## Remove a dashboard

While [you cannot delete a metric](edit-metrics.md#disabling-a-metric), you can get rid of an unused dashboard:

<img src={DashRemove} width="800"/>
