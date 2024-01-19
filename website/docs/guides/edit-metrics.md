---
sidebar_position: 5
---

import EditStart from '../img/edit-start.png'
import EditSave from '../img/edit-save.png'
import EditView from '../img/edit-view.png'
import Description from '../img/description.png'
import AggregationEdit from '../img/aggregation-edit.png'
import EnablePercentiles from '../img/enable-percentiles.png'
import ResolutionEdit from '../img/resolution-edit.png'
import ResolutionView from '../img/resolution-view.png'
import Petabytes from '../img/petabytes.png'
import TagsN from '../img/tags-n.png'
import TagsDash from '../img/tag-dash.png'
import TagDesc from '../img/tag-desc.png'
import Raw from '../img/raw.png'
import RawInt from '../img/raw-int.png'
import StringTag from '../img/string-tag.png'
import Disable from '../img/disable.png'

# Edit a metric

If you have started sending metric data, you have already set up the essential metric characteristics.
With the editing options, try more settings or change the look of your metric in the UI:

<img src={EditStart} width="700"/>

As soon as you have set up the necessary options, scroll down to the bottom of the page to save your settings:

<img src={EditSave} width="800"/>

Then scroll back to the top of the page to view your metric with the changes applied:

<img src={EditView} width="800"/>

## Description

Add a metric description on one or multiple lines to show in the UI. No formatting is supported.

<img src={Description} width="800"/>

You can [edit the graph name](view-graph.md#2--graph-name) to customize the look of the dashboard or the graph view 
without changing the name of the metric itself.

## Aggregation

Basically, a [metric type](send-data.md#how-to-choose-a-metric-type) affects the range of descriptive statistics 
that are meaningful for a metric. In the [_Descriptive statistics_](view-graph.md#3--descriptive-statistics) 
dropdown menu, you can see statistics, which may be not relevant for your metric type.
If you pick them, you will see 0 values for them on a graph. For example, you cannot view the cumulative graph 
for _unique_ metrics. 

To switch off showing irrelevant statistics in the UI, specify the type of your metric:

<img src={AggregationEdit} width="500"/>

The _Mixed_ type allows you to display all the statistics (even irrelevant ones) in the UI.
See more on [changing or combining metric types](send-data.md#metric-types-and-their-combinations).

### Percentiles

Percentiles are available for _value_ metrics only:

<img src={EnablePercentiles} width="500"/>

:::note
You start writing percentiles for a metric only upon switching the _Enable percentiles_ toggle on.
You will not see percentiles for the previously written data.
:::

Note that the amount of data increases for a metric with percentiles, so enabling them may lead to increased
[sampling](../conceptual-overview.md#sampling). If it is important for you to have the lower sampling factor, keep an
eye on your metric [cardinality](#cardinality) or choose custom [resolution](edit-metrics.md#resolution)
for writing metric data.

## Resolution

The minimum available resolution of data to show on a graph depends on the currently available aggregation:
* per-second aggregated data is stored for the first three days,
* per-minute aggregated data is stored for a month,
* per-hour aggregated data is available forever.

So, you can get per-second data for the last three days, per-minute data for the last month, and you can get 
per-hour data for any period you want.

If getting the highest available resolution is not crucial for you, but it is important for you 
to reduce [sampling](../conceptual-overview.md#sampling), reduce your metric resolution. 
For example, you may choose to send data once per 15 seconds instead of sending per-second data:

<img src={ResolutionEdit} width="400"/>

Some resolution levels are marked as "native": 1, 5, 15, 60 seconds. They correspond to a level of details 
for the UI, so we recommend using them to avoid jitter on a graph. See more about metric 
[resolution](../conceptual-overview.md#resolution).

You see custom resolution near the metric name in a graph view:

<img src={ResolutionView} width="600"/>

:::important
Setting up resolution affects processing data—not showing data in the UI.
For a metric with custom resolution, it is impossible to display data on a graph with 
the smaller [aggregation interval](view-graph.md#6--aggregation-interval): 
you cannot show per-second data if the resolution is set to 5 seconds.
:::

## Unit

Set up measurement units to show on a Y-axis in a graph view:

<img src={Petabytes} width="600"/>

## Tags

Customize your metric tags: their look in the UI, and their behavior while sending data. 

### Hide the unnecessary tags

Hide the unnecessary tags in the UI if you have less than 16 tags for your metric:

<img src={TagsN} width="250"/>

To hide only one tag in the middle, enter a hyphen (-) in the _Tag description_ field:

<img src={TagsDash} width="500"/>

### Describe tags

While sending data, you refer to a tag by its ID or a name. Add tag descriptions to 
show in the UI:

<img src={TagDesc} width="600"/>

### Set up _Raw tags_

If you need a tag with many different 32-bit integer values (such as `user_ID`), use the
_Raw_ tag values to avoid the [mapping flood](../conceptual-overview.md#mapping-and-budgets-for-creating-metrics).
Check [how many tag values](send-data.md#how-many-tag-values) you can create.

Tag values are usually `string` values. StatsHouse maps all of them to `int32` values for higher efficiency.
This huge `string`↔`int32` map is common for all metrics, and the budget for creating new mappings
is limited. Mapping flood appears when you exceed this budget.

If tag values in your metric are originally 32-bit integer values, you can prevent them from being mapped 
and mark them as _Raw_ ones. The _Raw_ tag values in the example below are stored as `1` and `2`.
To help yourself remember what they mean, choose a type to show in the UI and add descriptions:

<img src={Raw} width="800"/>

Value comments are stored as meta-information, so they do not expend the mapping budget.

### Set up _String tag_

To filter data with the [String tag](send-data.md#string-tag) on a graph, add a name or description to it:

<img src={StringTag} width="600"/>

## Disabling a metric

You cannot delete a metric, but you can disable it:

<img src={Disable} width="250"/>

Disabling a metric stops writing data for it and removes it from the metric list.

:::important
To enable a metric again, you need a direct link to it.
:::

## Admin settings

These settings are for administrators only:

* _Weight_
* _Mapping Flood Counter_
* _Presort key_
* _Presort key only_
* _Enable max host_
* _Enable min host_
* _Enable sum square_


