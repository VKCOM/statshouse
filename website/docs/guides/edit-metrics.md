---
sidebar_position: 6
---

import EditStart from '../img/edit-start.png'
import EditSave from '../img/edit-save.png'
import EditView from '../img/edit-view.png'
import Description from '../img/description.png'
import AggregationEdit from '../img/aggregation-edit.png'
import EnablePercentiles from '../img/enable-percentiles.png'
import ResolutionEdit from '../img/resolution-edit.png'
import ResolutionView from '../img/resolution-view.png'
import Milliseconds from '../img/milliseconds.png'
import TagsN from '../img/tags-n.png'
import TagsDash from '../img/tag-dash.png'
import TagDesc from '../img/tag-desc.png'
import TagName from '../img/tag-name.png'
import RawValueComments from '../img/raw-value-comments.png'
import RawFormat from '../img/raw-format.png'
import StringTag from '../img/string-tag.png'
import Disable from '../img/disable.png'
import DraftTags from '../img/draft-tags.png'
import FairKey from '../img/fair-key.png'

# Edit a metric

Learn [how to edit a metric](#how-to-edit-a-metric) and what the editing options are:
<!-- TOC -->
* [Description](#description)
* [Aggregation](#aggregation)
  * [Percentiles](#percentiles)
* [Resolution](#resolution)
* [Unit](#unit)
* [Tags](#tags)
  * [Hide the unnecessary tags](#hide-the-unnecessary-tags)
  * [Describe tags](#describe-tags)
  * [Set up _Raw tags_](#set-up-raw-tags)
    * [Value comments](#value-comments)
    * [Specifying formats for raw tag values](#specifying-formats-for-raw-tag-values)
  * [Set up _String top tag_](#set-up-string-top-tag)
  * [Map the draft tag names to the tag IDs](#map-the-draft-tag-names-to-the-tag-ids)
* [Disabling a metric](#disabling-a-metric)
* [Admin settings](#admin-settings)
  * [Fair key tags](#fair-key-tags)
<!-- TOC -->

## How to edit a metric

If you have started sending metric data, you have already set up the essential metric characteristics.
With the editing options, try more settings or change the look of your metric in the UI:

<img src={EditStart} width="700"/>

As soon as you have set up the necessary options, scroll down to the bottom of the page to save your settings:

<img src={EditSave} width="800"/>

Then scroll back to the top of the page to view your metric with the changes applied:

<img src={EditView} width="800"/>

## Description

Add a metric description on one or multiple lines to show in the UI. Feel free to use the [standard Markdown formatting](https://commonmark.org/help/) for descriptions.

<img src={Description} width="800"/>

You can [edit the graph name or description](view-graph.md#2--graph-name-and-description) to customize the look of the 
dashboard or the graph view 
without changing the name of the metric itself. You [cannot rename a metric](create-metric.md#can-i-rename-a-metric).

## Aggregation

Basically, a [metric type](design-metric.md#metric-types) affects the range of descriptive statistics 
that are meaningful for a metric. In the [_Descriptive statistics_](view-graph.md#3--descriptive-statistics) 
dropdown menu, you can see statistics, which may be not relevant for your metric type.
If you pick them, you will see 0 values for them on a graph. For example, you cannot view the cumulative graph 
for _unique_ metrics. 

To switch off showing irrelevant statistics in the UI, specify the type of your metric:

<img src={AggregationEdit} width="500"/>

The _Mixed_ type allows you to display all the statistics (even irrelevant ones) in the UI.
See more on [changing or combining metric types](design-metric.md#combining-metric-types).

### Percentiles

Percentiles are available for _value_ metrics only:

<img src={EnablePercentiles} width="500"/>

:::note
You start writing percentiles for a metric only upon switching the _Enable percentiles_ toggle on.
You will not see percentiles for the previously written data.
:::

Note that the amount of data increases for a metric with percentiles, so enabling them may lead to increased
[sampling](../overview/concepts.md#sampling). If it is important for you to have the lower sampling factor, keep an
eye on your metric [cardinality](view-graph.md#cardinality) or choose custom [resolution](edit-metrics.md#resolution)
for writing metric data.

## Resolution

The highest available resolution of data to show on a graph depends on the currently available 
[aggregate](../overview/concepts.md#aggregation):
* per-second aggregated data is stored for the first two days,
* per-minute aggregated data is stored for a month,
* per-hour aggregated data is available forever.

So, you can get per-second data for the last two days, per-minute data for the last month, and you can get 
per-hour data for any period you want.

If getting the highest available resolution is not crucial for you, but it is important for you 
to reduce [sampling](../overview/concepts.md#sampling), reduce your metric resolution. 
For example, choose a custom resolution to make the [agent](../overview/components.md#agent) send data once 
per 15 seconds instead of sending per-second data:

<img src={ResolutionEdit} width="400"/>

You see custom resolution near the metric name in a graph view:

<img src={ResolutionView} width="600"/>

:::important
Setting up resolution affects processing data—not showing data in the UI.
For a metric with custom resolution, it is impossible to display data on a graph with 
the smaller [aggregation interval](view-graph.md#6--aggregation-interval): 
you cannot show per-second data if the resolution is set to 5 seconds.
:::

See more about metric [resolution](../overview/concepts.md#resolution).

## Unit

Set up measurement units for the _value_ metric data you send to StatsHouse. 
With this unit information, StatsHouse generates the Y-axis label for a graph.

For example, if you set up _milliseconds_ for your metric, you can see _seconds_, _minutes_, or even _days_ as the 
Y-axis label on the graph:

<img src={Milliseconds} width="400"/>

Please note that the _«byte (shown as bits)»_ option means that you send bytes to StatsHouse, but they are converted to 
bits to appear on the Y-axis in the UI. It may be useful for those who work with network metrics.

:::note
If you have a _counter_ metric or view the _count_ and _count/sec_ statistics, you should not set a particular 
unit. A counter means the "number of times," so choose the _no unit_ option.
:::

## Tags

Customize your metric tags: their look in the UI, and their behavior while sending data. 

### Hide the unnecessary tags

Hide the unnecessary tags in the UI if you have less than 16 tags for your metric:

<img src={TagsN} width="250"/>

To hide only one tag in the middle, enter a hyphen (-) in the _Tag description_ field:

<img src={TagsDash} width="500"/>

### Describe tags

While sending data, you refer to a tag by its ID or a name.
Before you start referring to a tag by the custom name in your sending requests, 
specify these names here, in the UI:

<img src={TagName} width="170"/>

Add tag descriptions to show in the UI:

<img src={TagDesc} width="600"/>

### Set up _Raw tags_

If you need a tag with many different 32-bit integer values (such as `user_ID`), use the
[_Raw_ tag values](design-metric.md#raw-tags) to avoid the 
[mapping flood](../overview/components.md#the-budget-for-creating-tag-values).

To help yourself remember what they mean, specify a 
[format](#specifying-formats-for-raw-tag-values) for your data to show in the UI 
and add [value comments](#value-comments).

#### Value comments

Add value comments to remember what your raw tag values mean:

<img src={RawValueComments} width="1000"/>

:::note
Value comments are stored as meta-information, so they do not expend the mapping budget.
:::

#### Specifying formats for raw tag values

When you send tag values as raw ones, you send 32-bit integer values. For example, you send integers that are 
timestamps. To make such raw tag values more readable in the UI, choose the format for them: 

<img src={RawFormat} width="1000"/>

:::note
Please note that you send only 32-bit integer values as raw tag values. If you send IP-addresses, they look like 
`1062731276` but not `63.87.254.12` as the latter would be a string. When you choose an _ip_ option as the format, 
StatsHouse displays your `1062731276` raw tag value as `63.87.254.12` in the UI.
:::

### Set up _String top tag_

To filter data with the [String top tag](design-metric.md#string-top-tag) on a graph, add a name or description to it:

<img src={StringTag} width="600"/>

### Map the draft tag names to the tag IDs

Draft tags appear when the data you send contains the tag names, but these names were not mapped to tag IDs, so they 
are "unknown" to StatsHouse.

The feature was implemented to support
[migrating from Prometheus](../admin/migrating.md#how-to-migrate-from-prometheus). There may be more than 16 tags in 
the scraped metrics, and StatsHouse cannot map them to the tag IDs randomly.

StatsHouse extracts the "unknown" tag names from the scraped metric data and shows them as the _draft tags_. Users 
can map these draft tag names with the tag IDs manually:

<img src={DraftTags} width="700"/>

:::tip
Draft tags also appear when you refer to the tags in your code by the custom names, but you have not previously 
specified these names in the UI. See more about [describing tags](#describe-tags). The draft tag feature may help 
you to configure metric tags faster.
:::

Learn more about [designing tags for your metrics](design-metric.md#tags).

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

### Fair key tags

Choose the tag to [enable the tag-level budgeting](../overview/concepts.md#tag-level-budgeting-fair-key-tags) for it:

<img src={FairKey} width="600"/>
