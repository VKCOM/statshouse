---
sidebar_position: 6
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
import DashAddVariables from '../img/dash-add-variables.png'
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

In the _Setting_ section, you set up the key dashboard characteristics, such as general info or variables.

#### Dashboard info

Go to the _Setting_ section to set up a name and description for your dashboard:

<img src={DashName} width="800"/>

To finish creating the dashboard, press the _Create_ button. The button becomes active only when you add
a dashboard name.

#### Variables

Variables help you to get use of tags for several metrics simultaneously.
A variable allows you to bind the tags of two different metrics even if the sets of values for the tags do not 
intersect.

Imagine you want to view two metrics on a dashboard. Each metric has the `environment` tag. What if you would like to 
bind these tags together to filter data for both metrics simultaneously?

The first metric's `environment` tag has two values (`production`, `dev`):

<img src={DashVar1} width="900"/>

The second metric's `environment` tag has three values (`production`, `staging`, `testing`):

<img src={DashVar2} width="900"/>

In the _Setting_ section, _Add variable_ manually (remember to _Save_ changes):

<img src={DashAddVariables} width="800"/>

Bind the `environment` tags from two metrics together (remember to _Apply_ changes):

<img src={DashVar3} width="900"/>

In the _Layout_ section, see the option to filter data with the `environment` tag for both metrics simultaneously—with 
the full list of tag values
(`production`, `staging`, `testing`, `dev`):

<img src={DashVar4} width="900"/>

If two metrics have tags with the same names (such as the `environment` tag in our example), 
use the _Autofilter_ option to generate variables for them automatically.

### Layout

In the _Layout_ section, filter data for all the displayed metrics using created variables:

<img src={DashGroupByTags} width="900"/>

Group metrics by a subcategory, change the group order, and customize the layout:

<img src={DashLayout} width="900"/>

## View a dashboard

Use the same [viewing options](view-graph.md) as for the graphs. Reset to a saved state, if necessary:

<img src={DashView} width="800"/>

## Remove a dashboard

Get rid of an unused dashboard:

<img src={DashRemove} width="800"/>

:::note
The removed dashboard disappears from the dashboard list but still exists, so you cannot use the same dashboard name
later.
:::




Customize the layout



удаление через apply
автолейаут
фиксированная ширина — чтобы один под другим

отображается в урл - даже если не сохранен
можно переслать ссылку даже на несохраненный даш

Можно задать порядок групп

добавлять в начале и в конце пустую группу и перемещать

свернутые группы
вк ид


Можно 
скрытая группа не обновляется
на нем выкатываем полезную ифн
нагрузка на открытие и закрытие будет меньше

группировать по тематике
скрыть отсортировать, добавить комментарии

какие-то графики крупнее, какие-то мельче

плюсики добавляют перед
можно перетянуть чтобы создать группу

перемещать графики можно, когда включено редактирование


переменные

пром КЛ

создать переменную, тогда выбирать вводом вручную

когда галочка - галочкой добавляю фильтр
если просто наберу и кликну по названию - сбросятся другие

не даш, а отдельный график, можно через ПромКЛ

как добавить построение списка - с графиков с обычными тегами

для ПромКЛ можно более сложные вещи

дашборд на котором только промкл графики (но нет списка, из которого можно накликать параметры)

для флажков - тоже можно
убрать лишний график в скрытую группу
чтобы не отображать

переменные - как использовать с пром кл
у переменной три параметра - сортировка, группировка и отрицание

переменная может существовать в воздухе, не привязанная к графику

переключать значение - не включет

тег свзяываем с переменной
изменяя переменную, мы меняем фильтры связанных тегов

переменные в графане

синхронизация тегов - было недостаточно функциональности

чтобы урл не захламлялся - попадало все полотно изменений графика

переменная - в урл записать только
привязать к промКЛ запросу


связать два непересекающихся тега
например, продакшен или прод - значения

в разных метриках один и тот же тег имеет разные варианты значений

например, продакшен будет на 98 процентах графиков
построение общего списка

хост цпу

рандом на приращение

fn counter
генерируется на сервере

