---
sidebar_position: 5
description: TEST
---

# Send data from your code

:::important
Before sending data to a metric, you have to create a metric manually via the StatsHouse UI.
:::

отправлять в одном из трех протоколов
https://github.com/VKCOM/statshouse/blob/master/docs/protocol.md


ОТправлять данные можно из кода
с помощью клиентской либы
баш-запросом



Мишин тестовый проект:
https://github.com/alpinskiy/statshouse-load-generator/blob/main/main.go

Питоновская либа:
https://github.com/VKCOM/statshouse-py/blob/master/src/statshouse/_statshouse.py

как пользоваться - импортируешь, вызываешь нужные методы

На плюсах все непонятно

как сделано на джаве - Женя





Can I write data to someone's metric?

! Beware of spoiling someone's data


You can start sending data as soon as you create a metric

## Use StatsHouse client libraries

sending data from your code: examples for different languages


## Use your own code instrumentation

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>

<TabItem value="py" label="Python">

```py
def hello_world():
  print("Hello, world!")
```

</TabItem>
<TabItem value="js" label="JavaScript">

```js
function helloWorld() {
  console.log('Hello, world!');
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
class HelloWorld {
  public static void main(String args[]) {
    System.out.println("Hello, World");
  }
}
```

</TabItem>
</Tabs>
