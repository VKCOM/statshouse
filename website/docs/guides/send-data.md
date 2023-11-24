---
sidebar_position: 5
---

# Send data from your code


Be sure to create a metric manually in the UI prior to sending data.




Can I write data to someone's metric?

! Beware of spoiling someone's data

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
