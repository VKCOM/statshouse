---
sidebar_position: 3
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Как отправлять данные

<!-- TOC -->
* [С использованием клиентских библиотек](#с-использованием-клиентских-библиотек)
  * ["Что делать, если нет библиотеки для нужного мне языка?"](#что-делать-если-нет-библиотеки-для-нужного-мне-языка)
  * ["Что делать, если нативная библиотека не предоставляет необходимую функциональность?"](#что-делать-если-нативная-библиотека-не-предоставляет-необходимую-функциональность)
* [Без использования клиентских библиотек](#без-использования-клиентских-библиотек)
<!-- TOC -->

:::important
Прежде чем отправлять данные, [создайте метрику](create-metric.md) вручную в интерфейсе StatsHouse.
:::

:::warning
Не отправляйте данные в чужие метрики — вы можете испортить чьи-то данные.
:::

## С использованием клиентских библиотек

Мы рекомендуем инструментировать код приложений с помощью клиентских библиотек StatsHouse — они 
помогают отправлять данные в правильном формате. Вот репозитории готовых библиотек:

- [Go](https://github.com/VKCOM/statshouse-go)
- [PHP](https://github.com/VKCOM/statshouse-php)
- [C++](https://github.com/VKCOM/statshouse-cpp)
- [Java](https://github.com/VKCOM/statshouse-java)
- [Python](https://github.com/VKCOM/statshouse-py)

Для использования StatsHouse с _nginx_ есть [отдельный модуль](https://github.com/VKCOM/nginx-statshouse-module).

Ниже показаны примеры кода.
Чтобы установить нужную библиотеку, следуйте рекомендациям из соответствующего файла README.

<Tabs>

<TabItem value="cpp" label="C++">

```cpp
#include "statshouse.hpp"
#include <cstdio>

using namespace statshouse;
    
Registry r{{
    logger: puts // debug output
}};

int main() {
    auto v = r.metric("my_value_metric")
        .tag("subsystem", "foo")
        .tag("protocol", "bar")
        .event_metric_ref();

    v.write_value(42.5);
    return 0;
}
```

</TabItem>

<TabItem value="py" label="Python">

```Python
import statshouse
    
statshouse.value("my_value_metric", {"subsystem": "foo", "protocol": "bar"}, 42.5)
```

</TabItem>

<TabItem value="go" label="Go">
```go
To be provided later
```
</TabItem>
<TabItem value="php" label="PHP">
```php
To be provided later
```
</TabItem>
<TabItem value="java" label="Java">
```java
To be provided later
```
</TabItem>

</Tabs>

#### "Что делать, если нет библиотеки для нужного мне языка?"

Лучше всего отправить нам [запрос](https://github.com/VKCOM/statshouse/issues) на GitHub.

Вы можете самостоятельно разработать библиотеку для нужного вам языка.
Однако в этом случае мы не сможем отвечать за её качество и обеспечить полноценную поддержку.

Если вы всё же решили это сделать, используйте одну из готовых библиотек StatsHouse
в качестве модели — изучите [модель данных StatsHouse](design-metric.md).

#### "Что делать, если нативная библиотека не предоставляет необходимую функциональность?"

Лучше всего отправить нам [запрос](https://github.com/VKCOM/statshouse/issues) на GitHub.

Вы также можете подготовить JSON-файл и отправить данные в StatsHouse, но
мы не рекомендуем так делать. Вы не сможете использовать агрегацию и другие встроенные функции 
StatsHouse.

## Без использования клиентских библиотек

Чтобы проверить, как работает отправка данных, можно использовать [Netcat](https://netcat.sourceforge.net):

```bash
echo '{"metrics":[{"name":"my_metric","tags":{},"counter":1000}]}' | nc -q 1 -u 127.0.0.1 13337
```

Пример такого использования приведён в [Кратком руководстве](../quick-start.md#отправьте-данные-в-свою-метрику).

:::important
Мы настоятельно рекомендуем использовать [клиентские библиотеки StatsHouse](#с-использованием-клиентских-библиотек).

Клиентские библиотеки [агрегируют](../overview/concepts.md#агрегация) данные перед отправкой.
Агрегация предотвращает потерю данных.
Можно не использовать библиотеку, а просто создать сокет, подготовить JSON-файл и отправить данные —
но только если у вас не очень много данных.

StatsHouse отправляет данные по [UDP](../overview/components.md#получение-данных-по-udp).
Если отправлять датаграмму на каждое событие, а событий слишком много, буфер UDP-сокета может переполниться,
и никто этого не заметит.

Если вы не используете клиентскую библиотеку, ваши неагрегированные данные будут поступать в
[агент](../overview/components.md#агент), и он всё равно будет их агрегировать.
:::



