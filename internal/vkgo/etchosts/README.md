## etchosts

Предоставляет возможность получения ip адреса по hostname-у.

```go
import (
    "os"
    
    "gitlab.mvk.com/go/vkgo/pkg/etchosts"
)

func main() {
    hostname, err := os.Hostname()
    if err != nil {
        panic(err)
    }
    println(etchosts.Resolve())
}
```

### Устройство

`etchosts` парсит файл `/etc/hosts` (или другой указанный через вызов функции `SetFile`), сохраняя в
свое внутреннее состояние отображение hostname-а в ip адрес.

Раз в 1 секунду запущенная внутри горутина обновляет внутреннее состояние если ранее считанный файл изменился.

Библиотека поддерживает расширение своего функционала посредством callback-ов: `OnParse`. В callback передается
новое состояние, map из hostname в ip адрес.