# Сборка sqlite клиента для работы с базой

Клиент собирается из ветки, которая реализует механику двух wal

```bash
    git clone https://github.com/sqlite/sqlite.git && cd sqlite && git checkout wal2
    ./configure && make
    ./sqlite3 --version
```
