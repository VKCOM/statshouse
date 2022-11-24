# StatsHouse

StatsHouse is a highly-available, scalable, multi-tenant monitoring system.

## Why would I use StatsHouse?

StatsHouse was built from the ground-up with 2 main goals in mind:
interactive usage at massive scale (low latency, high resolution data
and low response time) and built-in protection from overload (multi-tenant
resource budgeting and automatic sampling in case of overuse).

StatsHouse is a mature project. StatsHouse is being used in production
as a main monitoring system of [vk.com](https://vk.com). As of November 2022,
main StatsHouse cluster is receiving 350 million metrics per second
from 15000 servers and stores 4 years of data.

## UI screenshots

![Home page](./docs/media/home.webp "Home page")

![Dashboard](./docs/media/dash.webp "Dashboard")

## Features

- High availability and fault tolerance
- Massive scalability
- Multi-tenant resource budgeting with automatic sampling
- Low latency, high resolution data
- Interactive built-in UI
- Long-term storage with automatic downsampling
- Compatibility with Grafana and Prometheus/PromQL (alpha)

## Documentation

- [Quick start guide](./docs/quickstart.md)
- [Internals](./docs/internals.ru.md) (in Russian)

## Clients

- [Go](https://github.com/VKCOM/statshouse-go)
- [PHP](https://github.com/VKCOM/statshouse-php)
- [C++](https://github.com/VKCOM/statshouse-cpp)
- [nginx](https://github.com/VKCOM/nginx-statshouse-module)

## License

StatsHouse is licensed under the [Mozilla Public License Version 2.0](./LICENSE). 
