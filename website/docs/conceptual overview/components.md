---
sidebar_position: 3
---

import Components from '../img/components.png'

# Components

## Agent

### Receiving data: implementation details

#### Receiving data via UDP

##### UDP socket buffer overflow

#### Receiving data via TL RPC

#### Receiving data via unix datagram socket or TCP

## Aggregator

### An agent-aggregator interaction

### Preventing double inserts and handling aggregator's shutdowns

## Database

### ClickHouse table structure

## Data access service (API)

## User interface

## Ingress proxy

### Cryptokeys

### A proxy behind the proxy

### Kubernetes-related questions

## Metadata

### Mapping budget

Tag values are usually `string` values and appear repeatedly. For higher efficiency, StatsHouse maps all of them to
`int32` values. This huge `string`↔`int32` map is common for all metrics. The elements of this map are never deleted.
To prevent the uncontrollable increase of the map, the budget for creating new mappings is limited to 300.
Upon exceeding the budget, new mappings can be added once per hour (this rule is customizable).

Mapping flood appears when you exceed this budget.

Then the budget is over and new mappings are not allowed, StatsHouse inserts a service `mapping flood` value to a
tag column not to lose the entire event.


### Mapping and budgets for creating metrics
