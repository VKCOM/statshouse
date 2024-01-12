---
sidebar_position: 5
---

# Conceptual overview

## Features

## Data model

## Aggregation


## Sampling

## Fair resource sharing

## Cardinality

## Mapping and budgets for creating metrics

## Agent


## Tags

## String tag

## Metric types: implementation


## Protocols

Without a client library, you can create a socket, prepare a JSON file, and send your formatted data.
This sounds simple, but only if you have not so much data.

StatsHouse uses [UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol).
If you send a datagram per event, and there are too many of them,
there is a risk of dropping datagrams, and no one will notice it.

If you do not use the client library, the non-aggregated data will reach StatsHouse
[agent](../conceptual-overview.md#agent), and the agent will aggregate them anyway.
