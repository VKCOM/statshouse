# StatsHouse protocol and data format

While there are multiple ways to get metrics into StatsHouse —
UDP, TCP, unix sockets, HTTP pull mode — the main way is to send
UDP packets with TL, msgpack or protobuf payloads. TL is probably
the fastest format to encode/decode, so we consider it our native
serialization format. Except for speed, there is no difference
between different formats, they all encode exactly same data.

Default StatsHouse UDP port is `localhost:13337`.

MessagePack has the following structure (which mirrors both TL and protobuf):

```yaml
{
  metrics: [
    {
      ts:   1670673392,     # uint32, UNIX timestamp in seconds (optional)
      name: "foobar",       # string([a-zA-Z][a-zA-Z0-9_]*), metric name
      tags: {
        "env":              # string([a-zA-Z][a-zA-Z0-9_]*), tag name
          "production"      # string(printable UTF-8),       tag value
      },
      counter: 100500.1,    # float64,        number of observed events
      value:   [0.7],       # array(float64), observed values array
      unique:  [591068825], # array(int64),   observed IDs array
    }
  ]
}
```

- [TL schema](../internal/data_model/public.tl)
- [protobuf schema](../internal/receiver/statshouse.proto)
