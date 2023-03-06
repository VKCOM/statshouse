---types---

statshouse.metric#3325d884 fields_mask:#
  name:    string
  tags:    (dictionary string)
  counter: fields_mask.0?double
  ts:      fields_mask.4?#               // UNIX timestamp UTC
  t:       fields_mask.5?long            // deprecated, UNIX nanoseconds UTC
  value:   fields_mask.1?(vector double)
  unique:  fields_mask.2?(vector long)

  = statshouse.Metric;

---functions---

// for smooth JSON interoperability, first byte of tag must not be 0x5b or 0x7b ("[" or "{")
// for smooth MessagePack interoperability, first byte of tag must be less than 0x80
// for smooth ProtoBuf interoperability, first byte must be as large as possible.

@write statshouse.addMetricsBatch#56580239 fields_mask:# metrics:(vector statshouse.metric) = True;
