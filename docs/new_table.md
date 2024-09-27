# New schema

- number of tags increased 16 -> 48
- separate `r0..r3` `Int64` columns for raw tags
- `flags` bitfield to indicate in which tables we want to write
	- new + old table or just new table (for migration to a new pipeline)
	- prekey, basic or both tables
	- hour table with TTL or default hour table
		- to store short lived metrics that live only for 1mo (like pod telemetry)
- `raw_flags UInt64` column to indicate that tag is raw
	- idea here is to write all number tags as raw tags automatically when we can
- each tag followed by unmapped stag option, only one them should be set at a time
- add `pre_stag` to store unmapped prekeys
- `pre_tag` is now 64 bit to support `r0..r3` tags
- `index_type UInt8` to merge basic and `pre_tag` tables into a single one
	- 0 - basic index
	- 2 - `pre_tag` index
- `PARTITION BY` increased from 6 to 24 hours
- `PRIMARY KEY` will be shortened to `index_type, metric, prekey, pre_stag, time` and first 8 tags
- `max_host`, `min_host` will become `AggregateFunction(argMax, String, Float32)` because we want to store location, and can't always map host
- add `max_counter_host` we can do it later
- `max_host_legacy`, `min_host_legacy` in the input table to write aggregates into original table, they will be removed after trasition period
- in `*_host` columns string will be binary data, where first byte signifies what is written in it, at start we will use 5 bytes, first is always 0, 4 other are Int32 tag

## Write dataflow
```mermaid  
graph LR;
agg-->input_table
subgraph new_tables
input_table-->new_map_view_1s;
input_table-->new_map_view_1m;
input_table-->new_map_view_1h;
input_table-->new_map_view_1h_short;
new_map_view_1s-->new_table_1s;
new_map_view_1m-->new_table_1m;
new_map_view_1h-->new_table_1h;
new_map_view_1h_short-->new_table_1h_short;
end

subgraph old_tables
input_table-->map_view_1s;
input_table-->map_view_1m;
input_table-->map_view_1h;
input_table-->prekey_map_view_1s;
input_table-->prekey_map_view_1m;
input_table-->prekey_map_view_1h;
map_view_1s-->table_1s;
map_view_1m-->table_1m;
map_view_1h-->table_1h;
prekey_map_view_1s-->prekey_table_1s;
prekey_map_view_1m-->prekey_table_1m;
prekey_map_view_1h-->prekey_table_1h;
end
```

## Schemas
### input_table
1. `insert_flags UInt8`
2. `metric Int32`
3. `pre_tag Int64`
4. `pre_stag Int64`
5. `time DateTime`
6. `rtag0 Int64`
7. ...
8. `rtag3 Int64`
9. `tag0 Int64`
10. `stag0 String`
11. ...
12. `tag47 Int64`
13. `stag47 String`
14. `raw_flags UInt64`
15. `min_host AggregateFunction(argMax, String, Float32)`
16. `max_host AggregateFunction(argMax, String, Float32)`
17. `max_count_host AggregateFunction(argMax, String, Float32)`
18. `min_host_legacy AggregateFunction(argMin, Int32, Float32)`
19. `max_host_legacy AggregateFunction(argMax, Int32, Float32)`
20. ... (rest of digest)

`insert_flags` - bitfield with 2 defined flags and 6 reserved ones
1. write to main index
2. write to prekey index

We have separate columns `insert_flags` and `index_type` because `insert_flags` could be used not only to distinguinsh between pre_tag and main index
but also to route between different tables.

`raw_flags`  - bitfield to indicate which tags are written as a raw tags

### table_1x
1. `index_type UInt8`
2. `metric Int32`
3. `pre_tag Int64`
4. `pre_stag String`
5. `time DateTime`
6. `rtag0 Int64`
7. ...
8. `rtag3 Int64`
9. `tag0 Int64`
10. `stag0 String`
11. ...
12. `tag47 Int64`
13. `stag47 String`
14. `raw_flags UInt64`
15. `min_host AggregateFunction(argMax, String, Float32)`
16. `max_host AggregateFunction(argMax, String, Float32)`
17. `max_count_host AggregateFunction(argMax, String, Float32)`
18. ... (rest of digest)

index_type 
- 0 - main index (pre_tag is empty)
- 1 - pre_tag index

## Automatic raw tags
Problem: raw tags are confusing for users and has to be set up in advance. Changing tag  raw flag breaks all existing tags.

Idea is to detect that tag value in an allowed range: `[-2^31, 2^31-1]`  and automatically save it as a raw tag. To distinguish mapped tags from raw tags we add `raw_flags` field. 
All existing raw tags will be simply copied into new ones.

For people who actually know that we want to write `UInt64` values  we have separate `Int64` row tag fields.

# Old schema

## Write dataflow

```mermaid  
graph LR;
agg-->input_table
input_table-->map_view_1s;
input_table-->map_view_1m;
input_table-->map_view_1h;
input_table-->prekey_map_view_1s;
input_table-->prekey_map_view_1m;
input_table-->prekey_map_view_1h;
map_view_1s-->table_1s;
map_view_1m-->table_1m;
map_view_1h-->table_1h;
prekey_map_view_1s-->prekey_table_1s;
prekey_map_view_1m-->prekey_table_1m;
prekey_map_view_1h-->prekey_table_1h;
```
## Schemas

### input_table
1. `metric Int32`
2. `prekey Int32`
3. `prekey_set UInt8`
4. `time DateTime`
5. `key0 Int32`
6. ...
7. `key15 Int32`
8. `skey String`
9. Digest

`prekey` is a value of one of the tags or 0
`prekey_set` is the enum describing where to write
- 0 write only to basic table
- 1 write to both basic and prekey tables
- 2 write only to prekey table


### map_view_1x and table_1x
1. `metric Int32`
2. `time DateTime`
3. `key0 Int32`
4. ...
5. `key15 Int32`
6. `skey String`
7. Digest

### prekey_map_view_1x and prekey_table_1x
1. `metric Int32`
2. `prekey Int32`
3. `time DateTime`
4. `key0 Int32`
5. ...
6. `key15 Int32`
7. `skey String`
8. Digest
