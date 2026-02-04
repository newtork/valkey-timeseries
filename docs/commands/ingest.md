# TS.INGEST

Ingest time-series samples from a JSON payload into a series.

## Syntax

```
TS.INGEST key data 
  [RETENTION duration] 
  [DUPLICATE_POLICY policy] 
  [ON_DUPLICATE policy_ovr] 
  [ENCODING COMPRESSED|UNCOMPRESSED] 
  [CHUNK_SIZE chunkSize] 
  [METRIC metric | LABELS labelName labelValue ...] 
  [IGNORE ignoreMaxTimediff ignoreMaxValDiff] 
  [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
```

## Required arguments

**key**

> Key name for the time series.

**data**

> JSON payload containing sample data. Must be a single JSON object with `values` and `timestamps` arrays.

## Optional arguments

**RETENTION duration**

> Maximum retention period in milliseconds. Samples older than this are automatically deleted. `0` means infinite
> retention. Default: module configuration.

**DUPLICATE_POLICY policy**

> Policy for handling duplicate timestamps:
> - `BLOCK` - Ignore duplicate (default when no policy is set)
> - `FIRST` - Keep first occurrence
> - `LAST` - Keep last occurrence
> - `MIN` - Keep minimum value
> - `MAX` - Keep maximum value
> - `SUM` - Sum all values

**ON_DUPLICATE policy_ovr**

> Override the duplicate policy for this command invocation only. Does not modify the series' configured policy.

**ENCODING COMPRESSED|UNCOMPRESSED**

> Storage encoding:
> - `COMPRESSED` - Gorilla compression (default)
> - `UNCOMPRESSED` - Raw storage

**CHUNK_SIZE chunkSize**

> Maximum size in bytes for each chunk. Actual memory usage may exceed this slightly. Default: 4096.

**METRIC metric** | **LABELS labelName labelValue ...**

> Series metadata for filtering and queries:
> - `METRIC` - Labels from JSON payload (when payload includes `metric` object)
> - `LABELS` - Explicit label name-value pairs

**IGNORE ignoreMaxTimediff ignoreMaxValDiff**

> Filtering thresholds for incoming samples:
> - `ignoreMaxTimediff` - Maximum time difference (ms) from last sample
> - `ignoreMaxValDiff` - Maximum absolute value difference from last sample
>
> Samples exceeding either threshold are dropped.

**SIGNIFICANT_DIGITS significantDigits** | **DECIMAL_DIGITS decimalDigits**

> Value precision control (mutually exclusive):
> - `SIGNIFICANT_DIGITS` - Number of significant digits (0-18)
> - `DECIMAL_DIGITS` - Number of decimal places

## JSON payload format

The `data` argument expects a JSON object with the following structure:

```json
{
  "values": [
    1.0,
    2.0,
    3.0
  ],
  "timestamps": [
    1620000000000,
    1620000001000,
    1620000002000
  ]
}
```

**Required fields:**

- `values` - Array of numeric values (parsed as floats)
- `timestamps` - Array of timestamps in milliseconds (parsed as 64-bit integers)

**Constraints:**

- `values` and `timestamps` arrays must have equal length
- At least one sample must be present

## Return value

[Array reply](https://valkey.io/docs/reference/protocol-spec/#arrays) of two integers:

1. Number of successfully ingested samples
2. Total number of samples in the payload

## Behavior

- **Sorting:** Input samples are automatically sorted by timestamp before insertion
- **Retention filtering:** Samples older than the retention window are dropped before processing
- **Duplicate handling:** Controlled by `DUPLICATE_POLICY` (or `ON_DUPLICATE` override)
- **Chunk allocation:** New chunks are created automatically as needed
- **Series creation:** If the key doesn't exist, a new series is created with the provided options
- **Ingestion count:** Only successfully inserted samples are counted; dropped or blocked samples are excluded from the
  success count

## Examples

### Basic ingestion

Ingest two samples into a series:

```valkey-cli
TS.INGEST sensor:temp:room1 '{"values":[22.5,23.1],"timestamps":[1620000000000,1620000060000]}'
```

**Result:**

```
1) (integer) 2
2) (integer) 2
```

### Create series with custom options

```valkey-cli
TS.INGEST sensor:pressure:tank2 '{"values":[101.3,101.5],"timestamps":[1620000000000,1620000001000]}' RETENTION 86400000 CHUNK_SIZE 8192 DUPLICATE_POLICY LAST LABELS sensor_type pressure location tank2
```

### Override duplicate policy

```valkey-cli
TS.INGEST sensor:temp:room1 '{"values":[22.5,22.8],"timestamps":[1620000000000,1620000000000]}' ON_DUPLICATE MAX
```

**Result:**

```
1) (integer) 1
2) (integer) 2
```

*Note: Second sample at same timestamp replaces first due to `MAX` policy.*

### With value filtering

```valkey-cli
TS.INGEST sensor:temp:room1 '{"values":[22.5,22.6,25.0],"timestamps":[1620000000000,1620000001000,1620000002000]}' IGNORE 5000 2.0
```

*Samples exceeding 2.0 difference or 5000ms gap from previous sample are dropped.*

## Error conditions

- **WRONGTYPE:** Key exists but is not a time series
- **Wrong arity:** Incorrect number of arguments
- **TSDB: missing values:** JSON payload lacks `values` array
- **TSDB: missing timestamps:** JSON payload lacks `timestamps` array
- **TSDB: values and timestamps length mismatch:** Arrays have different lengths
- **TSDB: no timestamps or values:** Arrays are empty
- **missing key or metric_name:** `metric` provided without `key` or `metric_name`

## Notes

- if the key does not exist, it will be created with the provided options
- Input samples are sorted by timestamp before insertion
- Samples are **not** guaranteed to be inserted in the same order as the input when timestamps differ across chunk
  boundaries
- Retention filtering occurs **before** chunk grouping and insertion
- The ingested count may be less than the payload count if samples are dropped due to retention, duplicates, or filters
- When series doesn't exist and no options are provided, module-level defaults apply
- For bulk ingestion from external sources (e.g., Prometheus, VictoriaMetrics), this command provides an efficient
  single-call interface

## Complexity

O(N*log(M)) where N is the number of samples and M is the number of existing chunks. Parallel processing is used for
chunk insertion when multiple chunks are affected.

## See also

[`TS.ADD`](ts.add.md) | [`TS.MADD`](ts.madd.md) | [`TS.RANGE`](ts.range.md) | [`TS.CREATE`](ts.create.md)