# Changelog

## [0.6.0] - 2026-02-21

### ⚠️ Breaking Changes

#### `condense_segments` — `segment_data` Now Returns a List of Structs by Default

When `condense_segments=True` is passed to any parsing function, the `segment_data` column
now contains a **`List<Struct>`** instead of a JSON string. This is a breaking change for any
code that was reading `segment_data` as a JSON string and parsing it manually.

**Affected functions:**

| Interface | Function |
|-----------|----------|
| Python    | `parse_ssim_to_dataframe()`, `parse_ssim_to_parquets()` |
| Rust      | `ssim_to_dataframe()`, `ssim_to_parquets()` |

**Not affected:** `ssim_to_csv()` / `parse_ssim_to_csv()` — see note below.

---

**Before (pre-0.6.0):** `segment_data` was a JSON string column, e.g.:
```
import polars as pl

df = rs.parse_ssim_to_dataframe("schedule.ssim", condense_segments=True)

# Segment Struct
schema = pl.List(
    pl.Struct(
        {
            "board_point_indicator": pl.Utf8,
            "off_point_indicator": pl.Utf8,
            "board_point": pl.Utf8,
            "off_point": pl.Utf8,
            "data_element_identifier": pl.Utf8,
            "data": pl.Utf8,
        }
    )
)

# Expand segments into a flat segments DataFrame
df_exploded = (
    df.with_columns(
        pl.col("segment_data").str.json_decode(schema).alias("segment_data")
    )
    .explode("segment_data")  # one row per segment
    .unnest("segment_data")  # convert struct to columns
)

```

**After (0.6.0+):** `segment_data` is a native Polars `List<Struct>` column. To work with it:
```python
import rustyssim as rs
import polars as pl

# Returns List<Struct> by default — no JSON parsing needed
df = rs.parse_ssim_to_dataframe("schedule.ssim", condense_segments=True)

# Expand segments into flat rows
df_exploded = (
    df.explode("segment_data")
      .unnest("segment_data")
)
```


**If you need JSON output** (e.g. for an ETL pipeline or downstream system that expects strings),
pass `serialize_segments=True` explicitly:
```python
# Python
df = rs.parse_ssim_to_dataframe("schedule.ssim", condense_segments=True, serialize_segments=True)
rs.parse_ssim_to_parquets("schedule.ssim", condense_segments=True, serialize_segments=True)
```
```rust
// Rust
ssim_to_dataframe("schedule.ssim",  condense_segments=Some(true), serialize_segments=Some(true))?;
ssim_to_parquets("schedule.ssim",  condense_segments=Some(true), serialize_segments=Some(true))?;
```

---

#### `parse_ssim_to_csv` / `ssim_to_csv` — Unchanged, Serializes to JSON by Default

CSV cannot represent nested types natively. When `condense_segments=True` is used with the
CSV functions, `segment_data` **always** serializes to JSON regardless of `serialize_segments`.
No changes are required for existing CSV usage.

```python
# Behavior unchanged — segment_data written as JSON string in the CSV
rs.parse_ssim_to_csv("schedule.ssim", "output.csv", condense_segments=True)
```

```rust
ssim_to_csv("schedule.ssim", "output.csv", condense_segments=True)
```

---

### Performance Improvements

Removing the default JSON serialization step from `condense_segments` eliminates a
per-row string encoding pass. Workflows using `parse_ssim_to_dataframe` or
`parse_ssim_to_parquets` with `condense_segments=True` will see a measurable reduction
in run time and allocations. The `parse_ssim_to_csv` path is unaffected as it still
requires serialization to write nested data into a flat file format.

---
