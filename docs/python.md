# RustySSIM

A fast Rust-based Python module for parsing SSIM (Standard Schedules Information Manual) files into various formats including Polars DataFrames, CSV, and Parquet files.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [parse_ssim_to_dataframe()](#parse_ssim_to_dataframe)
  - [split_ssim_to_dataframes()](#split_ssim_to_dataframes)
  - [parse_ssim_to_csv()](#parse_ssim_to_csv)
  - [parse_ssim_to_parquets()](#parse_ssim_to_parquets)
- [Example Workflows](#example-workflows)
  - [Complete Analysis Pipeline](#complete-analysis-pipeline)
  - [Large File Processing](#large-file-processing)
  - [Condense Segments](#condense-segments)


## Installation

```bash
pip install rustyssim
```

## Quick Start

```python
import rustyssim as rs

# Parse SSIM file to a single DataFrame
df = rs.parse_ssim_to_dataframe("schedule.ssim")
print(f"Loaded {df.height} records")
```

## API Reference

### Core Functions

#### `parse_ssim_to_dataframe()`

Parse an SSIM file into a single Polars DataFrame containing all record types (carriers, flights, and segments).

```python
def parse_ssim_to_dataframe(
    file_path: str,
    batch_size: int = 10000,
    buffer_size: int = 8192,
    condense_segments: bool = False,
    serialize_segments: bool = False
) -> pl.DataFrame
```

**Parameters:**
- **file_path** (str): Path to the SSIM file to parse
- **batch_size** (int, optional): Number of records to process in each batch for memory efficiency. Defaults to 10,000
- **buffer_size** (int, optional): Size of the read buffer in bytes for I/O operations. Larger values improve throughput for large files. Defaults to 8,192
- **condense_segments** (bool, optional): Consolidates all segment records (type 4) into a single `segment_data` column under their parent record (type 3). Produces flight-level rows with nested segment details — resulting in fewer rows and faster processing. When `True`, `segment_data` is returned as a native `List<Struct>` column. Defaults to `False`
- **serialize_segments** (bool, optional): Only applies when `condense_segments=True`. If `True`, serializes `segment_data` to a JSON string column instead of `List<Struct>`. Defaults to `False`

**Returns:**
- **polars.DataFrame**: Combined DataFrame containing all flight schedule data

**Example:**
```python
import rustyssim as rs

# Basic usage
df = rs.parse_ssim_to_dataframe("./data/schedule.ssim")
print(f"Parsed {df.height} records")
print(df.head())

# Filter for specific airline
aa_flights = df.filter(df['airline_designator'] == 'AA')
print(f"American Airlines has {aa_flights.height()} scheduled flights")

# Memory-optimized processing for large files
df = rs.parse_ssim_to_dataframe("./data/large_schedule.ssim", batch_size=10000)

# Performance-optimized processing with larger buffer
df = rs.parse_ssim_to_dataframe(
    "./data/large_schedule.ssim",
    batch_size=50000,
    buffer_size=65536  # 64KB buffer for better I/O performance
)

# Condense segments into a List<Struct> column (default as of 0.6.0)
df = rs.parse_ssim_to_dataframe("./data/schedule.ssim", condense_segments=True)

# Condense segments and serialize to JSON strings for backward compatibility.
df = rs.parse_ssim_to_dataframe(
    "./data/schedule.ssim",
    condense_segments=True,
    serialize_segments=True
)

# Convert to pandas
pandas_df = df.to_pandas()
```

---

#### `split_ssim_to_dataframes()`

Parse an SSIM file into three separate DataFrames for carriers, flights, and segments, providing more granular access to different data types.

```python
def split_ssim_to_dataframes(
    file_path: str,
    batch_size: int = 10000,
    buffer_size: int = 8192
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
```

**Parameters:**
- **file_path** (str): Path to the SSIM file to parse
- **batch_size** (int, optional): Batch size for streaming processing. Defaults to 10,000
- **buffer_size** (int, optional): Size of the read buffer in bytes for I/O operations. Larger values improve throughput for large files. Defaults to 8,192

**Returns:**
- **tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]**: Three separate DataFrames:
  1. **Carriers DataFrame**: Airline information and metadata
  2. **Flights DataFrame**: Flight-level information (routes, times, aircraft)
  3. **Segments DataFrame**: Individual flight segment details

**Example:**
```python
import rustyssim as rs

# Parse into separate DataFrames
carriers, flights, segments = rs.split_ssim_to_dataframes("./data/schedule.ssim")

print(f"Carriers: {carriers.height} records")
print(f"Flights: {flights.height} records")
print(f"Segments: {segments.height} records")

# Analyze carrier information
print("Available airlines:", carriers['airline_designator'].unique().to_list())

# Examine flight routes
popular_routes = (rt3
    .group_by(['departure_station', 'arrival_station'])
    .len("count")
    .sort('count', descending=True)
    .head(10)
)
print("Most frequent routes:")
print(popular_routes)
```

---

#### `parse_ssim_to_csv()`

Parse an SSIM file and write the results directly to a CSV file without loading everything into memory.

```python
def parse_ssim_to_csv(
    file_path: str,
    output_path: str,
    batch_size: int = 10000,
    buffer_size: int = 8192,
    condense_segments: bool = False
) -> None
```

**Parameters:**
- **file_path** (str): Path to the input SSIM file
- **output_path** (str): Path where the output CSV file will be created
- **batch_size** (int, optional): Batch size for streaming processing. Defaults to 10,000
- **buffer_size** (int, optional): Size of the read buffer in bytes for I/O operations. Larger values improve throughput for large files. Defaults to 8,192
- **condense_segments** (bool, optional): Consolidates all segment records (type 4) into a single `segment_data` column under their parent record (type 3). When `True`, `segment_data` is always written as a JSON string — CSV cannot represent nested types natively. Defaults to `False`

> **Note:** `parse_ssim_to_csv` does not have a `serialize_segments` parameter. When `condense_segments=True`, JSON serialization is always applied automatically to ensure valid CSV output.

**Returns:**
- **None**: Function writes directly to file

**Example:**
```python
import rustyssim as rs

# Direct file-to-file conversion
rs.parse_ssim_to_csv(
    file_path="./data/schedule.ssim",
    output_path="./output/schedule.csv"
)

# Process large files with larger batch size for better performance
rs.parse_ssim_to_csv(
    file_path="./data/large_schedule.ssim",
    output_path="./output/large_schedule.csv",
    batch_size=50000,
    buffer_size=65536  # 64KB buffer for better I/O throughput
)

# Condense segments — written as JSON strings in the CSV
rs.parse_ssim_to_csv(
    "./data/schedule.ssim",
    "./output/schedule.csv",
    condense_segments=True,
)

# Verify the output
import polars as pl
df = pl.read_csv("./output/schedule.csv")
print(f"CSV contains {df.height} records")
```

---

#### `parse_ssim_to_parquets()`

Parse an SSIM file and write to separate Parquet files, with one file created per airline carrier for optimized querying and storage.

```python
def parse_ssim_to_parquets(
    file_path: str,
    output_path: str = ".",
    compression: str = "uncompressed",
    batch_size: int = 10000,
    buffer_size: int = 8192,
    condense_segments: bool = False,
    serialize_segments: bool = False
) -> None
```

**Parameters:**
- **file_path** (str): Path to the input SSIM file
- **output_path** (str, optional): Directory where Parquet files will be created. Defaults to current directory
- **compression** (str, optional): Parquet compression algorithm. Defaults to `"uncompressed"`
  - **Available options**: `snappy`, `gzip`, `lz4`, `zstd`, `uncompressed`, `brotli`, `lzo`
- **batch_size** (int, optional): Batch size for streaming processing. Defaults to 10,000
- **buffer_size** (int, optional): Size of the read buffer in bytes for I/O operations. Larger values improve throughput for large files. Defaults to 8,192
- **condense_segments** (bool, optional): Consolidates all segment records (type 4) into a single `segment_data` column under their parent record (type 3). When `True`, `segment_data` is a native `List<Struct>` column in the Parquet output, suitable for hierarchical processing in downstream tools. Defaults to `False`
- **serialize_segments** (bool, optional): Only applies when `condense_segments=True`. If `True`, serializes `segment_data` to a JSON string column instead of `List<Struct>`. Useful for ETL pipelines that expect string output. Defaults to `False`

**Returns:**
- **None**: Function creates separate `.parquet` files for each airline

**File Output:** Creates files named `ssim_{airline_code}_{control_duplicate_indicator}.parquet` (e.g., `ssim_YY_.parquet`, `ssim_XX_X.parquet`) in the specified output directory.

**Example:**
```python
import rustyssim as rs
import os

# Basic usage - creates parquet files in current directory
rs.parse_ssim_to_parquets("./test_files/multi_ssim.dat")

# Specify output directory and use efficient compression
output_dir = "./output/parquet_files"
rs.parse_ssim_to_parquets(
    file_path="./test_files/multi_ssim.dat",
    output_path=output_dir,
    compression="zstd"
)

# Condense segments as List<Struct> in Parquet (default as of 0.6.0)
rs.parse_ssim_to_parquets(
    "./data/schedule.ssim",
    output_path="./out",
    condense_segments=True,
)

# Condense segments and serialize to JSON strings (for ETL pipelines)
rs.parse_ssim_to_parquets(
    "./data/schedule.ssim",
    output_path="./out",
    condense_segments=True,
    serialize_segments=True,
)

# List generated files
parquet_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
print(f"Created {len(parquet_files)} parquet files:")
for file in sorted(parquet_files):
    print(f"  - {file}")

# Read specific airline data
import polars as pl
yy_data = pl.read_parquet(f"{output_dir}/ssim_YY_X.zstd.parquet")
print(f"YY Airlines: {yy.heigt} flights")
```

---

## Example Workflows

### Complete Analysis Pipeline
```python
import rustyssim as rs
import polars as pl


FILE = "single_test.ssim"
BATCH = 50_000
BUF = 262144


# 1. Parse SSIM file
df = rs.parse_ssim_to_dataframe(FILE, batch_size=BATCH, buffer_size=BUF, condense_segments=True)

# 2. Basic analysis
print(f"Total flights: {df.height}")
print(f"Airlines: {df['airline_designator'].n_unique()}")
print(f"Routes: {df.select(['departure_station', 'arrival_station']).n_unique()}")

# 3. Top airlines by flight count
top_airlines = (df
    .group_by('airline_designator')
    .len("count")
    .sort('count', descending=True)
    .head(10)
)
print("Top airlines by flight count:")
print(top_airlines)

```

## Condense Segments

When `condense_segments=True` is passed, all record type 4 (segment) records are grouped
under their parent record type 3 and returned as a single column named `segment_data`.
Each value in `segment_data` contains one entry per segment with the following fields:

| Field | Description |
|---|---|
| `board_point_indicator` | Board point indicator code |
| `off_point_indicator` | Off point indicator code |
| `board_point` | Boarding airport IATA code |
| `off_point` | Off-point airport IATA code |
| `data_element_identifier` | DEI code |
| `data` | Associated data value |

### Behavior by Output Type

**DataFrame / Parquet** — `segment_data` is a native `List<Struct>` column (default as of 0.6.0).
Use `explode` + `unnest` to expand into flat rows, or pass `serialize_segments=True` to get
JSON strings instead.

**CSV** — `segment_data` is always JSON-encoded, since CSV cannot represent nested types.
`serialize_segments` has no effect here.

### Examples

```python
import rustyssim as rs
import polars as pl

# --- DataFrame: List<Struct> (default) ---

df = rs.parse_ssim_to_dataframe(
    "./data/schedule.ssim",
    condense_segments=True,
)

# Expand segments into flat rows
df_exploded = (
    df.explode("segment_data")
      .unnest("segment_data")
)
print(df_exploded.head())


# --- DataFrame: JSON strings (for old method results) ---

df = rs.parse_ssim_to_dataframe(
    "./data/schedule.ssim",
    condense_segments=True,
    serialize_segments=True,
)

# segment_data column contains JSON strings, e.g.:
# '[{"board_point":"AMS","off_point":"GRQ","data_element_identifier":"050",...}]'

# Expand Serialize segments into flat rows

import polars as pl
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

df_exploded = (
    df.explode("segment_data").str.json_decode(schema).alias("segment_data")
      .unnest("segment_data")
)
print(df_exploded.head())



# --- CSV: always JSON-encoded ---

rs.parse_ssim_to_csv(
    "./data/schedule.ssim",
    "./output/schedule.csv",
    condense_segments=True,
)


# --- Parquet: List<Struct> (default) ---

rs.parse_ssim_to_parquets(
    "./data/schedule.ssim",
    output_path="./out",
    condense_segments=True,
)


# --- Parquet: JSON strings (for old method results) ---

rs.parse_ssim_to_parquets(
    "./data/schedule.ssim",
    output_path="./out",
    condense_segments=True,
    serialize_segments=True,
)
```