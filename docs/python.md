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
print(f"Loaded {len(df)} records")
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
    condense_segments: bool = False
) -> pl.DataFrame
```

**Parameters:**
- **file_path** (str): Path to the SSIM file to parse
- **batch_size** (int, optional): Number of records to process in each batch for memory efficiency. Defaults to 10,000
- **buffer_size** (int, optional): Size of the read buffer in bytes for I/O operations. Larger values improve throughput for large files. Defaults to 8,192
- **condense_segments** (bool, optional): Consolidates all segment records (type 4) into a single `segment_data` column under their parent record (type 3). Produces flight-level rows with nested segment details—resulting in smaller files and faster processing. Defaults to False

**Returns:**
- **polars.DataFrame**: Combined DataFrame containing all flight schedule data with the following key columns:


**Example:**
```python
import rustyssim as rs

# Basic usage
df = rs.parse_ssim_to_dataframe("./data/schedule.ssim")
print(f"Parsed {len(df)} records")
print(df.head())

# Filter for specific airline
aa_flights = df.filter(df['airline_designator'] == 'AA')
print(f"American Airlines has {len(aa_flights)} scheduled flights")

# Memory-optimized processing for large files
df = rs.parse_ssim_to_dataframe("./data/large_schedule.ssim", batch_size=5000)

# Performance-optimized processing with larger buffer
df = rs.parse_ssim_to_dataframe(
    "./data/large_schedule.ssim",
    batch_size=50000,
    buffer_size=65536  # 64KB buffer for better I/O performance
)

# To convert to pandas.
pandas_df = df.to_pandas()
```




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

print(f"Carriers: {len(carriers)} records")
print(f"Flights: {len(flights)} records") 
print(f"Segments: {len(segments)} records")

# Analyze carrier information
print("Available airlines:", carriers['airline_designator'].unique().to_list())

# Examine flight routes
popular_routes = (flights
    .group_by(['departure_station', 'arrival_station'])
    .count()
    .sort('count', descending=True)
    .head(10)
)
print("Most frequent routes:")
print(popular_routes)
```

#### `parse_ssim_to_csv()`

Parse an SSIM file and write the results directly to a CSV file without loading too much into memory.

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
- **condense_segments** (bool, optional): Consolidates all segment records (type 4) into a single `segment_data` column under their parent record (type 3). Produces flight-level rows with nested segment details—resulting in smaller files and faster processing. Defaults to False

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

# Verify the output
import polars as pl
df = pl.read_csv("./output/schedule.csv")
print(f"CSV contains {len(df)} records")
```

#### `parse_ssim_to_parquets()`

Parse an SSIM file and write to separate Parquet files, with one file created per airline carrier for optimized querying and storage.

```python
def parse_ssim_to_parquets(
    file_path: str,
    output_path: str = ".",
    compression: str = "uncompressed",
    batch_size: int = 10000,
    buffer_size: int = 8192,
    condense_segments: bool = False
) -> None
```

**Parameters:**
- **file_path** (str): Path to the input SSIM file
- **output_path** (str, optional): Directory where Parquet files will be created. Defaults to current directory
- **compression** (str, optional): Parquet compression algorithm. Defaults to "uncompressed"
  - **Available options**: `snappy`, `gzip`, `lz4`, `zstd`, `uncompressed`, `brotli`, `lzo`
- **batch_size** (int, optional): Batch size for streaming processing. Defaults to 10,000
- **buffer_size** (int, optional): Size of the read buffer in bytes for I/O operations. Larger values improve throughput for large files. Defaults to 8,192
- **condense_segments** (bool, optional): Consolidates all segment records (type 4) into a single `segment_data` column under their parent record (type 3). Produces flight-level rows with nested segment details—resulting in smaller files and faster processing. Defaults to False

**Returns:**
- **None**: Function creates separate `.parquet` files for each airline

**File Output:** Creates files named `ssim_{airline_code}_control_duplicate_indicator.parquet` (e.g., `ssim_YY_.parquet`, `ssim_XX_X.parquet`) in the specified output directory.

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

# List generated files
parquet_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
print(f"Created {len(parquet_files)} parquet files:")
for file in sorted(parquet_files):
    print(f"  - {file}")

# Read specific airline data
import polars as pl
yy_data = pl.read_parquet(f"{output_dir}/ssim_YY_X.zstd.parquet")
print(f"YY Airlines: {len(yy_data)} flights")
```


## Example Workflows

### Complete Analysis Pipeline
```python
import rustyssim as rs
import polars as pl

# 1. Parse SSIM file
df = rs.parse_ssim_to_dataframe("./test_files/multi_ssim.dat")

# 2. Basic analysis
print(f"Total flights: {len(df)}")
print(f"Airlines: {df['airline_designator'].n_unique()}")
print(f"Routes: {df.select(['departure_station', 'arrival_station']).n_unique()}")

# 3. Top airlines by flight count
top_airlines = (df
    .group_by('airline_designator')
    .count()
    .sort('count', descending=True)
    .head(10)
)
print("Top airlines by flight count:")
print(top_airlines)
```

### Large File Processing
```python
import rustyssim as rs
import os

# For files > 1GB, use direct-to-parquet approach
input_file = "./test_files/huge_ssim.dat"
output_dir = "./airline_data"

# Process with optimized settings
rs.parse_ssim_to_parquets(
    file_path=input_file,
    output_path=output_dir,
    compression="lz4",  
    batch_size=100000,
    buffer_size=131072  # 128KB buffer for maximum throughput
)

```

## Condense segments

When `condense_segments=True` is passed to the parsing functions, all record
type 4 (segment) records are grouped under their parent record type 3 and
returned as a single column named `segment_data`. Each value in
`segment_data` is a list of dictionary-like structs (one per segment)
containing the parsed fields from the type 4 records. This keeps flight-level
rows flat while preserving associated segment details.

Behavior by output type:
- DataFrame: `segment_data` is a list column where each element is a list of
    struct/dict-like items. Use `explode` + `unnest` to expand segments into rows.
- CSV: `segment_data` is JSON-encoded; each row contains a JSON list of
    dictionaries in the `segment_data` column.
- Parquet: `segment_data` is a nested list/struct column, suitable for
    hierarchical processing in downstream tools.

Examples:

```python
import rustyssim as rs
import polars as pl

# Parse to a DataFrame with condensed segments
df = rs.parse_ssim_to_dataframe(
    "./data/schedule.ssim",
    batch_size=50_000,
    buffer_size=65536,
    condense_segments=True,
)


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

print(df_exploded.head())

# # If using CSV output, enable condense on write
rs.parse_ssim_to_csv(
    "./data/schedule.ssim",
    "./output/schedule.csv",
    batch_size=50_000,
    buffer_size=65536,
    condense_segments=True,
)

# For Parquet outputs the nested column is preserved for analytics
rs.parse_ssim_to_parquets(
    "./data/schedule.ssim",
    output_path="./out",
    batch_size=50_000,
    buffer_size=65536,
    condense_segments=True,
)
```

