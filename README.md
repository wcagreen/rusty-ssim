# rusty-ssim

A high-performance Rust-built IATA SSIM (Standard Schedules Information Manual) parser that can be used via CLI, Python or Rust. This tool efficiently parses SSIM files into Polars DataFrames or exports directly to CSV/Parquet formats with streaming support for large files.

## Quick Start

### Python (Most Common Use Case)

```python
# Install
pip install rustyssim

# Parse SSIM file to DataFrame
import rustyssim as rs
df = rs.parse_ssim_to_dataframe("path/to/schedule.ssim")
print(f"Parsed {len(df)} flight records")
```

- **Fast Performance**: Built in Rust for optimal parsing speed
- **Memory Efficient**: Streaming support for large SSIM files
- **Multiple Output Formats**: CSV, Parquet, and in-memory DataFrames
- **Flexible Compression**: Support for various Parquet compression options
- **Dual Interface**: Both CLI and Python APIs available


## Installation

### Python Installation (Recommended)

Install directly from PyPI:

```bash
pip install rustyssim
```

**Requirements:**
- Python 3.9+

### Development Installation

For development or the latest features, install from source:

```bash
git clone https://github.com/wcagreen/rusty-ssim.git
cd rusty-ssim
pip install maturin
maturin develop -m py-rusty-ssim/Cargo.toml
```

**Requirements for source installation:**
- Python 3.9+
- Rust toolchain

### CLI Installation

If you need the standalone CLI tool:

```bash
git clone https://github.com/wcagreen/rusty-ssim.git
cd rusty-ssim
cargo build -p cli-rusty-ssim --release
```

The binary will be located at `target/release/cli-rusty-ssim`. Add it to your PATH for global access:

```bash
# On Linux/macOS
export PATH="$PATH:/path/to/rusty-ssim/target/release"

# On Windows
# Add the target/release directory to your system PATH
```

## Usage

### CLI Usage

The CLI provides two main commands for different output formats:

#### Parse to CSV
**Options:**
- `--ssim-path, -s`: Path to the SSIM file (required)
- `--output-path, -o`: Output CSV file path (required)
- `--batch-size, -b`: Batch size for streaming (default: 10000)

**Example:**
```bash
ssim csv -s ./data/schedule.ssim -o ./output/parsed_schedule.csv -b 5000
```

#### Parse to Parquet

**Options:**
- `--ssim-path, -s`: Path to the SSIM file (required)
- `--output-path, -o`: Output directory path (default: current directory)
- `--compression, -c`: Compression type (default: "uncompressed")
    - Available options: `snappy`, `gzip`, `lz4`, `zstd`, `uncompressed`, `brotli`, `lzo`
- `--batch-size, -b`: Batch size for streaming (default: 10000)

**Example:**
```bash
cli-rusty-ssim parquet -s ./data/schedule.ssim -o ./output -c zstd -b 15000
```

### Python API

Import the module:

```python
import rustyssim as rs
```

#### Core Functions

##### `parse_ssim_to_dataframe()`

Parse an SSIM file into a single Polars DataFrame containing record type 2, 3, and 4 information.

```python
def parse_ssim_to_dataframe(
    file_path: str,
    batch_size: int = 10000
) -> pl.DataFrame
```

**Parameters:**
- `file_path` (str): Path to the SSIM file
- `batch_size` (int, optional): Batch size for streaming processing. Default: 10000

**Returns:**
- `polars.DataFrame`: Combined DataFrame with flight and segment information

**Example:**
```python
import rustyssim as rs

# Basic usage
df = rs.parse_ssim_to_dataframe("./data/schedule.ssim")
print(f"Parsed {len(df)} records")
print(df.head())

# With custom batch size for large files
df = rs.parse_ssim_to_dataframe("./data/large_schedule.ssim", batch_size=5000)
```

##### `split_ssim_to_dataframes()`

Parse an SSIM file into three separate DataFrames for carriers, flights, and segments.

```python
def split_ssim_to_dataframes(
    file_path: str,
    batch_size: int = 10000
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
```

**Parameters:**
- `file_path` (str): Path to the SSIM file
- `batch_size` (int, optional): Batch size for streaming processing. Default: 10000

**Returns:**
- `tuple`: Three DataFrames (carriers, flights, segments)

**Example:**
```python
import rustyssim as rs

# Parse into separate DataFrames
carriers, flights, segments = rs.split_ssim_to_dataframes("./data/schedule.ssim")

print(f"Carriers: {len(carriers)} records")
print(f"Flights: {len(flights)} records") 
print(f"Segments: {len(segments)} records")

# Access specific data
print("Available airlines:", carriers['airline_designator'].unique().to_list())
print("Flight routes:", flights[['departure_station', 'arrival_station']].head())
```

##### `parse_ssim_to_csv()`

Parse an SSIM file and write directly to a CSV file.

```python
def parse_ssim_to_csv(
    file_path: str,
    output_path: str,
    batch_size: int = 10000
) -> None
```

**Parameters:**
- `file_path` (str): Path to the SSIM file
- `output_path` (str): Output CSV file path
- `batch_size` (int, optional): Batch size for streaming processing. Default: 10000


**Example:**
```python
import rustyssim as rs

# Direct file-to-file conversion
rs.parse_ssim_to_csv(
    file_path="./data/schedule.ssim",
    output_path="./output/schedule.csv"
)

# With custom batch size for memory optimization
rs.parse_ssim_to_csv(
    file_path="./data/large_schedule.ssim",
    output_path="./output/large_schedule.csv",
    batch_size=5000
)
```

##### `parse_ssim_to_parquets()`

Parse an SSIM file and write to separate Parquet files (one per carrier).

```python
def parse_ssim_to_parquets(
    file_path: str,
    output_path: str = ".",
    compression: str = "uncompressed",
    batch_size: int = 10000
) -> None
```

**Parameters:**
- `file_path` (str): Path to the SSIM file
- `output_path` (str, optional): Directory path. Default: current directory
- `compression` (str, optional): Parquet compression algorithm. Default: "uncompressed"
  - Available options: `snappy`, `gzip`, `lz4`, `zstd`, `uncompressed`, `brotli`, `lzo`
- `batch_size` (int, optional): Batch size for streaming processing. Default: 10000


**Example:**
```python
import rustyssim as rs

# Basic usage with default settings
rs.parse_ssim_to_parquets("./data/schedule.ssim")

# Specify output directory and compression
rs.parse_ssim_to_parquets(
    file_path="./data/schedule.ssim",
    output_path="./output/parquet_files",
    compression="zstd"
)

# Memory-optimized processing for large files
rs.parse_ssim_to_parquets(
    file_path="./data/very_large_schedule.ssim",
    output_path="./output",
    compression="lz4",
    batch_size=2000
)
```

## Data Structure

The parser handles three types of SSIM records:

### Carrier Records (Type 2)
Contains airline and schedule metadata:
- `airline_designator`: IATA or ICAO airline code
- `control_duplicate_indicator`: Control Duplicate Indicator for IATA Airline Code.
- `time_mode`: Time format specification
- `season`: Schedule season
- `period_of_schedule_validity_from/to`: Valid date range
- `creation_date`: When the schedule was created
- `title_of_data`: Schedule description

### Flight Records (Type 3)
Contains flight leg information:
- `flight_designator`: Unique flight identifier
- `airline_designator`: IATA or ICAO airline code
- `control_duplicate_indicator`: Control Duplicate Indicator for IATA Airline Code.
- `flight_number`: Flight number
- `departure_station`/`arrival_station`: Airport codes
- `scheduled_time_of_passenger_departure/arrival`: Schedule times
- `aircraft_type`: Aircraft model
- `days_of_operation`: Operating days pattern
- `period_of_operation_from/to`: Operating date range

### Segment Records (Type 4)
Contains additional flight segment data:
- `flight_designator`: Links to flight record
- `airline_designator`: IATA or ICAO airline code
- `control_duplicate_indicator`: Control Duplicate Indicator for IATA Airline Code.
- `board_point`/`off_point`: Segment endpoints
- `data_element_identifier`: Type of segment data
- `data`: Segment-specific information


## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
