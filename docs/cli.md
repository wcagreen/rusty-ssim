# RustySim CLI Documentation

A command-line interface for parsing SSIM (Standard Schedules Information Manual) files into CSV and Parquet formats with high performance and memory efficiency.

## Table of Contents

- [Installation](#installation)
  - [Build from Source (Current)](#build-from-source-current)
  - [Prerequisites](#prerequisites)
  - [Future Installation Options](#future-installation-options)
- [Quick Start](#quick-start)
- [Commands Overview](#commands-overview)
- [Command Reference](#command-reference)
  - [ssim csv - Convert to CSV](#ssim-csv---convert-to-csv)
  - [ssim parquet - Convert to Parquet](#ssim-parquet---convert-to-parquet)
- [Performance Guide](#performance-guide)
  - [Batch Size Optimization](#batch-size-optimization)
  - [Buffer Size Optimization](#buffer-size-optimization)
  - [Format Selection Guide](#format-selection-guide)
- [Troubleshooting](#troubleshooting)
  - [Common Issues](#common-issues)
  - [Getting Help](#getting-help)

## Installation

### Build from Source (Current)
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

### Prerequisites
- **Rust**: Install from [rustup.rs](https://rustup.rs)

### Future Installation Options
*Coming soon:*
- `pip install rustyssim` - PyPI package  
- `docker run rustyssim` - Docker container

## Quick Start
```bash
# Parse SSIM to CSV (simplest usage)
ssim csv -s schedule.ssim -o schedule.csv

# Parse SSIM to Parquet files with compression
ssim parquet -s schedule.ssim -o ./output -c zstd
```

## Commands Overview

The CLI provides two main commands for different output formats:

| Command | Purpose | Output |
|---------|---------|--------|
| `csv` | Convert SSIM to single CSV file | One CSV file with all data |
| `parquet` | Convert SSIM to Parquet files | Multiple files (one per airline) |

---

## Command Reference

### `ssim csv` - Convert to CSV

Parse an SSIM file and output a single CSV file containing all flight schedule data.

#### Usage
```bash
ssim csv [OPTIONS]
```

#### Required Arguments
- **`--ssim-path, -s`** `<FILE>`: Path to the input SSIM file

#### Options
- **`--output-path, -o`** `<FILE>`: Output CSV file path *(required)*
- **`--batch-size`** `<NUMBER>`: Records to process per batch (default: 10,000)
- **`--buffer-size`** `<NUMBER>`: I/O buffer size in bytes (default: 8,192)
- **`--help, -h`**: Show help for this command
- **`--condense-segments`**: Groups segment records (type 4) into a `segment_data` 
    column nested under their parent record (type 3). Produces flight-level rows 
    with nested segment details—resulting in smaller files and faster processing. 
    (default: disabled)

#### Examples
```bash
# Basic usage
ssim csv -s ./data/schedule.ssim -o ./output/parsed_schedule.csv

# Process large file with custom batch size
ssim csv -s ./data/large_schedule.ssim -o ./output/large.csv --batch-size 50000

# Optimize I/O performance with larger buffer
ssim csv -s ./data/large_schedule.ssim -o ./output/large.csv --batch-size 50000 --buffer-size 65536

# Using full argument names
ssim csv --ssim-path ./data/schedule.ssim --output-path ./output/schedule.csv --batch-size 25000

# Condense segments into a single `segment_data` column (JSON in CSV)
ssim csv -s ./data/schedule.ssim -o ./output/parsed_schedule.csv --condense-segments
```

---

### `ssim parquet` - Convert to Parquet

Parse an SSIM file and create separate Parquet files for each airline, optimized for analytics and querying.

#### Usage
```bash
ssim parquet [OPTIONS]
```

#### Required Arguments
- **`--ssim-path, -s`** `<FILE>`: Path to the input SSIM file

#### Options
- **`--output-path, -o`** `<DIRECTORY>`: Output directory path (default: current directory)
- **`--compression, -c`** `<TYPE>`: Compression algorithm (default: "uncompressed")
- **`--batch-size`** `<NUMBER>`: Records to process per batch (default: 10,000)
- **`--buffer-size`** `<NUMBER>`: I/O buffer size in bytes (default: 8,192)
- **`--condense-segments`**: Groups segment records (type 4) into a `segment_data` 
    column nested under their parent record (type 3). Produces flight-level rows 
    with nested segment details—resulting in smaller files and faster processing. 
    (default: disabled)
- **`--help, -h`**: Show help for this command

#### Compression Options
| Option | Speed | Compression | Use Case |
|--------|--------|-------------|-----------|
| `uncompressed` | Fastest | None | Development, fast access |
| `lz4` | Very Fast | Good | Large files, fast reads/writes |
| `snappy` | Fast | Good | General-purpose, cross-platform use|
| `zstd` | Medium | Excellent | Production, storage-efficient pipelines|
| `gzip` | Slow | Very Good | Legacy compatibility |
| `brotli` | Slowest | Best | Maximum compression needed |
| `lzo` | Fast | Moderate | Streaming applications |

#### Examples
```bash
# Basic usage (uncompressed, current directory)
ssim parquet -s ./data/schedule.ssim

# Specify output directory
ssim parquet -s ./data/schedule.ssim -o ./parquet_output

# Use compression for smaller files
ssim parquet -s ./data/schedule.ssim -o ./output -c zstd

# Optimize for large file processing
ssim parquet -s ./data/huge_schedule.ssim -o ./output -c lz4 --batch-size 100000 --buffer-size 131072

# Maximum compression for archival
ssim parquet -s ./data/schedule.ssim -o ./archive -c brotli --batch-size 25000

# Condense segments into a nested `segment_data` column per record type 3
ssim parquet -s ./data/schedule.ssim -o ./output --condense-segments
```

#### Output Format
Creates separate `.parquet` files in the output directory:
- `ssim_YY_.parquet` - American Airlines flights
- `ssim_XX_X.parquet` - Delta Airlines flights  
- ... (one file per airline in the SSIM data)

---

## Performance Guide

### Batch Size Optimization

Choose batch size based on your file size and available memory:
```bash
# Small files (< 100MB)
ssim csv -s small.ssim -o output.csv --batch-size 10000

# Medium files (100MB - 1GB) 
ssim csv -s medium.ssim -o output.csv --batch-size 50000

# Large files (> 1GB)
ssim csv -s large.ssim -o output.csv --batch-size 100000

# Memory-constrained systems
ssim csv -s any_size.ssim -o output.csv --batch-size 5000
```

### Buffer Size Optimization

Tune I/O buffer size for better throughput on large files:
```bash
# Default (8KB) - Good for most files
ssim csv -s schedule.ssim -o output.csv

# Medium buffer (64KB) - Better for large files
ssim csv -s large.ssim -o output.csv --buffer-size 65536

# Large buffer (128KB) - Maximum throughput
ssim parquet -s huge.ssim -o ./output -c lz4 --buffer-size 131072

# Combined optimization for very large files
ssim parquet -s huge.ssim -o ./output -c lz4 --batch-size 100000 --buffer-size 131072
```

### Format Selection Guide

**Choose CSV when:**
- You need a single file with all data
- Importing into Excel or similar tools
- Simple data analysis requirements
- Maximum compatibility needed

**Choose Parquet when:**
- Working with analytics tools (Pandas, Polars, Spark)
- Need to query specific airlines efficiently
- Storage space is a concern
- Building data pipelines
- Want each carrier in it's own parquet.

---

## Troubleshooting

### Common Issues

**File not found error:**
```bash
# Check file path
ls -la /path/to/your/file.ssim
# Use absolute paths if needed
ssim csv -s "$(pwd)/data/schedule.ssim" -o output.csv
```

**Permission denied:**
```bash
# Check output directory permissions
mkdir -p ./output
chmod 755 ./output
ssim parquet -s schedule.ssim -o ./output
```

**Out of memory with large files:**
```bash
# Reduce batch size
ssim csv -s large_file.ssim -o output.csv --batch-size 5000

# Or use parquet format (more memory efficient)
ssim parquet -s large_file.ssim -o ./output -c lz4 --batch-size 10000
```

**Slow processing:**
```bash
# Increase batch size (if you have enough memory)
ssim csv -s file.ssim -o output.csv --batch-size 100000

# Increase buffer size for better I/O throughput
ssim csv -s file.ssim -o output.csv --batch-size 50000 --buffer-size 65536

# Use faster compression
ssim parquet -s file.ssim -o ./output -c lz4
```

### Getting Help
```bash
# General help
ssim --help

# Command-specific help
ssim csv --help
ssim parquet --help

# Version information
ssim --version
```