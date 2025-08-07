# RustySim CLI Documentation

A command-line interface for parsing SSIM (Standard Schedules Information Manual) files into CSV and Parquet formats with high performance and memory efficiency.

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
- **`--batch-size, -b`** `<NUMBER>`: Records to process per batch (default: 10,000)
- **`--help, -h`**: Show help for this command

#### Examples
```bash
# Basic usage
ssim csv -s ./data/schedule.ssim -o ./output/parsed_schedule.csv

# Process large file with custom batch size
ssim csv -s ./data/large_schedule.ssim -o ./output/large.csv -b 50000

# Using full argument names
ssim csv --ssim-path ./data/schedule.ssim --output-path ./output/schedule.csv --batch-size 25000
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
- **`--batch-size, -b`** `<NUMBER>`: Records to process per batch (default: 10,000)
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
ssim parquet -s ./data/huge_schedule.ssim -o ./output -c lz4 -b 100000

# Maximum compression for archival
ssim parquet -s ./data/schedule.ssim -o ./archive -c brotli -b 25000
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
ssim csv -s small.ssim -o output.csv -b 10000

# Medium files (100MB - 1GB) 
ssim csv -s medium.ssim -o output.csv -b 50000

# Large files (> 1GB)
ssim csv -s large.ssim -o output.csv -b 100000

# Memory-constrained systems
ssim csv -s any_size.ssim -o output.csv -b 5000
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
ssim csv -s large_file.ssim -o output.csv -b 5000

# Or use parquet format (more memory efficient)
ssim parquet -s large_file.ssim -o ./output -c lz4 -b 10000
```

**Slow processing:**
```bash
# Increase batch size (if you have enough memory)
ssim csv -s file.ssim -o output.csv -b 100000

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