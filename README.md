# rusty-ssim

A high-performance Rust-built IATA SSIM (Standard Schedules Information Manual) parser that can be used via CLI, Python, or Rust. This tool efficiently parses SSIM files into Polars DataFrames or exports directly to CSV/Parquet formats with streaming support for large files.

[![RustySSIM PyPI Build](https://github.com/wcagreen/rusty-ssim/actions/workflows/publish-to-pypi.yml/badge.svg)](https://github.com/wcagreen/rusty-ssim/actions/workflows/publish-to-pypi.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

## Features

- **🚀 Fast Performance**: Built in Rust for optimal parsing speed
- **💾 Memory Efficient**: Optimize for large SSIM files  
- **📊 Multiple Output Formats**: CSV, Parquet, and in-memory DataFrames
- **🗜️ Flexible Compression**: Support for various Parquet compression options (zstd, lz4, snappy, etc.)
- **🔧 Tooling Options**: Both CLI and Python APIs available
- **📈 Production Ready**: Handles files of any size with configurable batch processing

## Quick Start

### Python (Most Common Use Case)

```python
import rustyssim as rs

# Parse SSIM file to DataFrame
df = rs.parse_ssim_to_dataframe("path/to/schedule.ssim")
print(f"Parsed {len(df)} flight records")

# Split into separate DataFrames by record type
carriers, flights, segments = rs.split_ssim_to_dataframes("schedule.ssim")

# Direct export to optimized formats
rs.parse_ssim_to_csv("schedule.ssim", "output.csv")
rs.parse_ssim_to_parquets("schedule.ssim", "./parquet_files", compression="zstd")
```

### CLI (For Data Processing Pipelines)

```bash
# Convert to CSV
ssim csv -s schedule.ssim -o output.csv

# Convert to compressed Parquet files (one per airline)
ssim parquet -s schedule.ssim -o ./output -c zstd -b 50000
```

## Installation

### Current Installation (Build from Source)

```bash
# Clone the repository
git clone https://github.com/wcagreen/rusty-ssim.git
cd rusty-ssim

# Install Python package
pip install maturin
maturin develop -m py-rusty-ssim/Cargo.toml

# Build CLI tool
cargo build -p cli-rusty-ssim --release
```

**Requirements:**
- Python 3.9+
- Rust toolchain ([rustup.rs](https://rustup.rs))
- Build tools (build-essential, Xcode CLI tools, or VS Build Tools)

### Future Installation Options
*Coming soon:*
- `pip install rustyssim` - PyPI package
- Docker container with pre-built binaries

## Documentation

### 📖 [Python API Documentation](https://github.com/wcagreen/rusty-ssim/blob/main/docs/python.md)
Complete reference for all Python functions with examples, parameters, and return values.

### 💻 [CLI Documentation](https://github.com/wcagreen/rusty-ssim/blob/main/docs/cli-usage.md) 
Comprehensive guide for command-line usage, performance tuning, and integration examples.

## Data Structure

The parser handles three types of SSIM records according to IATA standards:

### Carrier Records (Type 2)
Contains airline and schedule metadata.

### Flight Records (Type 3)
Contains core flight leg information.

### Segment Records (Type 4)  
Contains flight segment information.



## Use Cases

### Data Analytics & Business Intelligence
```python
# Analyze route networks
df = rs.parse_ssim_to_dataframe("schedule.ssim")
routes = df.group_by(['departure_station', 'arrival_station']).count()

# Export for Tableau, Power BI, etc.
rs.parse_ssim_to_csv("schedule.ssim", "analytics_export.csv")
```

### Data Engineering Pipelines
```bash
# Batch processing in ETL pipelines
ssim parquet -s "./huge_multi_carrier_ssim.dat" -o /data/processed/ -c zstd -b 100000

```

### Airlines & Aviation Analytics
```python
# Split by carrier for airline-specific analysis
carriers, flights, segments = rs.split_ssim_to_dataframes("schedule.ssim")

# Analyze specific airline operations
aa_flights = flights.filter(flights['airline_designator'] == 'AA')
capacity_analysis = aa_flights.group_by('aircraft_type').agg([
    pl.count().alias('flights'),
    pl.col('departure_station').n_unique().alias('origins')
])
```

## Development

### Running Tests
```bash
# Rust tests
cargo test

# Python tests  
pip install pytest
pytest tests/
```


## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

### Quick Contribution Steps
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run the test suite (`cargo test && pytest`)
5. Submit a pull request

## Community & Support

- 🐛 **Issues**: [GitHub Issues](https://github.com/wcagreen/rusty-ssim/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/wcagreen/rusty-ssim/discussions)
- 📧 **Contact**: Create an issue for questions or feature requests

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<details>
<summary>📋 Project Structure</summary>

```
rusty-ssim/
├── cli-rusty-ssim/          # CLI application
├── py-rusty-ssim/           # Python bindings  
├── rusty-ssim-core/         # Core Rust library
├── docs/                    # Documentation
```
</details>