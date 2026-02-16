# rustyssim

A high-performance IATA SSIM (Standard Schedules Information Manual) parser for Rust. Parses SSIM files into [Polars](https://pola.rs) DataFrames or exports directly to CSV/Parquet formats.

[![Crates.io](https://img.shields.io/crates/v/rustyssim.svg)](https://crates.io/crates/rustyssim)
[![docs.rs](https://docs.rs/rustyssim/badge.svg)](https://docs.rs/rustyssim)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **🚀 Fast Performance**: Built in Rust with parallel processing via Rayon
- **💾 Memory Efficient**: Configurable batch processing for large SSIM files
- **📊 Multiple Output Formats**: In-memory DataFrames, CSV, and Parquet
- **🗜️ Flexible Compression**: Parquet compression options (zstd, lz4, snappy, etc.)
- **📦 Polars Re-exported**: Use `rustyssim::polars` without adding a separate dependency

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rustyssim = "0.5.2"
```

If you need additional Polars features (e.g., `csv`, `sql`), add it alongside:

```toml
[dependencies]
rustyssim = "0.5.2"
polars = { version = "0.50", features = ["csv", "sql"] }
```


## Polars Re-export

`rustyssim` re-exports [Polars](https://pola.rs) so you can work with the returned DataFrames without adding a separate dependency or worrying about version compatibility:


## Quick Start

```rust,no_run
use rustyssim::ssim_to_dataframe;
use rustyssim::polars::prelude::*;

fn main() {
    let df = ssim_to_dataframe("schedule.ssim", None, None, None)
        .expect("Failed to parse SSIM file");

    println!("Parsed {} flight records", df.height());
    println!("{}", df.head(Some(10)));
}
```

## API

### Parse to a Single DataFrame

Returns a single dataframe with carriers (type 2), flights (type 3), and segments (type 4) combine under one.
If you set condense_segments to True, it will aggregate segments into a JSON string column (smaller output). If false (default), each segment is a separate row.

```rust,no_run
use rustyssim::ssim_to_dataframe;

// All optional parameters accept None for defaults
let df = ssim_to_dataframe(
    "schedule.ssim",  // SSIM file path
    Some(10000),      // batch_size (default: 10,000)
    Some(8192),       // buffer_size in bytes (default: 8,192)
    Some(false),      // condense_segments (default: false)
).expect("Failed to parse SSIM file");
```

### Split by Record Type

Returns three DataFrames: carriers (type 2), flights (type 3), and segments (type 4).

```rust,no_run
use rustyssim::ssim_to_dataframes;

let (carriers, flights, segments) = ssim_to_dataframes(
    "schedule.ssim",
    Some(10000),  // batch_size
    Some(8192),   // buffer_size
).expect("Failed to parse SSIM file");

println!("Carriers: {}, Flights: {}, Segments: {}",
    carriers.height(), flights.height(), segments.height());
```

### Export to CSV

Writes and appends to single CSV file.

```rust,no_run
use rustyssim::ssim_to_csv;

ssim_to_csv(
    "schedule.ssim",
    "output.csv",
    Some(10000),   // batch_size
    Some(8192),    // buffer_size
    Some(false),   // condense_segments
).expect("Failed to parse SSIM file");
```

### Export to Parquet (One File Per Carrier)

```rust,no_run
use rustyssim::ssim_to_parquets;

ssim_to_parquets(
    "schedule.ssim",
    Some("./output"),   // output directory
    Some("zstd"),       // compression: zstd, snappy, lz4, gzip, or uncompressed
    Some(10000),        // batch_size
    Some(8192),         // buffer_size
    Some(false),        // condense_segments
).expect("Failed to parse SSIM file");
```

## Performance Tuning

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch_size` | 10,000 | Records per processing batch. Increase for throughput, decrease for memory |
| `buffer_size` | 8,192 (8 KB) | File read buffer. Use `131072` (128 KB) for large files |
| `condense_segments` | false | Aggregate segment records into a JSON column, reducing row count |

## Data Structure

The parser handles three SSIM record types per IATA standards:

| Record | Type | Contents |
|--------|------|----------|
| Carrier | 2 | Airline metadata |
| Flight Leg | 3 | Core flight information (route, times, aircraft) |
| Segment | 4 | Segment-level details (DEI codes, data elements) |

## Other Interfaces

This parser is also available as:

- **Python**: [`pip install rustyssim`](https://pypi.org/project/rustyssim/)
- **CLI**: [cli-rusty-ssim](https://github.com/wcagreen/rusty-ssim/tree/main/cli-rusty-ssim)

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run the test suite (`cargo test -p rustyssim`)
5. Submit a pull request

## Community & Support

- 🐛 **Issues**: [GitHub Issues](https://github.com/wcagreen/rusty-ssim/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/wcagreen/rusty-ssim/discussions)

## License

MIT — see [LICENSE](https://github.com/wcagreen/rusty-ssim/blob/main/LICENSE) for details.