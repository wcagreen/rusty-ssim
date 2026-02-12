//! # rustyssim
//!
//! A high-performance IATA SSIM (Standard Schedules Information Manual) parser
//! built in Rust. Parses SSIM files into [Polars](https://pola.rs) DataFrames
//! or exports directly to CSV/Parquet formats.
//!
//! ## Quick Start
//!
//! ```no_run
//! use rustyssim::ssim_to_dataframe;
//!
//! let df = ssim_to_dataframe("schedule.ssim", Some(10000), Some(8192), Some(false))
//!     .expect("Failed to parse SSIM file");
//!
//! println!("Parsed {} flight records", df.height());
//! ```
//!
//! ## Output Formats
//!
//! | Function | Output |
//! |----------|--------|
//! | [`ssim_to_dataframe`] | Single `DataFrame` |
//! | [`ssim_to_dataframes`] | Three `DataFrame`s (carriers, flights, segments) |
//! | [`ssim_to_csv`] | CSV file on disk |
//! | [`ssim_to_parquets`] | Parquet files (one per carrier) |
//!
//! ## Segment Condensing
//!
//! Functions that accept `condense_segments` can aggregate multiple segment
//! records for the same flight into a single JSON column, reducing row count
//! and file size.
//!
//! ## Performance Tuning
//!
//! - **`batch_size`**: Number of records processed per batch. Larger values
//!   use more memory but reduce overhead. Default: `10,000`.
//! - **`buffer_size`**: File read buffer in bytes. Increase for large files.
//!   Default: `8,192` (8 KB). Try `131,072` (128 KB) for large files.

// Re-export the public API
pub use rusty_ssim_core::{ssim_to_csv, ssim_to_dataframe, ssim_to_dataframes, ssim_to_parquets};

/// Re-exported [Polars](https://pola.rs) crate for working with the DataFrames
/// returned by this library.
///
/// Includes ["lazy", "parquet", "dtype-struct", "ipc", "performant", "json"] needed by rustyssim.
/// For additional features, add `polars` directly to your `Cargo.toml` —
pub use polars;