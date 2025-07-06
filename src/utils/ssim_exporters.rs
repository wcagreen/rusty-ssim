use polars::frame::DataFrame;
use polars::prelude::CsvWriter;
use polars::prelude::ParquetCompression;
use polars::prelude::ParquetWriter;
use polars::prelude::PolarsResult;
use polars::prelude::SerWriter;
use std::fs::File;

/// Writes a Polars DataFrame to a Parquet file with the specified compression.
///
/// # Arguments
/// * `dataframe` - The DataFrame to write.
/// * `file_path` - The output file path.
/// * `compression` - Compression type: \"snappy\", \"gzip\", \"lz4\", \"zstd\", or \"uncompressed\".
///
/// # Errors
/// Returns a `PolarsResult<()>` if writing fails.
pub fn to_parquet(
    dataframe: &mut DataFrame,
    file_path: &str,
    compression: &str,
) -> PolarsResult<()> {
    let mut file = File::create(file_path).expect("could not create file");

    let compression = match compression.to_lowercase().as_str() {
        "snappy" => ParquetCompression::Snappy,
        "gzip" => ParquetCompression::Gzip(None),
        "lz4" => ParquetCompression::Lz4Raw,
        "zstd" => ParquetCompression::Zstd(None),
        "uncompressed" | "none" => ParquetCompression::Uncompressed,
        _ => ParquetCompression::Uncompressed,
    };

    ParquetWriter::new(&mut file)
        .with_compression(compression)
        .finish(dataframe)
        .map(|_| ())
}

/// Writes a Polars DataFrame to a CSV file at the specified path.
///
/// # Arguments
/// * `dataframe` - The DataFrame to write.
/// * `file_path` - The output file path.
///
/// # Behavior
/// The CSV will include a header row and use a comma as the separator.
///
/// # Errors
/// Returns a `PolarsResult<()>` if writing fails.
pub fn to_csv(dataframe: &mut DataFrame, file_path: &str) -> PolarsResult<()> {
    let mut file = File::create(file_path).expect("could not create file");

    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(dataframe)
}
