pub use rusty_ssim_core::stream_ssim_to_file;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ssim")]
#[command(about = "CLI for converting IATA SSIM files to other data formats.", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert SSIM to Dataframe.
    Df(SsimDfOptions),
}

#[derive(Args)]
struct SsimDfOptions {
    /// Path of the SSIM File
    #[arg(short, long, required = true)]
    ssim_path: String,

    /// FileName / Output path + filename
    #[arg(short, long, required = true)]
    output_path: String,

    /// File Type examples CSV or Parquet
    #[arg(short, long, default_value = "csv", required = true)]
    file_type: String,

    /// Parquet Compression Options options are  "snappy", "gzip", "lz4", "zstd", or "uncompressed"
    #[arg(short, long, default_value = "snappy", required = true)]
    compression: String,

    /// Batch size for streaming.
    #[arg(short, long, default_value = "10000")]
    batch_size: usize,

}

fn main() {
    let cli = Cli::parse();

    let Commands::Df(options) = &cli.command;

    stream_ssim_to_file(
        &options.ssim_path,
        &options.output_path,
        &options.file_type,
        Some(options.compression.as_str()),
        Some(options.batch_size),
    ).expect("Failed to process SSIM file.");
}