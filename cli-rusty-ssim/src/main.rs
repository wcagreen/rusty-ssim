use clap::{Args, Parser, Subcommand};
use rusty_ssim_core::{ssim_to_csv, ssim_to_parquets};

#[derive(Parser)]
#[command(name = "ssim")]
#[command(about = "CLI for converting IATA SSIM files to other data formats.", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Parse SSIM file to Parquet(s).
    Parquet(SsimParquetOptions),
    /// Parse SSIM file to CSV.
    Csv(SsimCsvOptions),
}

#[derive(Args)]
struct SsimParquetOptions {
    /// Path of the SSIM File
    #[arg(short, long, required = true)]
    ssim_path: String,

    /// Output directory path.
    #[arg(short, long, default_value = ".")]
    output_path: String,

    /// Parquet Compression Options options are  "snappy", "gzip", "lz4", "zstd", or "uncompressed"
    #[arg(short, long, default_value = "snappy")]
    compression: String,

    /// Batch size for streaming.
    #[arg(short, long, default_value = "10000")]
    batch_size: usize,
}

#[derive(Args)]
struct SsimCsvOptions {
    /// Path of the SSIM File
    #[arg(short, long, required = true)]
    ssim_path: String,

    /// Output path / Directory + filename
    #[arg(short, long, required = true)]
    output_path: String,

    /// Batch size for streaming.
    #[arg(short, long, default_value = "10000")]
    batch_size: usize,
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Parquet(options) => {
            ssim_to_parquets(
                &options.ssim_path,
                Some(options.output_path.as_str()),
                Some(options.compression.as_str()),
                Some(options.batch_size),
            )
            .expect("Failed to parse SSIM File to Parquet's.");
        }

        Commands::Csv(options) => {
            ssim_to_csv(
                &options.ssim_path,
                &options.output_path,
                Some(options.batch_size),
            )
            .expect("Failed to parse SSIM File to CSV.");
        }
    }
}
