pub use rusty_ssim_core::{ssim_to_dataframe, to_csv, to_parquet};

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
    /// Convert SSIM to JSON.
    Json(SsimJsonOptions),
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

    /// Streaming indicator.
    #[arg(short, long, default_value = "false")]
    streaming: bool,

    /// Batch size for streaming.
    #[arg(short, long, default_value = "10000")]
    batch_size: usize,

}

#[derive(Args)]
struct SsimJsonOptions {
    /// Path of the SSIM File
    #[arg(short, long, required = true)]
    ssim_path: String,

    /// FileName / Output path + filename
    #[arg(short, long, required = true)]
    output_path: String,

}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Df(options) => {
            let mut ssim_dataframe =
                ssim_to_dataframe(&options.ssim_path, Option::from(options.streaming), Option::from(options.batch_size)).expect("Failed to read SSIM records.");
            if options.file_type == "csv" {
                to_csv(&mut ssim_dataframe, &options.output_path)
                    .expect("Unable to write out csv file.");
            } else if options.file_type == "parquet" {
                to_parquet(
                    &mut ssim_dataframe,
                    &options.output_path,
                    &options.compression,
                )
                .expect("Unable to write out parquet file.");
            } else {
                panic!("Unsupported file type '{}'", options.file_type);
            }
        }

        Commands::Json(_options) => {
            println!("{}", "To Be Added. :/")
        }
    }
}
