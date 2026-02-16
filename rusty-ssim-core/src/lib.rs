//! # rusty-ssim-core
//!
//! **This is an internal implementation crate for [`rustyssim`](https://crates.io/crates/rustyssim) and its [python version](https://pypi.org/project/rustyssim/).**
//!
//! **You should not depend on this crate directly.** Use
//! [`rustyssim`](https://crates.io/crates/rustyssim) instead.
//!
//! Internal APIs may change without notice across minor versions.

pub mod records {
    pub mod carrier_record;
    pub mod flight_leg_records;
    pub mod segment_records;
}

mod generators {
    pub mod ssim_dataframe;
}

mod converters {
    pub mod ssim_polars;
}

// Public API from unified reader
pub use crate::utils::ssim_exporters::to_parquet;
pub use crate::utils::ssim_reader::{
    // Export types for custom processors
    BatchProcessor,
    CombinedDataFrameProcessor,
    CsvWriterProcessor,
    ParquetWriterProcessor,
    SplitDataFrameProcessor,
    SsimReader,
    ssim_to_csv,
    ssim_to_dataframe,
    ssim_to_dataframes,
    ssim_to_parquets,
};

pub mod utils {
    pub mod ssim_exporters;
    pub mod ssim_parser;
    pub mod ssim_reader;
}
