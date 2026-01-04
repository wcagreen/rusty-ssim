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
pub use crate::utils::ssim_reader::{
    ssim_to_csv, ssim_to_dataframe, ssim_to_dataframes, ssim_to_parquets,
    // Export types for custom processors
    BatchProcessor, SsimReader,
    CombinedDataFrameProcessor, SplitDataFrameProcessor,
    CsvWriterProcessor, ParquetWriterProcessor,
};
pub use crate::utils::ssim_exporters::to_parquet;

pub mod utils {
    pub mod ssim_exporters;
    pub mod ssim_parser;
    pub mod ssim_reader;
}
