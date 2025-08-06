pub mod records {
    pub mod carrier_record;
    pub mod flight_leg_records;
    pub mod flight_segment;
    pub mod segment_records;
}

mod generators {
    pub mod ssim_dataframe;
}

mod converters {
    pub mod ssim_polars;
}

pub use crate::utils::ssim_exporters::to_parquet;
pub use crate::utils::ssim_streaming::{ssim_to_dataframe_streaming, ssim_to_dataframes_streaming};
pub use crate::utils::ssim_streaming_writer::{ssim_to_csv, ssim_to_parquets};
pub use converters::ssim_polars::ssim_to_dataframe;
pub use converters::ssim_polars::ssim_to_dataframes;

pub mod utils {
    pub mod ssim_exporters;
    pub mod ssim_parser;
    pub mod ssim_streaming;
    pub mod ssim_streaming_writer;
}
