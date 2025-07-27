mod records {
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

use crate::utils::ssim_exporters::{to_csv, to_parquet};
use converters::ssim_polars::ssim_to_dataframe;
// use converters::ssim_polars::ssim_to_dataframes;

mod utils {
    pub mod ssim_exporters;
    pub mod ssim_parser;
    pub mod ssim_parser_iterator;
    pub mod ssim_readers;
}

// use serde::Serialize;
// use serde_json::Value;
// use rayon::join;
// use pyo3::prelude::*;
// use pyo3::types::{PyDict, PyList};

// TODO - implement a Python interface for this Rust code
// TODO - implement a way to convert segments to json and join them to flight legs based on the flight leg identifier

fn main() {
    let file_path = "test_files/multi_ssim.dat";

    // let (mut carrier_df, mut flight_df, mut segment_df) =
    //     ssim_to_dataframes(file_path).expect("Failed to read SSIM records.");

    let mut ssim_dataframe = ssim_to_dataframe(file_path).expect("Failed to read SSIM records.");

    to_csv(&mut ssim_dataframe, "ssim.csv").expect("Failed to write ssim.csv");

    to_parquet(&mut ssim_dataframe, "ssim.parquet", "snappy")
        .expect("Failed to write ssim.parquet");
}
