mod records {
    pub mod carrier_record;
    pub mod flight_leg_records;
    pub mod flight_segment;
    pub mod segment_records;
}

mod generators {
    pub mod ssim_dataframe;
}

use generators::ssim_dataframe::convert_to_dataframes;
use std::collections::HashMap;

mod utils {
    pub mod ssim_exporters;
    pub mod ssim_parser;
    pub mod ssim_parser_iterator;
    pub mod ssim_readers;
}

use polars_core::utils::arrow::io::ipc::format::ipc::Struct;
use utils::ssim_exporters::to_csv;
use utils::ssim_parser_iterator::ssim_iterator;
use utils::ssim_readers::read_all_ssim;

use crate::utils::ssim_exporters::to_parquet;

use polars::lazy::dsl::*;
use polars::prelude::*;
use polars_core::prelude::*;

// use serde::Serialize;
// use serde_json::Value;
// use rayon::join;
// use pyo3::prelude::*;
// use pyo3::types::{PyDict, PyList};

// TODO - implement a Python interface for this Rust code
// TODO - implement a way to convert segments to json and join them to flight legs based on the flight leg identifier

fn ssim_to_dataframes(
    file_path: &str,
) -> polars::prelude::PolarsResult<(
    polars::prelude::DataFrame,
    polars::prelude::DataFrame,
    polars::prelude::DataFrame,
)> {
    let ssim = read_all_ssim(&file_path);
    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");
    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");
    Ok((carrier_df, flight_df, segment_df))
}

fn main() {
    let file_path = "test_files/multi_ssim.dat";
    let (carrier_df, flight_df, segment_df) =
        ssim_to_dataframes(file_path).expect("Failed to read SSIM records.");

    println!("{:?}", carrier_df);
    println!("{:?}", flight_df);
    println!("{:?}", segment_df);

    //
    // let test = build_segments(&segment_df);
    //
    // println!("{:?}", test);

    // to_csv(&mut carrier_df, "carrier.csv").expect("Failed to write carrier.csv");
    // to_csv(&mut flight_df, "flight.csv").expect("Failed to write flight.csv");
    // to_csv(&mut result, "segment.csv").expect("Failed to write segment.csv");

    // to_parquet(&mut carrier_df, "carrier.parquet", "snappy").expect("Failed to write carrier.csv");
    // to_parquet(&mut flight_df, "flight.parquet", "snappy").expect("Failed to write flight.csv");
    // to_parquet(&mut segment_df, "segment.parquet", "snappy").expect("Failed to write segment.csv");
}
