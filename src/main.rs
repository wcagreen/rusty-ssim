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

use converters::ssim_polars::ssim_to_dataframes;

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
