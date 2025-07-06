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

mod utils {
    pub mod ssim_exporters;
    pub mod ssim_parser_iterator;
    pub mod ssim_readers;
    pub mod ssim_parser;
}

use utils::ssim_exporters::to_csv;
use utils::ssim_parser_iterator::ssim_iterator;
use utils::ssim_readers::read_all_ssim;


// use serde::Serialize;
// use serde_json::Value;
// use rayon::join;
// use pyo3::prelude::*;
// use pyo3::types::{PyDict, PyList};

fn main() {
    let file_path = "test_files/multi_ssim.dat";
    let ssim = read_all_ssim(file_path);

    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");
    let (mut carrier_df, mut flight_df, mut segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");

    to_csv(&mut carrier_df, "carrier.csv").expect("Failed to write carrier.csv");
    to_csv(&mut flight_df, "flight.csv").expect("Failed to write flight.csv");
    to_csv(&mut segment_df, "segment.csv").expect("Failed to write segment.csv");
}
