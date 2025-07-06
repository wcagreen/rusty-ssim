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
    pub mod ssim_parser_iterator;
    pub mod ssim_readers;
}

use utils::ssim_parser_iterator::ssim_iterator;
use utils::ssim_readers::read_all_ssim;

mod parser;

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
    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");

    println!("{:?}", carrier_df.head(Some(3)));
    println!("{:?}", flight_df.head(Some(3)));
    println!("{:?}", segment_df.head(Some(3)));
}
