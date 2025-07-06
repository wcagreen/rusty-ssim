mod records {
    pub mod carrier_record;
    pub mod flight_leg_records;
    pub mod flight_segment;
    pub mod segment_records;
}
mod parser;


mod generators {
    pub mod ssim_dataframe;
}

use generators::ssim_dataframe::convert_to_dataframes;

use parser::{parse_carrier_record, parse_flight_record_legs, parse_segment_record};


// use serde::Serialize;
// use serde_json::Value;
// use rayon::join;
// use pyo3::prelude::*;
// use pyo3::types::{PyDict, PyList};


use std::fs::read_to_string;




fn read_all_ssim(file_path: &str) -> String {
    let ssim_file: String = read_to_string(file_path).expect("Failed to read in file.");
    return ssim_file;
}


fn ssim_iterator(ssim: String) -> polars::prelude::PolarsResult<()> {
    let mut record_type_2 =  Vec::new();
    let mut record_type_3s =  Vec::new();
    let mut record_type_4s =  Vec::new();
    for line in ssim.lines() {
        match line.chars().nth(0) {

            Some('2') => {
                if let Some(r) = parse_carrier_record(line) {
                    record_type_2.push(r);
                }
            }

            Some('3') => {
                if let Some(r) = parse_flight_record_legs(line) {
                    record_type_3s.push(r);
                }

            }

            Some('4') => {
                if let Some(r) = parse_segment_record(line) {
                    record_type_4s.push(r);

                }
            }

            _ => continue,

        }

    }

    let (carrier_df, flight_df, segment_df) = convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)?;

    println!("{:?}", carrier_df.head(Some(3)));
    println!("{:?}", flight_df.head(Some(3)));
    println!("{:?}", segment_df.head(Some(3)));

    Ok(())


}


fn main() {

    let file_path = "test_files/multi_ssim.dat";
    let ssim = read_all_ssim(file_path);

    if let Err(e) = ssim_iterator(ssim) {
        eprintln!("Error: {:?}", e);
    }


}

