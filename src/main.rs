mod records {
    pub mod carrier_record;
    pub mod flight_leg_records;
    pub mod flight_segment;
    pub mod segment_records;
}
mod parser;

use parser::{parse_carrier_record, parse_flight_record_legs, parse_segment_record};

// use polars::prelude::*;
// use pyo3::prelude::*;
// use pyo3::types::{PyDict, PyList};


use std::fs::read_to_string;


// fn parse_ssim_to_dataframe()

fn read_all_ssim(file_path: &str) -> String {
    let ssim_file: String = read_to_string(file_path).expect("Failed to read in file.");
    return ssim_file;
}

fn ssim_iterator(ssim: String) {
    let mut record_type_2 =  Vec::new();
    let mut record_type_3s =  Vec::new();
    let mut record_type_4s =  Vec::new();
    for line in ssim.lines() {
        let record_type = line.chars().nth(0);

        if record_type == Some('2') {

            let parsed_record_type_2 =  parse_carrier_record(line);
            // println!("{:?}", parsed_record_type_2);
            record_type_2.push(parsed_record_type_2);

        } else if record_type == Some('3') {

            let parsed_record_type_3 =  parse_flight_record_legs(line);
            // println!("{:?}", parsed_record_type_3);
            record_type_3s.push(parsed_record_type_3);

        } else if record_type == Some('4') {

            let parsed_record_type_4 =  parse_segment_record(line);
            record_type_4s.push(parsed_record_type_4);
            // println!("{:?}", parsed_record_type_4);
        } 

    }
    println!("{:?}", record_type_3s);
}


fn main() {

    let file_path = "test_files/multi_ssim.dat";
    let ssim = read_all_ssim(file_path);
    ssim_iterator(ssim);


}

