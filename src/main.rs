mod records {
    pub mod carrier_record;
    pub mod flight_leg_records;
    pub mod flight_segment;
    pub mod segment_records;
}
mod parser;

use parser::{parse_carrier_record, parse_flight_record_legs, parse_segment_record};

use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use records::{carrier_record, segment_records};
use serde_json::json;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::iter::Peekable;
// use std::path::Path;

use records::flight_segment::FlightSegment;
use  records::carrier_record::CarrierRecord;


use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SSIMRecordsJson {
    carrier: Option<CarrierRecord>,
    flights: Vec<FlightSegment>
}



fn main() {
    let read_buffer_size = None;
    let file_path = "test_files/multi_ssim.dat";
    let file = File::open(file_path).expect("Failed to open file");
    let reader = match read_buffer_size {
        Some(size) => BufReader::with_capacity(size, file),
        None => BufReader::new(file),
    };

    let mut lines: Peekable<_> = reader.lines().peekable();
    let mut records = Vec::new();
    let mut carrier = None;

    while let Some(Ok(line)) = lines.next() {

        if line.starts_with('2') {
            carrier = parse_carrier_record(&line);
        }
        
        if line.starts_with('3') {
            let flight_record = parse_flight_record_legs(&line);
            let mut segment_records = Vec::new();

            while let Some(Ok(peek_line)) = lines.peek() {
                if peek_line.starts_with('4') {
                    segment_records.push(parse_segment_record(peek_line));
                    lines.next(); 
                } else {
                    break; 
                }
            }

            if let Some(flight_leg) = flight_record {
                records.push(FlightSegment {
                    flight_leg,
                    segments: segment_records,
                });
            }
        
        }
    }


    let ssim_json = SSIMRecordsJson {
        carrier,
        flights: records, 
    };
    let json = serde_json::to_string_pretty(&ssim_json).unwrap();

    let output_file_path = "";
    let output_file = File::create(output_file_path).expect("Failed to create file");
    serde_json::to_writer_pretty(output_file, &ssim_json)
        .expect("Failed to write JSON to file");

}

