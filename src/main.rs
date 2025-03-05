mod records {
    pub mod carrier_record;
    pub mod flight_leg_records;
    pub mod flight_records;
    pub mod segment_records;
}
mod parser;

use parser::{parse_flight_record_legs, parse_segment_record};

use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use records::segment_records;
use serde_json::json;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::iter::Peekable;
// use std::path::Path;

use records::flight_records::FlightSegment;


fn main() {
    let read_buffer_size = None;
    let file_path = "test_files/ssim_file.dat";
    let file = File::open(file_path).expect("Failed to open file");
    let reader = match read_buffer_size {
        Some(size) => BufReader::with_capacity(size, file),
        None => BufReader::new(file),
    };

    let mut lines: Peekable<_> = reader.lines().peekable();
    let mut records = Vec::new();

    while let Some(Ok(line)) = lines.next() {
        
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

    println!("{:?}", records);
}

