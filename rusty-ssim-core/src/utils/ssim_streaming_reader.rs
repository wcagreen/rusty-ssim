use std::fs::File;
use std::io::{BufRead, BufReader};
use polars::prelude::*;
use crate::utils::ssim_parser::{parse_carrier_record, parse_flight_record_legs, parse_segment_record};
use crate::generators::ssim_dataframe::convert_to_dataframes;

const BATCH_SIZE: usize = 10_000;

pub struct StreamingSsimReader {
    reader: BufReader<File>,
    batch_size: usize,
}

impl StreamingSsimReader {
    pub fn new(file_path: &str, batch_size: Option<usize>) -> std::io::Result<Self> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        
        Ok(StreamingSsimReader {
            reader,
            batch_size: batch_size.unwrap_or(BATCH_SIZE),
        })
    }

    pub fn process_to_dataframes(&mut self) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
        let mut final_carrier_df = DataFrame::empty();
        let mut final_flight_df = DataFrame::empty();
        let mut final_segment_df = DataFrame::empty();

        let mut carrier_batch = Vec::with_capacity(self.batch_size);
        let mut flight_batch = Vec::with_capacity(self.batch_size);
        let mut segment_batch = Vec::with_capacity(self.batch_size);
        
        let mut line = String::new();
        
        loop {
            line.clear();
            match self.reader.read_line(&mut line) {
                Ok(0) => break, // EOF
                Ok(_) => {
                    match line.chars().nth(0) {
                        Some('2') => {
                            if let Some(record) = parse_carrier_record(&line) {
                                carrier_batch.push(record);
                            }
                        }
                        Some('3') => {
                            if let Some(record) = parse_flight_record_legs(&line) {
                                flight_batch.push(record);
                            }
                        }
                        Some('4') => {
                            if let Some(record) = parse_segment_record(&line) {
                                segment_batch.push(record);
                            }
                        }
                        _ => continue,
                    }


                    if carrier_batch.len() + flight_batch.len() + segment_batch.len() >= self.batch_size {
                        let (carrier_df, flight_df, segment_df) = 
                            convert_to_dataframes(carrier_batch, flight_batch, segment_batch)?;
                        

                        final_carrier_df = concatenate_dataframes(final_carrier_df, carrier_df)?;
                        final_flight_df = concatenate_dataframes(final_flight_df, flight_df)?;
                        final_segment_df = concatenate_dataframes(final_segment_df, segment_df)?;


                        carrier_batch = Vec::with_capacity(self.batch_size);
                        flight_batch = Vec::with_capacity(self.batch_size);
                        segment_batch = Vec::with_capacity(self.batch_size);
                    }
                }
                Err(e) => return Err(PolarsError::IO { error: Arc::from(e), msg: None}),
            }
        }


        if !carrier_batch.is_empty() || !flight_batch.is_empty() || !segment_batch.is_empty() {
            let (carrier_df, flight_df, segment_df) = 
                convert_to_dataframes(carrier_batch, flight_batch, segment_batch)?;
            
            final_carrier_df = concatenate_dataframes(final_carrier_df, carrier_df)?;
            final_flight_df = concatenate_dataframes(final_flight_df, flight_df)?;
            final_segment_df = concatenate_dataframes(final_segment_df, segment_df)?;
        }

        Ok((final_carrier_df, final_flight_df, final_segment_df))
    }
}

fn concatenate_dataframes(mut existing: DataFrame, new: DataFrame) -> PolarsResult<DataFrame> {
    if existing.is_empty() {
        Ok(new)
    } else {
        existing.vstack_mut(&new)?;
        Ok(existing)
    }
}


pub fn ssim_to_dataframes_streaming(file_path: &str) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    let mut reader = StreamingSsimReader::new(file_path, None)
        .map_err(|e| PolarsError::IO { error: Arc::from(e), msg: None })?;
    
    reader.process_to_dataframes()
}