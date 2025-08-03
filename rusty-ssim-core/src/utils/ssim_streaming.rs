use crate::converters::ssim_polars::{combine_carrier_and_flights, combine_flights_and_segments};
use crate::generators::ssim_dataframe::convert_to_dataframes;
use crate::utils::ssim_parser::{
    parse_carrier_record, parse_flight_record_legs, parse_segment_record,
};
use polars::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};

const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Unified streaming SSIM reader that can output to memory or file
pub struct StreamingSsimReader {
    reader: BufReader<File>,
    batch_size: usize,
    line_buffer: String,
    peeked_line: Option<String>,
    // Persistent carrier records until we hit record type 5
    persistent_carriers: Vec<crate::records::carrier_record::CarrierRecord>,
}

impl StreamingSsimReader {
    pub fn new(file_path: &str, batch_size: Option<usize>) -> std::io::Result<Self> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        Ok(StreamingSsimReader {
            reader,
            batch_size: batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            line_buffer: String::new(),
            peeked_line: None,
            persistent_carriers: Vec::new(),
        })
    }

    fn peek_next_line(&mut self) -> std::io::Result<Option<&str>> {
        if self.peeked_line.is_none() {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => return Ok(None), // EOF
                Ok(_) => {
                    // Remove trailing newline for consistent processing
                    if line.ends_with('\n') {
                        line.pop();
                        if line.ends_with('\r') {
                            line.pop();
                        }
                    }
                    self.peeked_line = Some(line);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(self.peeked_line.as_deref())
    }

    fn consume_peeked_line(&mut self) -> Option<String> {
        self.peeked_line.take()
    }

    fn read_next_line(&mut self) -> std::io::Result<Option<String>> {
        if let Some(line) = self.consume_peeked_line() {
            return Ok(Some(line));
        }

        self.line_buffer.clear();
        match self.reader.read_line(&mut self.line_buffer) {
            Ok(0) => Ok(None), // EOF
            Ok(_) => {
                // Remove trailing newline for consistent processing
                if self.line_buffer.ends_with('\n') {
                    self.line_buffer.pop();
                    if self.line_buffer.ends_with('\r') {
                        self.line_buffer.pop();
                    }
                }
                Ok(Some(self.line_buffer.clone()))
            }
            Err(e) => Err(e),
        }
    }

    fn should_continue_batch(
        &mut self,
        current_batch_size: usize,
        last_record_type: Option<char>,
    ) -> std::io::Result<bool> {
        // If we haven't reached batch size, continue
        if current_batch_size < self.batch_size {
            return Ok(true);
        }

        // If the last record was type 3, we need to check if the next records are type 4
        if last_record_type == Some('3') {
            loop {
                match self.peek_next_line()? {
                    Some(line) => {
                        match line.chars().nth(0) {
                            Some('4') => {
                                // The Next line is type 4, continue batch
                                return Ok(true);
                            }
                            Some('3') | Some('2') | Some('1') | Some('5') => {
                                // The Next line is not type 4, stop batch
                                return Ok(false);
                            }
                            _ => {
                                // Skip invalid lines and continue checking
                                self.consume_peeked_line();
                                continue;
                            }
                        }
                    }
                    None => {
                        // EOF, stop batch
                        return Ok(false);
                    }
                }
            }
        }

        // If the last record was type 4, we also need to check for more type 4's
        if last_record_type == Some('4') {
            loop {
                match self.peek_next_line()? {
                    Some(line) => {
                        match line.chars().nth(0) {
                            Some('4') => {
                                // Another type 4, continue batch
                                return Ok(true);
                            }
                            Some('3') | Some('2') | Some('1') | Some('5') => {
                                // Different type, stop batch
                                return Ok(false);
                            }
                            _ => {
                                // Skip invalid lines and continue checking
                                self.consume_peeked_line();
                                continue;
                            }
                        }
                    }
                    None => {
                        // EOF, stop batch
                        return Ok(false);
                    }
                }
            }
        }

        // For other record types, stop at batch size
        Ok(false)
    }

    /// Process an SSIM file and return a single combined DataFrame in memory
    pub fn process_to_combined_dataframe(&mut self) -> PolarsResult<DataFrame> {
        let mut final_combined_df = DataFrame::empty();

        let mut flight_batch = Vec::new();
        let mut segment_batch = Vec::new();
        let mut last_record_type: Option<char> = None;

        loop {
            match self.read_next_line() {
                Ok(Some(line)) => {
                    let record_type = line.chars().nth(0);

                    match record_type {
                        Some('1') => continue, // Skip header records
                        Some('0') => continue, // Skip Zeros Records
                        Some('2') => {
                            if let Some(record) = parse_carrier_record(&line) {
                                self.persistent_carriers.push(record);
                                last_record_type = Some('2');
                            }
                        }
                        Some('3') => {
                            if let Some(record) =
                                parse_flight_record_legs(&line, &self.persistent_carriers)
                            {
                                flight_batch.push(record);
                                last_record_type = Some('3');
                            }
                        }
                        Some('4') => {
                            if let Some(record) =
                                parse_segment_record(&line, &self.persistent_carriers)
                            {
                                segment_batch.push(record);
                                last_record_type = Some('4');
                            }
                        }
                        Some('5') => {
                            // Process the final batch with persistent carriers before clearing
                            if !flight_batch.is_empty() || !segment_batch.is_empty() {
                                let batch_df = self.process_batch_to_combined_dataframe_internal(
                                    &mut flight_batch,
                                    &mut segment_batch,
                                )?;
                                final_combined_df =
                                    concatenate_dataframes(final_combined_df, batch_df)?;
                                flight_batch.clear();
                                segment_batch.clear();
                            }

                            self.persistent_carriers.clear();
                            last_record_type = Some('5');
                            continue;
                        }
                        _ => continue,
                    }

                    let current_batch_size = flight_batch.len() + segment_batch.len();

                    match self.should_continue_batch(current_batch_size, last_record_type) {
                        Ok(should_continue) => {
                            if !should_continue {
                                let batch_df = self.process_batch_to_combined_dataframe_internal(
                                    &mut flight_batch,
                                    &mut segment_batch,
                                )?;
                                final_combined_df =
                                    concatenate_dataframes(final_combined_df, batch_df)?;

                                flight_batch.clear();
                                segment_batch.clear();
                            }
                        }
                        Err(e) => {
                            return Err(PolarsError::IO {
                                error: Arc::from(e),
                                msg: None,
                            });
                        }
                    }
                }
                Ok(None) => break, // EOF
                Err(e) => {
                    return Err(PolarsError::IO {
                        error: Arc::from(e),
                        msg: None,
                    });
                }
            }
        }

        // Process any remaining records
        if !flight_batch.is_empty() || !segment_batch.is_empty() {
            let batch_df = self.process_batch_to_combined_dataframe_internal(
                &mut flight_batch,
                &mut segment_batch,
            )?;
            final_combined_df = concatenate_dataframes(final_combined_df, batch_df)?;
        }

        Ok(final_combined_df)
    }

    /// Process SSIM file and return separate DataFrames in memory
    fn process_to_dataframes(&mut self) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
        let mut final_carrier_df = DataFrame::empty();
        let mut final_flight_df = DataFrame::empty();
        let mut final_segment_df = DataFrame::empty();

        let mut flight_batch = Vec::new();
        let mut segment_batch = Vec::new();
        let mut last_record_type: Option<char> = None;

        loop {
            match self.read_next_line() {
                Ok(Some(line)) => {
                    let record_type = line.chars().nth(0);

                    match record_type {
                        Some('1') => continue, // Skip header records
                        Some('0') => continue, // Skip Zero records
                        Some('2') => {
                            if let Some(record) = parse_carrier_record(&line) {
                                self.persistent_carriers.push(record);
                                last_record_type = Some('2');
                            }
                        }
                        Some('3') => {
                            if let Some(record) =
                                parse_flight_record_legs(&line, &self.persistent_carriers)
                            {
                                flight_batch.push(record);
                                last_record_type = Some('3');
                            }
                        }
                        Some('4') => {
                            if let Some(record) =
                                parse_segment_record(&line, &self.persistent_carriers)
                            {
                                segment_batch.push(record);
                                last_record_type = Some('4');
                            }
                        }
                        Some('5') => {
                            // Process the final batch with persistent carriers before clearing
                            if !flight_batch.is_empty() || !segment_batch.is_empty() {
                                let (carrier_df, flight_df, segment_df) = self
                                    .process_batch_with_persistent_carriers(
                                        &mut flight_batch,
                                        &mut segment_batch,
                                    )?;

                                final_carrier_df =
                                    concatenate_dataframes(final_carrier_df, carrier_df)?;
                                final_flight_df =
                                    concatenate_dataframes(final_flight_df, flight_df)?;
                                final_segment_df =
                                    concatenate_dataframes(final_segment_df, segment_df)?;

                                flight_batch.clear();
                                segment_batch.clear();
                            }

                            self.persistent_carriers.clear();
                            last_record_type = Some('5');
                            continue;
                        }
                        _ => continue,
                    }

                    let current_batch_size = flight_batch.len() + segment_batch.len();

                    match self.should_continue_batch(current_batch_size, last_record_type) {
                        Ok(should_continue) => {
                            if !should_continue {
                                let (carrier_df, flight_df, segment_df) = self
                                    .process_batch_with_persistent_carriers(
                                        &mut flight_batch,
                                        &mut segment_batch,
                                    )?;

                                final_carrier_df =
                                    concatenate_dataframes(final_carrier_df, carrier_df)?;
                                final_flight_df =
                                    concatenate_dataframes(final_flight_df, flight_df)?;
                                final_segment_df =
                                    concatenate_dataframes(final_segment_df, segment_df)?;

                                flight_batch.clear();
                                segment_batch.clear();
                            }
                        }
                        Err(e) => {
                            return Err(PolarsError::IO {
                                error: Arc::from(e),
                                msg: None,
                            });
                        }
                    }
                }
                Ok(None) => break, // EOF
                Err(e) => {
                    return Err(PolarsError::IO {
                        error: Arc::from(e),
                        msg: None,
                    });
                }
            }
        }

        // Process any remaining records
        if !flight_batch.is_empty() || !segment_batch.is_empty() {
            let (carrier_df, flight_df, segment_df) =
                self.process_batch_with_persistent_carriers(&mut flight_batch, &mut segment_batch)?;

            final_carrier_df = concatenate_dataframes(final_carrier_df, carrier_df)?;
            final_flight_df = concatenate_dataframes(final_flight_df, flight_df)?;
            final_segment_df = concatenate_dataframes(final_segment_df, segment_df)?;
        }

        final_carrier_df = final_carrier_df.unique_stable(None, UniqueKeepStrategy::First, None)?;

        Ok((final_carrier_df, final_flight_df, final_segment_df))
    }

    fn process_batch_with_persistent_carriers(
        &self,
        flight_batch: &mut Vec<crate::records::flight_leg_records::FlightLegRecord>,
        segment_batch: &mut Vec<crate::records::segment_records::SegmentRecords>,
    ) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
        let carrier_batch = self.persistent_carriers.clone();

        let (carrier_df, flight_df, segment_df) = convert_to_dataframes(
            carrier_batch,
            std::mem::take(flight_batch),
            std::mem::take(segment_batch),
        )?;

        Ok((carrier_df, flight_df, segment_df))
    }

    fn process_batch_to_combined_dataframe_internal(
        &self,
        flight_batch: &mut Vec<crate::records::flight_leg_records::FlightLegRecord>,
        segment_batch: &mut Vec<crate::records::segment_records::SegmentRecords>,
    ) -> PolarsResult<DataFrame> {
        let (carrier_df, flight_df, segment_df) =
            self.process_batch_with_persistent_carriers(flight_batch, segment_batch)?;

        // Combine carrier and flights
        let mut combined_df = combine_carrier_and_flights(carrier_df, flight_df)?;

        // Combine with segments
        combined_df = combine_flights_and_segments(combined_df, segment_df)?;

        Ok(combined_df)
    }
}

fn concatenate_dataframes(mut existing: DataFrame, new: DataFrame) -> PolarsResult<DataFrame> {
    if existing.is_empty() {
        Ok(new)
    } else if new.is_empty() {
        Ok(existing)
    } else {
        existing.vstack_mut(&new)?;
        Ok(existing)
    }
}

// Public API functions for backward compatibility

/// Parse SSIM file into three separate DataFrames using streaming
pub fn ssim_to_dataframes_streaming(
    file_path: &str,
    batch_size: Option<usize>,
) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    let mut reader =
        StreamingSsimReader::new(file_path, batch_size).map_err(|e| PolarsError::IO {
            error: Arc::from(e),
            msg: None,
        })?;

    reader.process_to_dataframes()
}

/// Parse SSIM file into a single combined DataFrame using streaming
pub fn ssim_to_dataframe_streaming(
    file_path: &str,
    batch_size: Option<usize>,
) -> PolarsResult<DataFrame> {
    let mut reader =
        StreamingSsimReader::new(file_path, batch_size).map_err(|e| PolarsError::IO {
            error: Arc::from(e),
            msg: None,
        })?;

    reader.process_to_combined_dataframe()
}
