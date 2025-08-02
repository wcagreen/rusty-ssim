use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::path::Path;
use polars::prelude::*;
use crate::utils::ssim_parser::{parse_carrier_record, parse_flight_record_legs, parse_segment_record};
use crate::generators::ssim_dataframe::convert_to_dataframes;
use crate::utils::ssim_exporters::to_parquet;
use crate::converters::ssim_polars::{combine_carrier_and_flights, combine_flights_and_segments};

const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Enhanced streaming SSIM reader that writes incrementally to avoid memory issues
pub struct EnhancedStreamingSsimWriter {
    reader: BufReader<File>,
    batch_size: usize,
    line_buffer: String,
    peeked_line: Option<String>,
    persistent_carriers: Vec<crate::records::carrier_record::CarrierRecord>,
    csv_writer: Option<CsvWriterState>,
    parquet_file_counter: usize,
}

struct CsvWriterState {
    writer: csv::Writer<File>,
    headers_written: bool,
}

impl EnhancedStreamingSsimWriter {
    pub fn new(file_path: &str, batch_size: Option<usize>) -> std::io::Result<Self> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        Ok(EnhancedStreamingSsimWriter {
            reader,
            batch_size: batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            line_buffer: String::new(),
            peeked_line: None,
            persistent_carriers: Vec::new(),
            csv_writer: None,
            parquet_file_counter: 0,
        })
    }

    fn peek_next_line(&mut self) -> std::io::Result<Option<&str>> {
        if self.peeked_line.is_none() {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => return Ok(None), // EOF
                Ok(_) => {
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

    fn should_continue_batch(&mut self, current_batch_size: usize, last_record_type: Option<char>) -> std::io::Result<bool> {
        if current_batch_size < self.batch_size {
            return Ok(true);
        }

        if last_record_type == Some('3') {
            loop {
                match self.peek_next_line()? {
                    Some(line) => {
                        match line.chars().nth(0) {
                            Some('4') => return Ok(true),
                            Some('3') | Some('2') | Some('1') | Some('5') => return Ok(false),
                            _ => {
                                self.consume_peeked_line();
                                continue;
                            }
                        }
                    }
                    None => return Ok(false),
                }
            }
        }

        if last_record_type == Some('4') {
            loop {
                match self.peek_next_line()? {
                    Some(line) => {
                        match line.chars().nth(0) {
                            Some('4') => return Ok(true),
                            Some('3') | Some('2') | Some('1') | Some('5') => return Ok(false),
                            _ => {
                                self.consume_peeked_line();
                                continue;
                            }
                        }
                    }
                    None => return Ok(false),
                }
            }
        }

        Ok(false)
    }

    /// Initialize CSV writer for incremental writing
    fn init_csv_writer(&mut self, output_path: &str) -> PolarsResult<()> {
    // Check if file already exists
        let file_exists = Path::new(output_path).exists();

        // Open in append mode if file exists, create otherwise
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)
            .expect(&format!("Unable to open file {}", output_path));

        let writer = csv::WriterBuilder::new()
            .has_headers(false) // We'll control headers ourselves
            .from_writer(file);

        self.csv_writer = Some(CsvWriterState {
            writer,
            headers_written: file_exists, // Skip headers if file already exists
        });

        Ok(())
    }

    /// Write batch to CSV incrementally
    fn write_batch_to_csv(&mut self, mut batch_df: DataFrame) -> PolarsResult<()> {
        if let Some(ref mut csv_state) = self.csv_writer {
            // Write headers if needed
            if !csv_state.headers_written {
                let headers: Vec<String> = batch_df.get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();

                csv_state.writer.write_record(&headers).expect("Failed to write CSV headers");

                csv_state.headers_written = true;
            }

            // Convert DataFrame to CSV lines (no header)
            let mut csv_buffer = Vec::new();
            CsvWriter::new(&mut csv_buffer)
                .include_header(false)
                .finish(&mut batch_df)?;

            let csv_string = String::from_utf8(csv_buffer)
                .map_err(|e| PolarsError::ComputeError(format!("UTF-8 conversion error: {}", e).into()))?;

            // Append rows
            for line in csv_string.lines() {
                if !line.is_empty() {
                    csv_state.writer.write_record(line.split(',')).expect("Failed to write CSV record");
                }
            }

            csv_state.writer.flush()
                .map_err(|e| PolarsError::IO { error: Arc::from(e), msg: None })?;
        }

        Ok(())
    }

    /// Write a complete carrier's data to a separate parquet file
    fn write_carrier_to_parquet(
        &mut self,
        mut carrier_df: DataFrame,
        output_path: &str,
        compression: Option<&str>,
    ) -> PolarsResult<()> {
        if carrier_df.is_empty() {
            return Ok(());
        }

        // TODO Fix the path outputs.
        // Generate filename for this carrier
        let carrier_name = self.get_carrier_filename();
        let path = Path::new(output_path);
        let parent = path.parent().unwrap_or(Path::new("."));
        let stem = path.file_stem().unwrap_or(std::ffi::OsStr::new("ssim"));
        let extension = path.extension().unwrap_or(std::ffi::OsStr::new("parquet"));

        let carrier_file_path = parent.join(format!(
            "{}_{}.{}",
            stem.to_string_lossy(),
            carrier_name,
            extension.to_string_lossy()
        ));

        let compression_str = compression.unwrap_or("snappy");
        to_parquet(&mut carrier_df, carrier_file_path.to_str().unwrap(), compression_str)?;

        self.parquet_file_counter += 1;
        println!("Written carrier {} to: {}", carrier_name, carrier_file_path.display());

        Ok(())
    }

    /// Extract carrier name for filename generation using persistent carriers
    fn get_carrier_filename(&self) -> String {
        if let Some(carrier) = self.persistent_carriers.first() {
            let airline_designator = carrier.airline_designator.trim();
            let control_duplicate_indicator = carrier.control_duplicate_indicator.trim();

            if !airline_designator.is_empty() && !control_duplicate_indicator.is_empty() {
                format!("{}_{}", airline_designator, control_duplicate_indicator)
            } else {
                format!("{}_", airline_designator)
            }
        } else {
            panic!("Unable to generate carrier filename");
        }
    }


    /// Process batch and combine into a single DataFrame
    fn process_batch_to_combined_dataframe(
        &self,
        flight_batch: &mut Vec<crate::records::flight_leg_records::FlightLegRecord>,
        segment_batch: &mut Vec<crate::records::segment_records::SegmentRecords>,
    ) -> PolarsResult<DataFrame> {
        let carrier_batch = self.persistent_carriers.clone();

        let (carrier_df, flight_df, segment_df) = convert_to_dataframes(
            carrier_batch,
            std::mem::take(flight_batch),
            std::mem::take(segment_batch),
        )?;

        // Combine carrier and flights
        let mut combined_df = combine_carrier_and_flights(carrier_df, flight_df)?;

        // Combine with segments
        combined_df = combine_flights_and_segments(combined_df, segment_df)?;

        Ok(combined_df)
    }

    /// Main streaming function for CSV output (incremental writing)
    pub fn stream_to_csv(&mut self, output_path: &str) -> PolarsResult<()> {
        self.init_csv_writer(output_path)?;

        let mut flight_batch = Vec::new();
        let mut segment_batch = Vec::new();
        let mut last_record_type: Option<char> = None;

        loop {
            match self.read_next_line() {
                Ok(Some(line)) => {
                    let record_type = line.chars().nth(0);

                    match record_type {
                        Some('1') => continue, // Skip header records
                        Some('0') => continue, // Skip zero records
                        Some('2') => {
                            if let Some(record) = parse_carrier_record(&line) {
                                self.persistent_carriers.push(record);
                                last_record_type = Some('2');
                            }
                        }
                        Some('3') => {
                            if let Some(record) = parse_flight_record_legs(&line, &self.persistent_carriers) {
                                flight_batch.push(record);
                                last_record_type = Some('3');
                            }
                        }
                        Some('4') => {
                            if let Some(record) = parse_segment_record(&line, &self.persistent_carriers) {
                                segment_batch.push(record);
                                last_record_type = Some('4');
                            }
                        }
                        Some('5') => {
                            // Process and write final batch for this carrier
                            if !flight_batch.is_empty() || !segment_batch.is_empty() {
                                let batch_df = self.process_batch_to_combined_dataframe(&mut flight_batch, &mut segment_batch)?;
                                self.write_batch_to_csv(batch_df)?;
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
                                let batch_df = self.process_batch_to_combined_dataframe(&mut flight_batch, &mut segment_batch)?;
                                self.write_batch_to_csv(batch_df)?;
                                flight_batch.clear();
                                segment_batch.clear();
                            }
                        }
                        Err(e) => return Err(PolarsError::IO { error: Arc::from(e), msg: None }),
                    }
                }
                Ok(None) => break, // EOF
                Err(e) => return Err(PolarsError::IO { error: Arc::from(e), msg: None }),
            }
        }

        // Process any remaining records
        if !flight_batch.is_empty() || !segment_batch.is_empty() {
            let batch_df = self.process_batch_to_combined_dataframe(&mut flight_batch, &mut segment_batch)?;
            self.write_batch_to_csv(batch_df)?;
        }

        Ok(())
    }

    /// Main streaming function for Parquet output (one file per carrier)
    pub fn stream_to_parquet(&mut self, output_path: &str, compression: Option<&str>) -> PolarsResult<()> {
        let mut carrier_combined_df = DataFrame::empty();
        let mut flight_batch = Vec::new();
        let mut segment_batch = Vec::new();
        let mut last_record_type: Option<char> = None;

        loop {
            match self.read_next_line() {
                Ok(Some(line)) => {
                    let record_type = line.chars().nth(0);

                    match record_type {
                        Some('1') => continue, // Skip header records
                        Some('0') => continue, // Skip zero records
                        Some('2') => {
                            if let Some(record) = parse_carrier_record(&line) {
                                self.persistent_carriers.push(record);
                                last_record_type = Some('2');
                            }
                        }
                        Some('3') => {
                            if let Some(record) = parse_flight_record_legs(&line, &self.persistent_carriers) {
                                flight_batch.push(record);
                                last_record_type = Some('3');
                            }
                        }
                        Some('4') => {
                            if let Some(record) = parse_segment_record(&line, &self.persistent_carriers) {
                                segment_batch.push(record);
                                last_record_type = Some('4');
                            }
                        }
                        Some('5') => {
                            // Process final batch for this carrier
                            if !flight_batch.is_empty() || !segment_batch.is_empty() {
                                let batch_df = self.process_batch_to_combined_dataframe(&mut flight_batch, &mut segment_batch)?;
                                carrier_combined_df = self.concatenate_dataframes(carrier_combined_df, batch_df)?;
                                flight_batch.clear();
                                segment_batch.clear();
                            }

                            // Write this carrier's complete data to a parquet file
                            if !carrier_combined_df.is_empty() {
                                self.write_carrier_to_parquet(carrier_combined_df, output_path, compression)?;
                                carrier_combined_df = DataFrame::empty();
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
                                let batch_df = self.process_batch_to_combined_dataframe(&mut flight_batch, &mut segment_batch)?;
                                carrier_combined_df = self.concatenate_dataframes(carrier_combined_df, batch_df)?;
                                flight_batch.clear();
                                segment_batch.clear();
                            }
                        }
                        Err(e) => return Err(PolarsError::IO { error: Arc::from(e), msg: None }),
                    }
                }
                Ok(None) => break, // EOF
                Err(e) => return Err(PolarsError::IO { error: Arc::from(e), msg: None }),
            }
        }

        // Process any remaining records and write final carrier file
        if !flight_batch.is_empty() || !segment_batch.is_empty() {
            let batch_df = self.process_batch_to_combined_dataframe(&mut flight_batch, &mut segment_batch)?;
            carrier_combined_df = self.concatenate_dataframes(carrier_combined_df, batch_df)?;
        }

        if !carrier_combined_df.is_empty() {
            self.write_carrier_to_parquet(carrier_combined_df, output_path, compression)?;
        }

        Ok(())
    }

    fn concatenate_dataframes(&self, mut existing: DataFrame, new: DataFrame) -> PolarsResult<DataFrame> {
        if existing.is_empty() {
            Ok(new)
        } else if new.is_empty() {
            Ok(existing)
        } else {
            existing.vstack_mut(&new)?;
            Ok(existing)
        }
    }
}

pub fn ssim_to_file(
    file_path: &str,
    output_path: &str,
    file_type: &str,
    compression: Option<&str>,
    batch_size: Option<usize>,
) -> PolarsResult<()> {
    let mut writer = EnhancedStreamingSsimWriter::new(file_path, batch_size)
        .map_err(|e| PolarsError::IO { error: Arc::from(e), msg: None })?;

    match file_type.to_lowercase().as_str() {
        "csv" => {
            writer.stream_to_csv(output_path)?;
            println!("Successfully streamed SSIM data to CSV: {}", output_path);
        }
        "parquet" => {
            writer.stream_to_parquet(output_path, compression)?;
            println!("Successfully streamed SSIM data to Parquet files at: {}", output_path);
        }
        _ => {
            return Err(PolarsError::ComputeError(
                format!("Unsupported file type: {}", file_type).into()
            ));
        }
    }

    Ok(())
}
