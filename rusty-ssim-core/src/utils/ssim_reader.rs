//! Unified SSIM reader with pluggable batch processors.
//!
//! This module provides a single `SsimReader` that handles all file reading logic,
//! with different `BatchProcessor` implementations for various output strategies:
//! - In-memory combined DataFrame
//! - In-memory split DataFrames (carriers, flights, segments)
//! - Streaming CSV output
//! - Per-carrier Parquet output

use crate::converters::ssim_polars::combine_all_dataframes;
use crate::generators::ssim_dataframe::convert_to_dataframes;
use crate::records::carrier_record::CarrierRecord;
use crate::records::flight_leg_records::FlightLegRecord;
use crate::records::segment_records::SegmentRecords;
use crate::utils::ssim_exporters::to_parquet;
use crate::utils::ssim_parser::{parse_carrier_record, parse_flight_record_legs, parse_segment_record};
use polars::prelude::*;
use rayon::prelude::*;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_BATCH_SIZE: usize = 10_000;
const DEFAULT_BUFFER_SIZE: usize = 8 * 1024; // 8 KB

// ============================================================================
// BatchProcessor Trait
// ============================================================================

/// Trait for processing SSIM batches in different ways.
///
/// Implementors define how batches are handled:
/// - Accumulated in memory
/// - Written to CSV incrementally
/// - Written to Parquet per carrier
pub trait BatchProcessor {
    /// Process a batch of flight and segment records.
    ///
    /// Called when batch size is reached or carrier changes.
    fn process_batch(
        &mut self,
        flight_batch: Vec<FlightLegRecord>,
        segment_batch: Vec<SegmentRecords>,
        carrier: Option<&CarrierRecord>,
    ) -> PolarsResult<()>;

    /// Called when a carrier section ends (record type 5).
    ///
    /// Useful for Parquet output to write per-carrier files.
    fn on_carrier_complete(&mut self, _carrier: Option<&CarrierRecord>) -> PolarsResult<()> {
        Ok(()) // Default: no-op
    }

    /// Called at end of file to finalize processing.
    fn finalize(&mut self) -> PolarsResult<()>;
}

// ============================================================================
// Unified SSIM Reader
// ============================================================================

/// Unified streaming SSIM reader that delegates batch processing to a `BatchProcessor`.
pub struct SsimReader {
    reader: BufReader<File>,
    batch_size: usize,
    line_buffer: String,
    peeked_line: Option<String>,
    persistent_carriers: Option<CarrierRecord>,
}

impl SsimReader {
    pub fn new(file_path: &str, batch_size: Option<usize>, buffer_size: Option<usize>) -> std::io::Result<Self> {
        let file = File::open(file_path)?;
        let reader = BufReader::with_capacity(buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE), file);

        Ok(SsimReader {
            reader,
            batch_size: batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            line_buffer: String::new(),
            peeked_line: None,
            persistent_carriers: None,
        })
    }

    fn peek_next_line(&mut self) -> std::io::Result<Option<&str>> {
        if self.peeked_line.is_none() {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => return Ok(None),
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
            Ok(0) => Ok(None),
            Ok(_) => {
                if self.line_buffer.ends_with('\n') {
                    self.line_buffer.pop();
                    if self.line_buffer.ends_with('\r') {
                        self.line_buffer.pop();
                    }
                }
                let line = std::mem::take(&mut self.line_buffer);
                Ok(Some(line))
            }
            Err(e) => Err(e),
        }
    }

    fn should_continue_batch(&mut self, current_batch_size: usize, last_record_type: Option<char>) -> std::io::Result<bool> {
        if current_batch_size < self.batch_size {
            return Ok(true);
        }

        if last_record_type == Some('3') || last_record_type == Some('4') {
            loop {
                match self.peek_next_line()? {
                    Some(line) => match line.chars().nth(0) {
                        Some('4') if last_record_type == Some('3') => return Ok(true),
                        Some('4') if last_record_type == Some('4') => return Ok(true),
                        Some('3') | Some('2') | Some('1') | Some('5') => return Ok(false),
                        _ => {
                            self.consume_peeked_line();
                            continue;
                        }
                    },
                    None => return Ok(false),
                }
            }
        }

        Ok(false)
    }

    /// Main processing loop - works with any BatchProcessor
    /// 
    /// Uses parallel parsing with rayon for flight and segment records
    /// while maintaining sequential carrier context handling.
    pub fn process<P: BatchProcessor>(&mut self, processor: &mut P) -> PolarsResult<()> {
        // Collect raw lines for parallel parsing
        let mut flight_lines: Vec<String> = Vec::new();
        let mut segment_lines: Vec<String> = Vec::new();
        let mut last_record_type: Option<char> = None;

        loop {
            match self.read_next_line() {
                Ok(Some(line)) => {
                    let record_type = line.chars().next();

                    match record_type {
                        Some('1') | Some('0') => continue,
                        Some('2') => {
                            if let Some(record) = parse_carrier_record(&line) {
                                self.persistent_carriers = Some(record);
                                last_record_type = Some('2');
                            }
                        }
                        Some('3') => {
                            if self.persistent_carriers.is_some() {
                                flight_lines.push(line);
                                last_record_type = Some('3');
                            }
                        }
                        Some('4') => {
                            if self.persistent_carriers.is_some() {
                                segment_lines.push(line);
                                last_record_type = Some('4');
                            }
                        }
                        Some('5') => {
                            if !flight_lines.is_empty() || !segment_lines.is_empty() {
                                let (flight_batch, segment_batch) = self.parse_lines_parallel(
                                    std::mem::take(&mut flight_lines),
                                    std::mem::take(&mut segment_lines),
                                );
                                processor.process_batch(
                                    flight_batch,
                                    segment_batch,
                                    self.persistent_carriers.as_ref(),
                                )?;
                            }
                            // Notify processor that carrier section is complete
                            processor.on_carrier_complete(self.persistent_carriers.as_ref())?;
                            self.persistent_carriers = None;
                            last_record_type = Some('5');
                            continue;
                        }
                        _ => continue,
                    }

                    let current_batch_size = flight_lines.len() + segment_lines.len();
                    match self.should_continue_batch(current_batch_size, last_record_type) {
                        Ok(should_continue) => {
                            if !should_continue {
                                let (flight_batch, segment_batch) = self.parse_lines_parallel(
                                    std::mem::take(&mut flight_lines),
                                    std::mem::take(&mut segment_lines),
                                );
                                processor.process_batch(
                                    flight_batch,
                                    segment_batch,
                                    self.persistent_carriers.as_ref(),
                                )?;
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
                Ok(None) => break,
                Err(e) => {
                    return Err(PolarsError::IO {
                        error: Arc::from(e),
                        msg: None,
                    });
                }
            }
        }

        if !flight_lines.is_empty() || !segment_lines.is_empty() {
            let (flight_batch, segment_batch) = self.parse_lines_parallel(
                std::mem::take(&mut flight_lines),
                std::mem::take(&mut segment_lines),
            );
            processor.process_batch(
                flight_batch,
                segment_batch,
                self.persistent_carriers.as_ref(),
            )?;
        }

        processor.finalize()?;
        Ok(())
    }

    /// Parse flight and segment lines in parallel using rayon.
    /// 
    /// This is the hot path - parsing string slices into records is CPU-bound
    /// and benefits significantly from parallelization.
    fn parse_lines_parallel(
        &self,
        flight_lines: Vec<String>,
        segment_lines: Vec<String>,
    ) -> (Vec<FlightLegRecord>, Vec<SegmentRecords>) {
        let carrier = self.persistent_carriers.as_ref();
        
        // Parse flights and segments in parallel
        let (flights, segments) = rayon::join(
            || {
                if let Some(c) = carrier {
                    flight_lines
                        .par_iter()
                        .filter_map(|line| parse_flight_record_legs(line, c))
                        .collect()
                } else {
                    Vec::new()
                }
            },
            || {
                if let Some(c) = carrier {
                    segment_lines
                        .par_iter()
                        .filter_map(|line| parse_segment_record(line, c))
                        .collect()
                } else {
                    Vec::new()
                }
            },
        );

        (flights, segments)
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

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

// ============================================================================
// Processor: Combined DataFrame (in-memory)
// ============================================================================

/// Processor that accumulates all data into a single combined DataFrame.
pub struct CombinedDataFrameProcessor {
    result: DataFrame,
}

impl CombinedDataFrameProcessor {
    pub fn new() -> Self {
        Self {
            result: DataFrame::empty(),
        }
    }

    pub fn into_result(self) -> DataFrame {
        self.result
    }
}

impl Default for CombinedDataFrameProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchProcessor for CombinedDataFrameProcessor {
    fn process_batch(
        &mut self,
        flight_batch: Vec<FlightLegRecord>,
        segment_batch: Vec<SegmentRecords>,
        carrier: Option<&CarrierRecord>,
    ) -> PolarsResult<()> {
        let (carrier_df, flight_df, segment_df) = convert_to_dataframes(
            carrier.cloned(),
            flight_batch,
            segment_batch,
        )?;

        let batch_df = combine_all_dataframes(carrier_df, flight_df, segment_df)?;
        self.result = concatenate_dataframes(std::mem::take(&mut self.result), batch_df)?;
        Ok(())
    }

    fn finalize(&mut self) -> PolarsResult<()> {
        Ok(())
    }
}

// ============================================================================
// Processor: Split DataFrames (in-memory)
// ============================================================================

/// Processor that accumulates into three separate DataFrames.
pub struct SplitDataFrameProcessor {
    carriers: DataFrame,
    flights: DataFrame,
    segments: DataFrame,
}

impl SplitDataFrameProcessor {
    pub fn new() -> Self {
        Self {
            carriers: DataFrame::empty(),
            flights: DataFrame::empty(),
            segments: DataFrame::empty(),
        }
    }

    pub fn into_result(mut self) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
        // Deduplicate carriers
        self.carriers = self.carriers.unique_stable(None, UniqueKeepStrategy::First, None)?;
        Ok((self.carriers, self.flights, self.segments))
    }
}

impl Default for SplitDataFrameProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchProcessor for SplitDataFrameProcessor {
    fn process_batch(
        &mut self,
        flight_batch: Vec<FlightLegRecord>,
        segment_batch: Vec<SegmentRecords>,
        carrier: Option<&CarrierRecord>,
    ) -> PolarsResult<()> {
        let (carrier_df, flight_df, segment_df) = convert_to_dataframes(
            carrier.cloned(),
            flight_batch,
            segment_batch,
        )?;

        self.carriers = concatenate_dataframes(std::mem::take(&mut self.carriers), carrier_df)?;
        self.flights = concatenate_dataframes(std::mem::take(&mut self.flights), flight_df)?;
        self.segments = concatenate_dataframes(std::mem::take(&mut self.segments), segment_df)?;
        Ok(())
    }

    fn finalize(&mut self) -> PolarsResult<()> {
        Ok(())
    }
}

// ============================================================================
// Processor: CSV Writer (streaming append)
// ============================================================================

/// Processor that writes batches to CSV incrementally.
pub struct CsvWriterProcessor {
    writer: csv::Writer<File>,
    headers_written: bool,
}

impl CsvWriterProcessor {
    pub fn new(output_path: &str) -> PolarsResult<Self> {
        Self::ensure_directory_exists(output_path)?;

        let file_exists = Path::new(output_path).exists();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)
            .map_err(|e| PolarsError::IO {
                error: Arc::from(e),
                msg: Some(format!("Unable to open file {}", output_path).into()),
            })?;

        let writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(file);

        Ok(Self {
            writer,
            headers_written: file_exists,
        })
    }

    fn ensure_directory_exists(file_path: &str) -> PolarsResult<()> {
        let path = Path::new(file_path);
        if let Some(parent) = path.parent() && !parent.exists() {
            create_dir_all(parent).map_err(|e| PolarsError::IO {
                error: Arc::from(e),
                msg: Some("Failed to create directory".into()),
            })?;
        }
        Ok(())
    }

    fn write_dataframe(&mut self, mut df: DataFrame) -> PolarsResult<()> {
        // Write headers if needed
        if !self.headers_written {
            let headers: Vec<String> = df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();

            self.writer
                .write_record(&headers)
                .map_err(|e| PolarsError::IO {
                    error: Arc::from(std::io::Error::new(std::io::ErrorKind::Other, e)),
                    msg: Some("Failed to write CSV headers".into()),
                })?;

            self.headers_written = true;
        }

        // Convert DataFrame to CSV
        let mut csv_buffer = Vec::new();
        CsvWriter::new(&mut csv_buffer)
            .include_header(false)
            .finish(&mut df)?;

        let csv_string = String::from_utf8(csv_buffer).map_err(|e| {
            PolarsError::ComputeError(format!("UTF-8 conversion error: {}", e).into())
        })?;

        for line in csv_string.lines() {
            if !line.is_empty() {
                self.writer
                    .write_record(line.split(','))
                    .map_err(|e| PolarsError::IO {
                        error: Arc::from(std::io::Error::new(std::io::ErrorKind::Other, e)),
                        msg: Some("Failed to write CSV record".into()),
                    })?;
            }
        }

        self.writer.flush().map_err(|e| PolarsError::IO {
            error: Arc::from(e),
            msg: None,
        })?;

        Ok(())
    }
}

impl BatchProcessor for CsvWriterProcessor {
    fn process_batch(
        &mut self,
        flight_batch: Vec<FlightLegRecord>,
        segment_batch: Vec<SegmentRecords>,
        carrier: Option<&CarrierRecord>,
    ) -> PolarsResult<()> {
        let (carrier_df, flight_df, segment_df) = convert_to_dataframes(
            carrier.cloned(),
            flight_batch,
            segment_batch,
        )?;

        let batch_df = combine_all_dataframes(carrier_df, flight_df, segment_df)?;
        self.write_dataframe(batch_df)
    }

    fn finalize(&mut self) -> PolarsResult<()> {
        Ok(())
    }
}

// ============================================================================
// Processor: Parquet Writer (per-carrier files)
// ============================================================================

/// Processor that accumulates per carrier and writes Parquet files.
pub struct ParquetWriterProcessor {
    output_path: String,
    compression: String,
    accumulated_df: DataFrame,
    current_carrier: Option<CarrierRecord>,
}

impl ParquetWriterProcessor {
    pub fn new(output_path: &str, compression: Option<&str>) -> PolarsResult<Self> {
        let path = Path::new(output_path);

        if path.extension().is_some() {
            return Err(PolarsError::IO {
                error: Arc::from(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Output path should be a directory, not a file.",
                )),
                msg: None,
            });
        }

        if !path.exists() {
            create_dir_all(path).map_err(|e| PolarsError::IO {
                error: Arc::from(e),
                msg: Some(format!("Failed to create directory: {}", output_path).into()),
            })?;
        }

        Ok(Self {
            output_path: output_path.to_string(),
            compression: compression.unwrap_or("uncompressed").to_string(),
            accumulated_df: DataFrame::empty(),
            current_carrier: None,
        })
    }

    fn build_filename(&self, carrier: &CarrierRecord) -> String {
        let airline = carrier.airline_designator.trim();
        let control = carrier.control_duplicate_indicator.trim();

        let carrier_name = if !airline.is_empty() && !control.is_empty() {
            format!("{}_{}", airline, control)
        } else {
            format!("{}_", airline)
        };

        match self.compression.as_str() {
            "uncompressed" => format!("ssim_{}.parquet", carrier_name),
            "gzip" => format!("ssim_{}.parquet.gz", carrier_name),
            comp @ ("snappy" | "lz4" | "zstd" | "brotli" | "lzo") => {
                format!("ssim_{}.{}.parquet", carrier_name, comp)
            }
            _ => format!("ssim_{}.parquet", carrier_name),
        }
    }

    fn write_accumulated(&mut self) -> PolarsResult<()> {
        if self.accumulated_df.is_empty() {
            return Ok(());
        }

        let carrier = self.current_carrier.as_ref()
            .ok_or_else(|| PolarsError::ComputeError("No carrier for parquet file".into()))?;

        let filename = self.build_filename(carrier);
        let file_path: PathBuf = Path::new(&self.output_path).join(filename);

        to_parquet(
            &mut self.accumulated_df,
            file_path.to_str().expect("Invalid file path"),
            &self.compression,
        )?;

        self.accumulated_df = DataFrame::empty();
        Ok(())
    }
}

impl BatchProcessor for ParquetWriterProcessor {
    fn process_batch(
        &mut self,
        flight_batch: Vec<FlightLegRecord>,
        segment_batch: Vec<SegmentRecords>,
        carrier: Option<&CarrierRecord>,
    ) -> PolarsResult<()> {
        // Store carrier for filename generation
        if let Some(c) = carrier {
            self.current_carrier = Some(c.clone());
        }

        let (carrier_df, flight_df, segment_df) = convert_to_dataframes(
            carrier.cloned(),
            flight_batch,
            segment_batch,
        )?;

        let batch_df = combine_all_dataframes(carrier_df, flight_df, segment_df)?;
        self.accumulated_df = concatenate_dataframes(
            std::mem::take(&mut self.accumulated_df),
            batch_df,
        )?;
        Ok(())
    }

    fn on_carrier_complete(&mut self, _carrier: Option<&CarrierRecord>) -> PolarsResult<()> {
        self.write_accumulated()?;
        self.current_carrier = None;
        Ok(())
    }

    fn finalize(&mut self) -> PolarsResult<()> {
        self.write_accumulated()
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Parse SSIM file into a single combined DataFrame.
pub fn ssim_to_dataframe(
    file_path: &str,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
) -> PolarsResult<DataFrame> {
    let mut reader = SsimReader::new(file_path, batch_size, buffer_size)
        .map_err(|e| PolarsError::IO {
            error: Arc::from(e),
            msg: None,
        })?;

    let mut processor = CombinedDataFrameProcessor::new();
    reader.process(&mut processor)?;
    Ok(processor.into_result())
}

/// Parse SSIM file into three separate DataFrames.
pub fn ssim_to_dataframes(
    file_path: &str,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    let mut reader = SsimReader::new(file_path, batch_size, buffer_size)
        .map_err(|e| PolarsError::IO {
            error: Arc::from(e),
            msg: None,
        })?;

    let mut processor = SplitDataFrameProcessor::new();
    reader.process(&mut processor)?;
    processor.into_result()
}

/// Parse SSIM file and write to CSV (streaming).
pub fn ssim_to_csv(
    file_path: &str,
    output_path: &str,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
) -> PolarsResult<()> {
    let mut reader = SsimReader::new(file_path, batch_size, buffer_size)
        .map_err(|e| PolarsError::IO {
            error: Arc::from(e),
            msg: None,
        })?;

    let mut processor = CsvWriterProcessor::new(output_path)?;
    reader.process(&mut processor)
}

/// Parse SSIM file and write to Parquet files (one per carrier).
pub fn ssim_to_parquets(
    file_path: &str,
    output_path: Option<&str>,
    compression: Option<&str>,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
) -> PolarsResult<()> {
    let mut reader = SsimReader::new(file_path, batch_size, buffer_size)
        .map_err(|e| PolarsError::IO {
            error: Arc::from(e),
            msg: None,
        })?;

    let mut processor = ParquetWriterProcessor::new(
        output_path.unwrap_or("."),
        compression,
    )?;
    reader.process(&mut processor)
}