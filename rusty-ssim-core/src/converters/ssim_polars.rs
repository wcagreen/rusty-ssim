use polars::error::PolarsResult;
use polars::prelude::{DataFrame, IntoLazy, JoinArgs, JoinType, IntoColumn, NamedFrom, Series, col, cols};
use std::fmt::Write;

/// Condenses segments DataFrame by grouping and serializing to JSON strings.
/// This reduces memory by converting List<Struct> to a single JSON string per flight.
fn condense_segments_to_json(segments: DataFrame) -> PolarsResult<DataFrame> {
    // Group segments by flight keys using Polars
    let grouped = segments
        .lazy()
        .group_by([col("flight_designator"), col("control_duplicate_indicator")])
        .agg([
            col("board_point_indicator"),
            col("off_point_indicator"),
            col("board_point"),
            col("off_point"),
            col("data_element_identifier"),
            col("data"),
        ])
        .collect()?;
    
    let height = grouped.height();
    let flight_designators = grouped.column("flight_designator")?.clone();
    let control_dups = grouped.column("control_duplicate_indicator")?.clone();
    
    // Get list columns - extract the ChunkedArrays directly for faster access
    let board_point_ind = grouped.column("board_point_indicator")?.list()?;
    let off_point_ind = grouped.column("off_point_indicator")?.list()?;
    let board_point = grouped.column("board_point")?.list()?;
    let off_point = grouped.column("off_point")?.list()?;
    let dei = grouped.column("data_element_identifier")?.list()?;
    let data_col = grouped.column("data")?.list()?;
    
    // Pre-allocate with estimated capacity
    let mut json_strings: Vec<String> = Vec::with_capacity(height);
    
    for i in 0..height {
        // Allocate per-row buffer (reusing would require clearing)
        let mut json_buffer = String::with_capacity(1024);
        json_buffer.push('[');
        
        // Get the Series for each list column at row i
        if let (Some(bpi), Some(opi), Some(bp), Some(bo), Some(d), Some(da)) = (
            board_point_ind.get_as_series(i),
            off_point_ind.get_as_series(i),
            board_point.get_as_series(i),
            off_point.get_as_series(i),
            dei.get_as_series(i),
            data_col.get_as_series(i),
        ) {
            let len = bpi.len();
            
            // Get string chunked arrays for direct access
            let bpi_ca = bpi.str().ok();
            let opi_ca = opi.str().ok();
            let bp_ca = bp.str().ok();
            let bo_ca = bo.str().ok();
            let d_ca = d.str().ok();
            let da_ca = da.str().ok();
            
            for j in 0..len {
                if j > 0 {
                    json_buffer.push(',');
                }
                json_buffer.push('{');
                
                let mut first = true;
                
                // Optimized field writing with minimal allocations
                macro_rules! write_field {
                    ($ca:expr, $name:literal) => {
                        if let Some(ca) = &$ca {
                            if let Some(s) = ca.get(j) {

                                if !first { 
                                    json_buffer.push(','); 
                                }
                                #[allow(unused_assignments)]
                                { first = false; }
                                json_buffer.push('"');
                                json_buffer.push_str($name);
                                json_buffer.push_str("\":\"");
                                escape_json_into(&mut json_buffer, s);
                                json_buffer.push('"');
                            }
                        }
                    };
                }
                
                write_field!(bpi_ca, "board_point_indicator");
                write_field!(opi_ca, "off_point_indicator");
                write_field!(bp_ca, "board_point");
                write_field!(bo_ca, "off_point");
                write_field!(d_ca, "data_element_identifier");
                write_field!(da_ca, "data");
                
                json_buffer.push('}');
            }
        }
        
        json_buffer.push(']');
        
        // Escape the entire JSON string to make it CSV-safe
        let escaped_json = escape_for_csv(&json_buffer);
        json_strings.push(escaped_json);
    }
    
    // Build the condensed DataFrame with just 3 columns
    DataFrame::new(vec![
        flight_designators,
        control_dups,
        Series::new("segment_data".into(), json_strings).into_column(),
    ])
}

/// Escapes a JSON string to be safe for CSV output by wrapping in quotes
/// and escaping internal quotes
#[inline]
fn escape_for_csv(s: &str) -> String {
    // Check if escaping is needed (contains comma, quote, or newline)
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let mut result = String::with_capacity(s.len() + 10);
        result.push('"');
        for c in s.chars() {
            if c == '"' {
                result.push_str("\"\""); // CSV doubles quotes
            } else {
                result.push(c);
            }
        }
        result.push('"');
        result
    } else {
        s.to_string()
    }
}

/// Escapes special JSON characters directly into a String buffer (zero-copy when possible)
#[inline]
fn escape_json_into(buffer: &mut String, s: &str) {
    // Fast path: check if any escaping is needed
    if !s.bytes().any(|b| matches!(b, b'"' | b'\\' | b'\n' | b'\r' | b'\t' | b'\x00'..=b'\x1F')) {
        buffer.push_str(s);
        return;
    }
    
    // Slow path: escape characters
    for c in s.chars() {
        match c {
            '"' => buffer.push_str("\\\""),
            '\\' => buffer.push_str("\\\\"),
            '\n' => buffer.push_str("\\n"),
            '\r' => buffer.push_str("\\r"),
            '\t' => buffer.push_str("\\t"),
            '\x08' => buffer.push_str("\\b"),
            '\x0C' => buffer.push_str("\\f"),
            c if c.is_control() => {
                // Unicode escape for other control characters
                let _ = write!(buffer, "\\u{:04x}", c as u32);
            }
            _ => buffer.push(c),
        }
    }
}

/// Combines Carrier, Flight, and Segment DataFrames into a single DataFrame.
/// 
/// # Arguments
/// * `carrier` - DataFrame containing Carrier records.
/// * `flights` - DataFrame containing Flight Leg records.
/// * `segments` - DataFrame containing Segment records.
/// * `condense_segments` - If true, aggregates all segments per flight into a single JSON string 
///   column called `segment_data`. This reduces file size and memory but changes the output format.
///   If false (default), each segment remains as a separate row with individual columns.
/// 
/// # Returns
/// * `PolarsResult<DataFrame>` - A combined DataFrame with all records joined.
/// 
/// # Errors
/// Returns an error if the join operations fail.
/// 
/// # Example
/// ```ignore 
/// // Default behavior - flat format (each segment is a row)
/// let combined_df = combine_all_dataframes(carrier_df, flights_df, segments_df, false)?;
/// 
/// // Condensed format - segments as JSON string (smaller file size)
/// let combined_df = combine_all_dataframes(carrier_df, flights_df, segments_df, true)?;
/// ```
pub(crate) fn combine_all_dataframes(
    carrier: DataFrame,
    flights: DataFrame,
    segments: DataFrame,
    condense_segments: bool,
) -> PolarsResult<DataFrame> {
    let flights_with_carrier = flights
        .lazy()
        .drop(cols(["record_type", "record_serial_number"]))
        .join(
            carrier.lazy().drop(cols(["record_type", "record_serial_number"])),
            [col("airline_designator"), col("control_duplicate_indicator")],
            [col("airline_designator"), col("control_duplicate_indicator")],
            JoinArgs::new(JoinType::Left),
        );

    if condense_segments {
        // Condense segments to JSON strings - reduces file size and memory
        let condensed_segments = condense_segments_to_json(segments)?;
        
        flights_with_carrier
            .join(
                condensed_segments.lazy(),
                [col("flight_designator"), col("control_duplicate_indicator")],
                [col("flight_designator"), col("control_duplicate_indicator")],
                JoinArgs::new(JoinType::Left),
            )
            .with_new_streaming(true)
            .collect()
    } else {
        // Flat format - each segment is a separate row (original behavior)
        flights_with_carrier
            .join(
                segments.lazy().select([
                    col("flight_designator"),
                    col("control_duplicate_indicator"),
                    col("board_point_indicator"),
                    col("off_point_indicator"),
                    col("board_point"),
                    col("off_point"),
                    col("data_element_identifier"),
                    col("data"),
                ]),
                [col("flight_designator"), col("control_duplicate_indicator")],
                [col("flight_designator"), col("control_duplicate_indicator")],
                JoinArgs::new(JoinType::Left),
            )
            .with_new_streaming(true)
            .collect()
    }
}