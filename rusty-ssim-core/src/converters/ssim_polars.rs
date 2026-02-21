use polars::error::PolarsResult;
use polars::prelude::*;

/// Aggregates raw segment rows into a nested list structure keyed by flight.
///
/// When the caller wishes to "condense" the segment data, every record in the
/// supplied `segments` DataFrame is grouped by the combination of
/// `flight_designator`, `control_duplicate_indicator` and
/// `leg_sequence_number`.  Instead of returning a flat table with one row per
/// individual segment, the output DataFrame contains one row per unique flight
/// key.  All of the original segment columns are collected into a single
/// `List<Struct>` column named `segment_data` where each struct holds the
/// fields listed below:
///
/// * `board_point_indicator`
/// * `off_point_indicator`
/// * `board_point`
/// * `off_point`
/// * `data_element_identifier`
/// * `data`
///
/// The resulting column can be serialized to JSON (see
/// [`serialize_segment_data_to_json`]) for CSV output or kept in its nested form
/// for parquet/Polars-native workflows.
///
/// # Arguments
///
/// * `segments` - a DataFrame containing the raw segment records produced by
///   the parser.  The caller is responsible for ensuring the necessary columns
///   are present; they are not validated by this helper.
///
/// # Returns
///
/// A `PolarsResult<DataFrame>` holding the grouped and nested output.  On
/// failure, the error will propagate from the underlying lazy execution and
/// collection process.
///
/// # Example
///
/// ```ignore
/// let condensed = condense_segments_to_structs(segments_df)?;
///
/// ```
fn condense_segments_to_structs(segments: DataFrame) -> PolarsResult<DataFrame> {
    let grouped = segments
        .lazy()
        .group_by([
            col("flight_designator"),
            col("control_duplicate_indicator"),
            col("leg_sequence_number"),
        ])
        .agg([
            as_struct(vec![
                col("board_point_indicator"),
                col("off_point_indicator"),
                col("board_point"),
                col("off_point"),
                col("data_element_identifier"),
                col("data"),
            ])
            .alias("segment_data"),
        ])
        .collect()?;

    Ok(grouped)
}

/// Serializes the `segment_data` List<Struct> column to JSON strings for CSV compatibility.
/// CSV cannot represent nested types natively, so this will convert `segment_data` into a `String` column where each cell contains a JSON
/// array string, e.g. `[{"board_point":"LHR","off_point":"JFK",...},...]`.
///
/// This is only nneeded if `condense_segments` is true and you want to export to CSV. If exporting to Parquet, you can keep the nested List<Struct> format without serialization.
/// 
pub(crate) fn serialize_segment_data_to_json(df: DataFrame) -> PolarsResult<DataFrame> {
    let mut df = df
        .lazy()
        .with_column(
            col("segment_data")
                .list()
                .eval(col("").struct_().json_encode())
                .list()
                .join(lit(","), false)
                .alias("segment_data"),
        )
        .collect()?;

    let wrapped = df
        .column("segment_data")?
        .str()?
        .apply(|opt| opt.map(|v| format!("[{}]", v).into()))
        .into_series()
        .with_name("segment_data".into());

    df.replace("segment_data", wrapped.into())?;

    Ok(df)
}

/// Combines Carrier, Flight, and Segment DataFrames into a single DataFrame.
///
/// # Arguments
/// * `carrier` - DataFrame containing Carrier records.
/// * `flights` - DataFrame containing Flight Leg records.
/// * `segments` - DataFrame containing Segment records.
/// * `condense_segments` - If true, aggregates all segments per flight into a List<Struct>
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
/// // Condensed format - segments as List<Struct> (smaller file size)
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
            carrier
                .lazy()
                .drop(cols(["record_type", "record_serial_number"])),
            [
                col("airline_designator"),
                col("control_duplicate_indicator"),
            ],
            [
                col("airline_designator"),
                col("control_duplicate_indicator"),
            ],
            JoinArgs::new(JoinType::Left),
        );

    if condense_segments {
        // Condense segments to List<Struct> per flight
        let condensed_segments = condense_segments_to_structs(segments)?;

        flights_with_carrier
            .join(
                condensed_segments.lazy(),
                [
                    col("flight_designator"),
                    col("control_duplicate_indicator"),
                    col("leg_sequence_number"),
                ],
                [
                    col("flight_designator"),
                    col("control_duplicate_indicator"),
                    col("leg_sequence_number"),
                ],
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
                    col("leg_sequence_number"),
                    col("control_duplicate_indicator"),
                    col("board_point_indicator"),
                    col("off_point_indicator"),
                    col("board_point"),
                    col("off_point"),
                    col("data_element_identifier"),
                    col("data"),
                ]),
                [
                    col("flight_designator"),
                    col("control_duplicate_indicator"),
                    col("leg_sequence_number"),
                ],
                [
                    col("flight_designator"),
                    col("control_duplicate_indicator"),
                    col("leg_sequence_number"),
                ],
                JoinArgs::new(JoinType::Left),
            )
            .with_new_streaming(true)
            .collect()
    }
}
