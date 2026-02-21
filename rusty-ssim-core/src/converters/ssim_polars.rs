use polars::error::PolarsResult;
use polars::prelude::*;

/// Condenses segments DataFrame into List<Struct> per flight
fn condense_segments_to_json(segments: DataFrame) -> PolarsResult<DataFrame> {
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
        let condensed_segments = condense_segments_to_json(segments)?;

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
