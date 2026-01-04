use polars::error::PolarsResult;
use polars::prelude::{DataFrame, IntoLazy, JoinArgs, JoinType, col, cols};


/// Combines Carrier, Flight, and Segment DataFrames into a single DataFrame.
/// # Arguments
/// * `carrier` - DataFrame containing Carrier records.
/// * `flights` - DataFrame containing Flight Leg records.
/// * `segments` - DataFrame containing Segment records.
/// # Returns
/// * `PolarsResult<DataFrame>` - A combined DataFrame with all records joined.
/// # Errors
/// Returns an error if the join operations fail.
/// 
/// Example:
/// ```ignore 
/// let combined_df = combine_all_dataframes(carrier_df, flights_df, segments_df)?;
/// ```
pub(crate) fn combine_all_dataframes(
    carrier: DataFrame,
    flights: DataFrame,
    segments: DataFrame,
) -> PolarsResult<DataFrame> {
    flights
        .lazy()
        .drop(cols(["record_type", "record_serial_number"]))
        .join(
            carrier.lazy().drop(cols(["record_type", "record_serial_number"])),
            [col("airline_designator"), col("control_duplicate_indicator")],
            [col("airline_designator"), col("control_duplicate_indicator")],
            JoinArgs::new(JoinType::Left),
        )
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
        .collect()
}