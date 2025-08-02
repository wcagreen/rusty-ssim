use crate::utils::ssim_streaming::{ssim_to_dataframe_streaming, ssim_to_dataframes_streaming};
use polars::error::PolarsResult;
use polars::prelude::{DataFrame, IntoLazy, JoinArgs, JoinType, col, cols};

/// Combines Flights and Carriers into a single DataFrame based on `airline_designator`.
///
/// This function performs a left join of the `flights` and `carrier` DataFrames on the `airline_designator` column.
/// Unnecessary columns such as `record_type` and `record_serial_number` are dropped before merging.
///
/// # Arguments
/// * `carrier` - A Polars DataFrame containing carrier records.
/// * `flights` - A Polars DataFrame containing flight records.
///
/// # Returns
/// * `PolarsResult<DataFrame>` - A combined DataFrame with carrier details joined to flight records.
///
/// # Errors
/// Returns an error if the join operation fails.
pub(crate) fn combine_carrier_and_flights(
    carrier: DataFrame,
    flights: DataFrame,
) -> PolarsResult<DataFrame> {
    let combined_records = flights
        .clone()
        .lazy()
        .drop(cols(["record_type", "record_serial_number"]))
        .join(
            carrier
                .clone()
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
        )
        .collect()?;

    Ok(combined_records)
}

/// Combines Flights and Segments into a single DataFrame based on `flight_designator`.
///
/// The `flight_designator` is a concatenation of:
/// `airline_designator`, `flight_number`, `operational_suffix`,
/// `itinerary_variation_identifier`, `leg_sequence_number`,
/// `service_type`, and `itinerary_variation_identifier_overflow`.
///
/// # Arguments
/// * `flights` - A Polars DataFrame containing flight records.
/// * `segments` - A Polars DataFrame containing segment records.
///
/// # Returns
/// * `PolarsResult<DataFrame>` - A combined DataFrame with segment details joined to flight records.
///
/// # Errors
/// Returns an error if the join operation fails.
pub(crate) fn combine_flights_and_segments(
    flights: DataFrame,
    segments: DataFrame,
) -> PolarsResult<DataFrame> {
    let combined_records = flights
        .clone()
        .lazy()
        .join(
            segments.clone().lazy().select([
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
        .collect()?;

    Ok(combined_records)
}

/// Parses an SSIM file into a single DataFrame.
///
/// Automatically chooses between in-memory or streaming mode based on the `streaming` flag.
///
/// # Arguments
/// * `file_path` - Path to the SSIM file.
/// * `streaming` - Optional flag to enable streaming mode (for large files).
/// * `batch_size` - Optional batch size for streaming mode.
///
/// # Returns
/// * `PolarsResult<DataFrame>` - A combined SSIM DataFrame.
///
/// # Errors
/// Returns an error if parsing or merging fails.
pub fn ssim_to_dataframe(file_path: &str, batch_size: Option<usize>) -> PolarsResult<DataFrame> {
    Ok(ssim_to_dataframe_streaming(file_path, batch_size)?)
}

/// Parses an SSIM file into three DataFrames (Carriers, Flights, Segments).
///
/// Automatically chooses between in-memory or streaming mode based on the `streaming` flag.
///
/// # Arguments
/// * `file_path` - Path to the SSIM file.
/// * `streaming` - Optional flag to enable streaming mode (for large files).
/// * `batch_size` - Optional batch size for streaming mode.
///
/// # Returns
/// * `PolarsResult<(DataFrame, DataFrame, DataFrame)>` - A tuple containing (Carriers, Flights, Segments) DataFrames.
///
/// # Errors
/// Returns an error if parsing fails.
pub fn ssim_to_dataframes(
    file_path: &str,
    batch_size: Option<usize>,
) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    Ok(ssim_to_dataframes_streaming(file_path, batch_size)?)
}
