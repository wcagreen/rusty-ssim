use crate::utils::ssim_streaming::{ssim_to_dataframe_streaming, ssim_to_dataframes_streaming};
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
pub fn ssim_to_dataframe(file_path: &str, batch_size: Option<usize>, buffer_size: Option<usize>) -> PolarsResult<DataFrame> {
    ssim_to_dataframe_streaming(file_path, batch_size, buffer_size)
}


/// Parses an SSIM file into three DataFrames (Carriers, Flights, Segments).
///
/// Automatically chooses between in-memory or streaming mode based on the `streaming` flag.
///
/// # Arguments
/// * `file_path` - Path to the SSIM file.
/// * `streaming` - Optional flag to enable streaming mode (for large files).
/// * `batch_size` - Optional batch size for streaming mode.
/// * `buffer_size` - Optional buffer size for streaming mode. Default is 8192 bytes.
///
/// # Returns
/// * `PolarsResult<(DataFrame, DataFrame, DataFrame)>` - A tuple containing (Carriers, Flights, Segments) DataFrames.
///
/// # Errors
/// Returns an error if parsing fails.
/// 
pub fn ssim_to_dataframes(
    file_path: &str,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    ssim_to_dataframes_streaming(file_path, batch_size, buffer_size)
}
