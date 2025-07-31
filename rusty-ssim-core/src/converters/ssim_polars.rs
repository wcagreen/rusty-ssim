use polars::error::PolarsResult;
use polars::prelude::DataFrame;
use polars::prelude::{col, IntoLazy, JoinArgs, JoinType};


use crate::utils::ssim_streaming::ssim_to_dataframes_streaming;


use crate::{generators, utils};
use generators::ssim_dataframe::convert_to_dataframes;
use utils::ssim_parser_iterator::ssim_iterator;
use utils::ssim_readers::read_all_ssim;



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
        .drop([col("record_type"), col("record_serial_number")])
        .join(
            carrier
                .clone()
                .lazy()
                .drop([col("record_type"), col("record_serial_number")]),
            [col("airline_designator")],
            [col("airline_designator")],
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
                col("board_point_indicator"),
                col("off_point_indicator"),
                col("board_point"),
                col("off_point"),
                col("data_element_identifier"),
                col("data"),
            ]),
            [col("flight_designator")],
            [col("flight_designator")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    Ok(combined_records)
}

/// Parses an SSIM file into a single DataFrame in memory.
///
/// Reads all SSIM records into memory, converts record types 2 (Carriers), 3 (Flights),
/// and 4 (Segments) into DataFrames, and then merges them into a single DataFrame.
///
/// # Arguments
/// * `file_path` - Path to the SSIM file.
///
/// # Returns
/// * `PolarsResult<DataFrame>` - A combined SSIM DataFrame.
///
/// # Errors
/// Returns an error if file reading, parsing, or merging fails.
pub fn ssim_to_dataframe_memory(file_path: &str) -> PolarsResult<DataFrame> {
    let ssim = read_all_ssim(&file_path);

    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");

    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");
    let mut ssim_dataframe = combine_carrier_and_flights(carrier_df, flight_df);

    // TODO Need to rework how merge works because control duplicate indicator. This is only an issue with multi carrier files that codes like XX and XX*.
    ssim_dataframe = combine_flights_and_segments(ssim_dataframe?, segment_df);

    Ok(ssim_dataframe?)
}


/// Parses an SSIM file into a single DataFrame using streaming (optimized for large files).
///
/// This function processes the SSIM file in batches, converting record types 2 (Carriers),
/// 3 (Flights), and 4 (Segments) into DataFrames. It then merges them into a single combined DataFrame.
/// Streaming mode reduces memory usage by loading and processing only a portion of the file at a time.
///
/// # Arguments
/// * `file_path` - Path to the SSIM file.
/// * `batch_size` - Optional batch size for streaming. If `None`, a default batch size is used.
///
/// # Returns
/// * `PolarsResult<DataFrame>` - A combined SSIM DataFrame containing carriers, flights, and segments.
///
/// # Errors
/// Returns an error if file reading, parsing, or merging fails.
pub fn ssim_to_dataframe_streaming(file_path: &str, batch_size: Option<usize>) -> PolarsResult<DataFrame> {
    let (carrier_df, flight_df, segment_df) = ssim_to_dataframes_streaming(file_path, batch_size)?;
    let mut ssim_dataframe = combine_carrier_and_flights(carrier_df, flight_df)?;
    ssim_dataframe = combine_flights_and_segments(ssim_dataframe, segment_df)?;
    Ok(ssim_dataframe)
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
pub fn ssim_to_dataframe(file_path: &str, streaming: Option<bool>, batch_size: Option<usize>) -> PolarsResult<DataFrame> {

    if streaming.unwrap_or(false) {
        Ok(ssim_to_dataframe_streaming(file_path, batch_size)?)
    } else {
        Ok(ssim_to_dataframe_memory(file_path)?)
    }

}


/// Parses an SSIM file into three DataFrames (Carriers, Flights, Segments) in memory.
///
/// Reads all SSIM records into memory and separates record types 2, 3, and 4
/// into individual DataFrames.
///
/// # Arguments
/// * `file_path` - Path to the SSIM file.
///
/// # Returns
/// * `PolarsResult<(DataFrame, DataFrame, DataFrame)>` - A tuple containing (Carriers, Flights, Segments) DataFrames.
///
/// # Errors
/// Returns an error if file reading or parsing fails.
pub fn ssim_to_dataframes_memory(file_path: &str) ->PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    let ssim = read_all_ssim(&file_path);

    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");

    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");
    Ok((carrier_df, flight_df, segment_df))
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
pub fn ssim_to_dataframes(file_path: &str, streaming: Option<bool>, batch_size: Option<usize>) ->  PolarsResult<(DataFrame, DataFrame, DataFrame)> {

    if streaming.unwrap_or(false) {
        Ok(ssim_to_dataframes_streaming(file_path, batch_size)?)
    } else {
        Ok(ssim_to_dataframes_memory(file_path)?)
    }

}

