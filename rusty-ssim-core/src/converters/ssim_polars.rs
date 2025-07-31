use polars::error::PolarsResult;
use polars::prelude::DataFrame;
use polars::prelude::{col, IntoLazy, JoinArgs, JoinType};


use crate::utils::ssim_streaming::ssim_to_dataframes_streaming;


use crate::{generators, utils};
use generators::ssim_dataframe::convert_to_dataframes;
use utils::ssim_parser_iterator::ssim_iterator;
use utils::ssim_readers::read_all_ssim;

/// Takes Flights and Carriers and combines them under one dataframe based on Airline Designator.

///
/// # Arguments
/// * `flights` - Flight Polars Dataframe.
/// * `carrier` - carrier Polars Dataframes.
/// # Errors
/// Returns a Polars Dataframe and if merge fails, then it errors out.
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

/// Takes Flights and Segments and combines them under one dataframe based on Flight Designator.
/// Flight Designator is a string of "airline_designator", "flight_number", "operational_suffix", "itinerary_variation_identifier" ,"leg_sequence_number", "service_type", "itinerary_variation_identifier_overflow" combine.
///
/// # Arguments
/// * `flights` - Flight Polars Dataframe.
/// * `segments` - Segment Polars Dataframes.
/// # Errors
/// Returns a Polars Dataframe and if merge fails, then it errors out.
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

pub fn ssim_to_dataframe_streaming(file_path: &str, batch_size: Option<usize>) -> PolarsResult<DataFrame> {
    let (carrier_df, flight_df, segment_df) = ssim_to_dataframes_streaming(file_path, batch_size)?;
    let mut ssim_dataframe = combine_carrier_and_flights(carrier_df, flight_df)?;
    ssim_dataframe = combine_flights_and_segments(ssim_dataframe, segment_df)?;
    Ok(ssim_dataframe)
}


/// Takes Flights and Segments and combines them under one dataframe based on Flight Designator.
/// Flight Designator is a string of "airline_designator", "flight_number", "operational_suffix", "itinerary_variation_identifier" ,"leg_sequence_number", "service_type", "itinerary_variation_identifier_overflow" combine.
///
/// # Arguments
/// * `file_path` - SSIM File Path.
/// # Errors
/// Returns a Polars Dataframe others it errors out.
pub fn ssim_to_dataframe(file_path: &str, streaming: Option<bool>, batch_size: Option<usize>) -> PolarsResult<DataFrame> {

    if streaming.unwrap_or(false) {
        Ok(ssim_to_dataframe_streaming(file_path, batch_size)?)
    } else {
        Ok(ssim_to_dataframe_memory(file_path)?)
    }

}

pub fn ssim_to_dataframes_memory(file_path: &str) ->PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    let ssim = read_all_ssim(&file_path);

    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");

    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");
    Ok((carrier_df, flight_df, segment_df))
}


/// Reads in SSIM file and parses it out as three dataframes. One dataframe for each of the following records types (2, 3, 4).
/// # Arguments
/// * `file_path` - SSIM File Path.
/// # Errors
/// Returns three Polars Dataframe others it errors out.
pub fn ssim_to_dataframes(file_path: &str, streaming: Option<bool>, batch_size: Option<usize>) ->  PolarsResult<(DataFrame, DataFrame, DataFrame)> {

    if streaming.unwrap_or(false) {
        Ok(ssim_to_dataframes_streaming(file_path, batch_size)?)
    } else {
        Ok(ssim_to_dataframes_memory(file_path)?)
    }

}

