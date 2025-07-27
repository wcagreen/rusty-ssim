use crate::{generators, utils};
use polars::prelude::{col, IntoLazy, JoinArgs, JoinType};
use polars_core::prelude::DataFrame;

use generators::ssim_dataframe::convert_to_dataframes;
use utils::ssim_parser_iterator::ssim_iterator;
use utils::ssim_readers::read_all_ssim;

pub fn ssim_to_dataframes(
    file_path: &str,
) -> polars::prelude::PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    let ssim = read_all_ssim(&file_path);

    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");

    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");

    Ok((carrier_df, flight_df, segment_df))
}

// "airline_designator", "flight_number", "operational_suffix", "itinerary_variation_identifier" ,"leg_sequence_number", "service_type", "itinerary_variation_identifier_overflow"
fn combine_flights_and_segments(
    flights: DataFrame,
    segments: DataFrame,
) -> polars::prelude::PolarsResult<(DataFrame)> {
    let combine_records = flights
        .clone()
        .lazy()
        .drop([col("record_type"), col("record_serial_number")])
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

    Ok(combine_records)
}

pub fn ssim_to_dataframe(file_path: &str) -> polars::prelude::PolarsResult<(DataFrame)> {
    let ssim = read_all_ssim(&file_path);

    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");

    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");

    let ssim_dataframe = combine_flights_and_segments(flight_df, segment_df);

    Ok(ssim_dataframe?)
}
