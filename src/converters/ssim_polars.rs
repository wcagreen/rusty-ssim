use crate::{generators, utils};

use utils::ssim_readers::read_all_ssim;
use utils::ssim_parser_iterator::ssim_iterator;
use generators::ssim_dataframe::convert_to_dataframes;

pub fn ssim_to_dataframes(
    file_path: &str,
) -> polars::prelude::PolarsResult<(
    polars::prelude::DataFrame,
    polars::prelude::DataFrame,
    polars::prelude::DataFrame,
)> {
    let ssim = read_all_ssim(&file_path);
    let (record_type_2, record_type_3s, record_type_4s) =
        ssim_iterator(ssim).expect("Failed to parse SSIM records.");
    let (carrier_df, flight_df, segment_df) =
        convert_to_dataframes(record_type_2, record_type_3s, record_type_4s)
            .expect("Failed to build dataframes.");
    Ok((carrier_df, flight_df, segment_df))
}