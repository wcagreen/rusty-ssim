use crate::{generators, utils};

use utils::ssim_readers::read_all_ssim;
use utils::ssim_parser_iterator::ssim_iterator;

// TODO - implement a way to convert segments to json and join them to flight legs based on the flight leg identifier