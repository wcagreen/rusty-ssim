use serde::{Deserialize, Serialize};
use std::borrow::Cow;
#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentRecords {
    pub flight_designator: Cow<'static, str>,
    pub operational_suffix: Cow<'static, str>,
    pub airline_designator: Cow<'static, str>,
    pub control_duplicate_indicator: Cow<'static, str>,
    pub flight_number: Cow<'static, str>,
    pub itinerary_variation_identifier: Cow<'static, str>,
    pub leg_sequence_number: Cow<'static, str>,
    pub service_type: Cow<'static, str>,
    pub itinerary_variation_identifier_overflow: Cow<'static, str>,
    pub board_point_indicator: Cow<'static, str>,
    pub off_point_indicator: Cow<'static, str>,
    pub data_element_identifier: Cow<'static, str>,
    pub board_point: Cow<'static, str>,
    pub off_point: Cow<'static, str>,
    pub data: Cow<'static, str>,
    pub record_type: char,
    pub record_serial_number: Cow<'static, str>,
}
