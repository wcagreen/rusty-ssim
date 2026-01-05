use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentRecords<'a> {
    pub flight_designator: Cow<'a, str>,
    pub operational_suffix: Cow<'a, str>,
    pub airline_designator: Cow<'a, str>,
    pub control_duplicate_indicator: Cow<'a, str>,
    pub flight_number: Cow<'a, str>,
    pub itinerary_variation_identifier: Cow<'a, str>,
    pub leg_sequence_number: Cow<'a, str>,
    pub service_type: Cow<'a, str>,
    pub itinerary_variation_identifier_overflow: Cow<'a, str>,
    pub board_point_indicator: Cow<'a, str>,
    pub off_point_indicator: Cow<'a, str>,
    pub data_element_identifier: Cow<'a, str>,
    pub board_point: Cow<'a, str>,
    pub off_point: Cow<'a, str>,
    pub data: Cow<'a, str>,
    pub record_type: char,
    pub record_serial_number: Cow<'a, str>,
}
