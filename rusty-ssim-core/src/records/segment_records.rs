use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentRecords {
    pub flight_designator: String,
    pub operational_suffix: String,
    pub airline_designator: String,
    pub control_duplicate_indicator: String,
    pub flight_number: String,
    pub itinerary_variation_identifier: String,
    pub leg_sequence_number: String,
    pub service_type: String,
    pub itinerary_variation_identifier_overflow: String,
    pub board_point_indicator: String,
    pub off_point_indicator: String,
    pub data_element_identifier: String,
    pub board_point: String,
    pub off_point: String,
    pub data: String,
    pub record_type: char,
    pub record_serial_number: String,
}
