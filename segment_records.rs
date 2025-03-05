use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentRecords<'a> {
    pub flight_designator: String,
    pub operational_suffix:  &'a str ,
    pub airline_designator:  &'a str,
    pub flight_number: &'a str,
    pub itinerary_variation_identifier:  &'a str,
    pub leg_sequence_number:  &'a str,
    pub service_type:  &'a str,
    pub itinerary_variation_identifier_overflow: &'a str,
    pub board_point_indicator:  &'a str,
    pub off_point_indicator:  &'a str,
    pub data_element_identifier:  &'a str,
    pub board_point:  &'a str,
    pub off_point:  &'a str,
    pub data:  &'a str,
    pub record_type: char,
    pub record_serial_number:  &'a str,
}
