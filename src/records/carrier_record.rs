use serde::{Deserialize, Serialize};
use std::borrow::Cow;


#[derive(Debug, Serialize, Deserialize)]
pub struct CarrierRecord<'a> {
    pub airline_designator:  &'a str,
    pub control_duplicate_indicator: &'a str,
    pub time_mode: &'a str,
    pub season: &'a str,
    pub period_of_schedule_validity_from: &'a str,
    pub period_of_schedule_validity_to: &'a str,
    pub creation_date: &'a str,
    pub title_of_data: &'a str,
    pub release_date: &'a str,
    pub schedule_status: &'a str,
    pub general_information: &'a str,
    pub in_flight_service_information: &'a str,
    pub electronic_ticketing_information: &'a str,
    pub creation_time: &'a str,
    pub record_type: char,
    pub record_serial_number: &'a str,
}
