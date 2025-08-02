use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CarrierRecord {
    pub airline_designator: String,
    pub control_duplicate_indicator: String,
    pub time_mode: String,
    pub season: String,
    pub period_of_schedule_validity_from: String,
    pub period_of_schedule_validity_to: String,
    pub creation_date: String,
    pub title_of_data: String,
    pub release_date: String,
    pub schedule_status: String,
    pub general_information: String,
    pub in_flight_service_information: String,
    pub electronic_ticketing_information: String,
    pub creation_time: String,
    pub record_type: char,
    pub record_serial_number: String,
}
