use polars::prelude::*;
use crate::utils::ssim_parser::{CarrierRecord, FlightLegRecord, SegmentRecords};

pub fn convert_to_dataframes(
    carriers: Vec<CarrierRecord>,
    flights: Vec<FlightLegRecord>,
    segments: Vec<SegmentRecords>,
) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    

    let carrier_df = if !carriers.is_empty() {
        df! {
            "airline_designator" => carriers.iter().map(|r| r.airline_designator.as_str()).collect::<Vec<_>>(),
            "control_duplicate_indicator" => carriers.iter().map(|r| r.control_duplicate_indicator.as_str()).collect::<Vec<_>>(),
            "time_mode" => carriers.iter().map(|r| r.time_mode.as_str()).collect::<Vec<_>>(),
            "season" => carriers.iter().map(|r| r.season.as_str()).collect::<Vec<_>>(),
            "period_of_schedule_validity_from" => carriers.iter().map(|r| r.period_of_schedule_validity_from.as_str()).collect::<Vec<_>>(),
            "period_of_schedule_validity_to" => carriers.iter().map(|r| r.period_of_schedule_validity_to.as_str()).collect::<Vec<_>>(),
            "creation_date" => carriers.iter().map(|r| r.creation_date.as_str()).collect::<Vec<_>>(),
            "title_of_data" => carriers.iter().map(|r| r.title_of_data.as_str()).collect::<Vec<_>>(),
            "release_date" => carriers.iter().map(|r| r.release_date.as_str()).collect::<Vec<_>>(),
            "schedule_status" => carriers.iter().map(|r| r.schedule_status.as_str()).collect::<Vec<_>>(),
            "general_information" => carriers.iter().map(|r| r.general_information.as_str()).collect::<Vec<_>>(),
            "in_flight_service_information" => carriers.iter().map(|r| r.in_flight_service_information.as_str()).collect::<Vec<_>>(),
            "electronic_ticketing_information" => carriers.iter().map(|r| r.electronic_ticketing_information.as_str()).collect::<Vec<_>>(),
            "creation_time" => carriers.iter().map(|r| r.creation_time.as_str()).collect::<Vec<_>>(),
            "record_type" => carriers.iter().map(|r| r.record_type.to_string()).collect::<Vec<_>>(),
            "record_serial_number" => carriers.iter().map(|r| r.record_serial_number.as_str()).collect::<Vec<_>>(),
        }?
    } else {
        DataFrame::empty()
    };

        let flight_df = if !flights.is_empty() {
        df! {
            "flight_designator" => flights.iter().map(|r| r.flight_designator.as_str()).collect::<Vec<_>>(),
            "operational_suffix" => flights.iter().map(|r| r.operational_suffix.as_str()).collect::<Vec<_>>(),
            "airline_designator" => flights.iter().map(|r| r.airline_designator.as_str()).collect::<Vec<_>>(),
            "flight_number" => flights.iter().map(|r| r.flight_number.as_str()).collect::<Vec<_>>(),
            "itinerary_variation_identifier" => flights.iter().map(|r| r.itinerary_variation_identifier.as_str()).collect::<Vec<_>>(),
            "leg_sequence_number" => flights.iter().map(|r| r.leg_sequence_number.as_str()).collect::<Vec<_>>(),
            "service_type" => flights.iter().map(|r| r.service_type.as_str()).collect::<Vec<_>>(),
            "period_of_operation_from" => flights.iter().map(|r| r.period_of_operation_from.as_str()).collect::<Vec<_>>(),
            "period_of_operation_to" => flights.iter().map(|r| r.period_of_operation_to.as_str()).collect::<Vec<_>>(),
            "days_of_operation" => flights.iter().map(|r| r.days_of_operation.as_str()).collect::<Vec<_>>(),
            "frequency_rate" => flights.iter().map(|r| r.frequency_rate.as_str()).collect::<Vec<_>>(),
            "departure_station" => flights.iter().map(|r| r.departure_station.as_str()).collect::<Vec<_>>(),
            "scheduled_time_of_passenger_departure" => flights.iter().map(|r| r.scheduled_time_of_passenger_departure.as_str()).collect::<Vec<_>>(),
            "scheduled_time_of_aircraft_departure" => flights.iter().map(|r| r.scheduled_time_of_aircraft_departure.as_str()).collect::<Vec<_>>(),
            "time_variation_departure" => flights.iter().map(|r| r.time_variation_departure.as_str()).collect::<Vec<_>>(),
            "passenger_terminal_departure" => flights.iter().map(|r| r.passenger_terminal_departure.as_str()).collect::<Vec<_>>(),
            "arrival_station" => flights.iter().map(|r| r.arrival_station.as_str()).collect::<Vec<_>>(),
            "scheduled_time_of_aircraft_arrival" => flights.iter().map(|r| r.scheduled_time_of_aircraft_arrival.as_str()).collect::<Vec<_>>(),
            "scheduled_time_of_passenger_arrival" => flights.iter().map(|r| r.scheduled_time_of_passenger_arrival.as_str()).collect::<Vec<_>>(),
            "time_variation_arrival" => flights.iter().map(|r| r.time_variation_arrival.as_str()).collect::<Vec<_>>(),
            "passenger_terminal_arrival" => flights.iter().map(|r| r.passenger_terminal_arrival.as_str()).collect::<Vec<_>>(),
            "aircraft_type" => flights.iter().map(|r| r.aircraft_type.as_str()).collect::<Vec<_>>(),
            "passenger_reservations_booking_designator" => flights.iter().map(|r| r.passenger_reservations_booking_designator.as_str()).collect::<Vec<_>>(),
            "passenger_reservations_booking_modifier" => flights.iter().map(|r| r.passenger_reservations_booking_modifier.as_str()).collect::<Vec<_>>(),
            "meal_service_note" => flights.iter().map(|r| r.meal_service_note.as_str()).collect::<Vec<_>>(),
            "joint_operation_airline_designators" => flights.iter().map(|r| r.joint_operation_airline_designators.as_str()).collect::<Vec<_>>(),
            "min_connecting_time_status_departure"=> flights.iter().map(|r| r.min_connecting_time_status_departure.as_str()).collect::<Vec<_>>(),
            "min_connecting_time_status_arrival"=> flights.iter().map(|r| r.min_connecting_time_status_arrival.as_str()).collect::<Vec<_>>(),
            "secure_flight_indicator" => flights.iter().map(|r| r.secure_flight_indicator.as_str()).collect::<Vec<_>>(),
            "itinerary_variation_identifier_overflow" => flights.iter().map(|r| r.itinerary_variation_identifier_overflow.as_str()).collect::<Vec<_>>(),
            "aircraft_owner" => flights.iter().map(|r| r.aircraft_owner.as_str()).collect::<Vec<_>>(),
            "cockpit_crew_employer" => flights.iter().map(|r| r.cockpit_crew_employer.as_str()).collect::<Vec<_>>(),
            "cabin_crew_employer" => flights.iter().map(|r| r.cabin_crew_employer.as_str()).collect::<Vec<_>>(),
            "onward_flight" => flights.iter().map(|r| r.record_serial_number.as_str()).collect::<Vec<_>>(),
            "airline_designator2" => flights.iter().map(|r| r.airline_designator2.as_str()).collect::<Vec<_>>(),
            "flight_number2" => flights.iter().map(|r| r.flight_number2.as_str()).collect::<Vec<_>>(),
            "aircraft_rotation_layover" => flights.iter().map(|r| r.aircraft_rotation_layover.as_str()).collect::<Vec<_>>(),
            "operational_suffix2" => flights.iter().map(|r| r.operational_suffix2.as_str()).collect::<Vec<_>>(),
            "flight_transit_layover" => flights.iter().map(|r| r.flight_transit_layover.as_str()).collect::<Vec<_>>(),
            "operating_airline_disclosure" => flights.iter().map(|r| r.operating_airline_disclosure.as_str()).collect::<Vec<_>>(),
            "traffic_restriction_code" => flights.iter().map(|r| r.traffic_restriction_code.as_str()).collect::<Vec<_>>(),
            "traffic_restriction_code_leg_overflow_indicator" => flights.iter().map(|r| r.traffic_restriction_code_leg_overflow_indicator.as_str()).collect::<Vec<_>>(),
            "aircraft_configuration" => flights.iter().map(|r| r.aircraft_configuration.as_str()).collect::<Vec<_>>(),
            "date_variation" => flights.iter().map(|r| r.date_variation.as_str()).collect::<Vec<_>>(),
            "record_type" => flights.iter().map(|r| r.record_type.to_string()).collect::<Vec<_>>(),
            "record_serial_number" => flights.iter().map(|r| r.record_serial_number.as_str()).collect::<Vec<_>>(),
        }?
    } else {
        DataFrame::empty()
    };

    let segment_df = if !segments.is_empty() {
        df! {
            "flight_designator" => segments.iter().map(|r| r.flight_designator.as_str()).collect::<Vec<_>>(),
            "operational_suffix" => segments.iter().map(|r| r.operational_suffix.as_str()).collect::<Vec<_>>(),
            "airline_designator" => segments.iter().map(|r| r.airline_designator.as_str()).collect::<Vec<_>>(),
            "flight_number" => segments.iter().map(|r| r.flight_number.as_str()).collect::<Vec<_>>(),
            "itinerary_variation_identifier" => segments.iter().map(|r| r.itinerary_variation_identifier.as_str()).collect::<Vec<_>>(),
            "leg_sequence_number" => segments.iter().map(|r| r.leg_sequence_number.as_str()).collect::<Vec<_>>(),
            "itinerary_variation_identifier_overflow" => segments.iter().map(|r| r.itinerary_variation_identifier_overflow.as_str()).collect::<Vec<_>>(),
            "board_point_indicator" => segments.iter().map(|r| r.board_point_indicator.as_str()).collect::<Vec<_>>(),
            "off_point_indicator" => segments.iter().map(|r| r.off_point_indicator.as_str()).collect::<Vec<_>>(),
            "data_element_identifier" => segments.iter().map(|r| r.data_element_identifier.as_str()).collect::<Vec<_>>(),
            "board_point" => segments.iter().map(|r| r.board_point.as_str()).collect::<Vec<_>>(),
            "off_point" => segments.iter().map(|r| r.off_point.as_str()).collect::<Vec<_>>(),
            "data" => segments.iter().map(|r| r.data.as_str()).collect::<Vec<_>>(),
            "record_type" => segments.iter().map(|r| r.record_type.to_string()).collect::<Vec<_>>(),
            "record_serial_number" => segments.iter().map(|r| r.record_serial_number.as_str()).collect::<Vec<_>>(),
        }?
    } else {
        DataFrame::empty()
    };

        Ok((carrier_df, flight_df, segment_df))
}