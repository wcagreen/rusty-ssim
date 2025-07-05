// use polars::prelude::{Series, PolarsResult, NamedFrom};
use polars::prelude::*;
use crate::parser::{CarrierRecord, FlightLegRecord, SegmentRecords};

pub fn convert_to_dataframes(
    carriers: Vec<CarrierRecord>,
    flights: Vec<FlightLegRecord>,
    segments: Vec<SegmentRecords>,
) -> polars::prelude::PolarsResult<(polars::prelude::DataFrame, polars::prelude::DataFrame, polars::prelude::DataFrame)> {
    let carrier_series = CarrierRecord::get_columns(&carriers)?;
    let flight_series = FlightLegRecord::get_columns(&flights)?;
    let segment_series = SegmentRecords::get_columns(&segments)?;
    let carrier_df = polars::prelude::DataFrame::new(carrier_series)?;
    let flight_df = polars::prelude::DataFrame::new(flight_series)?;
    let segment_df = polars::prelude::DataFrame::new(segment_series)?;
    Ok((carrier_df, flight_df, segment_df))
}


impl CarrierRecord {
    pub fn get_columns(records: &[CarrierRecord]) -> PolarsResult<Vec<Series>> {
        Ok(vec![
            polars::prelude::Series::new("airline_designator".into(), records.iter().map(|r| r.airline_designator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("control_duplicate_indicator".into(), records.iter().map(|r| r.control_duplicate_indicator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("time_mode".into(), records.iter().map(|r| r.time_mode.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("season".into(), records.iter().map(|r| r.season.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("period_of_schedule_validity_from".into(), records.iter().map(|r| r.period_of_schedule_validity_from.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("period_of_schedule_validity_to".into(), records.iter().map(|r| r.period_of_schedule_validity_to.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("creation_date".into(), records.iter().map(|r| r.creation_date.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("title_of_data".into(), records.iter().map(|r| r.title_of_data.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("release_date".into(), records.iter().map(|r| r.release_date.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("schedule_status".into(), records.iter().map(|r| r.schedule_status.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("general_information".into(), records.iter().map(|r| r.general_information.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("in_flight_service_information".into(), records.iter().map(|r| r.in_flight_service_information.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("electronic_ticketing_information".into(), records.iter().map(|r| r.electronic_ticketing_information.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("creation_time".into(), records.iter().map(|r| r.creation_time.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("record_type".into(), records.iter().map(|r| r.record_type.to_string()).collect::<Vec<String>>()),
            polars::prelude::Series::new("record_serial_number".into(), records.iter().map(|r| r.record_serial_number.clone()).collect::<Vec<String>>()),
        ])
    }
}


impl FlightLegRecord {
    pub fn get_columns(records: &[FlightLegRecord]) -> PolarsResult<Vec<Series>> {
        Ok(vec![
            polars::prelude::Series::new("flight_designator".into(), records.iter().map(|r| r.flight_designator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("operational_suffix".into(), records.iter().map(|r| r.operational_suffix.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("airline_designator".into(), records.iter().map(|r| r.airline_designator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("itinerary_variation_identifier".into(), records.iter().map(|r| r.itinerary_variation_identifier.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("leg_sequence_number".into(), records.iter().map(|r| r.leg_sequence_number.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("service_type".into(), records.iter().map(|r| r.service_type.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("period_of_operation_from".into(), records.iter().map(|r| r.period_of_operation_from.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("period_of_operation_to".into(), records.iter().map(|r| r.period_of_operation_to.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("days_of_operation".into(), records.iter().map(|r| r.days_of_operation.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("frequency_rate".into(), records.iter().map(|r| r.frequency_rate.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("departure_station".into(), records.iter().map(|r| r.departure_station.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("scheduled_time_of_passenger_departure".into(), records.iter().map(|r| r.scheduled_time_of_passenger_departure.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("scheduled_time_of_aircraft_departure".into(), records.iter().map(|r| r.scheduled_time_of_aircraft_departure.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("time_variation_departure".into(), records.iter().map(|r| r.time_variation_departure.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("passenger_terminal_departure".into(), records.iter().map(|r| r.passenger_terminal_departure.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("arrival_station".into(), records.iter().map(|r| r.arrival_station.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("scheduled_time_of_aircraft_arrival".into(), records.iter().map(|r| r.scheduled_time_of_aircraft_arrival.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("scheduled_time_of_passenger_arrival".into(), records.iter().map(|r| r.scheduled_time_of_passenger_arrival.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("time_variation_arrival".into(), records.iter().map(|r| r.time_variation_arrival.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("passenger_terminal_arrival".into(), records.iter().map(|r| r.passenger_terminal_arrival.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("aircraft_type".into(), records.iter().map(|r| r.aircraft_type.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("passenger_reservations_booking_designator".into(), records.iter().map(|r| r.passenger_reservations_booking_designator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("passenger_reservations_booking_modifier".into(), records.iter().map(|r| r.passenger_reservations_booking_modifier.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("meal_service_note".into(), records.iter().map(|r| r.meal_service_note.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("joint_operation_airline_designators".into(), records.iter().map(|r| r.joint_operation_airline_designators.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("min_connecting_time_status_departure".into(), records.iter().map(|r| r.min_connecting_time_status_departure.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("min_connecting_time_status_arrival".into(), records.iter().map(|r| r.min_connecting_time_status_arrival.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("secure_flight_indicator".into(), records.iter().map(|r| r.secure_flight_indicator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("itinerary_variation_identifier_overflow".into(), records.iter().map(|r| r.itinerary_variation_identifier_overflow.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("aircraft_owner".into(), records.iter().map(|r| r.aircraft_owner.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("cockpit_crew_employer".into(), records.iter().map(|r| r.cockpit_crew_employer.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("cabin_crew_employer".into(), records.iter().map(|r| r.cabin_crew_employer.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("onward_flight".into(), records.iter().map(|r| r.onward_flight.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("airline_designator2".into(), records.iter().map(|r| r.airline_designator2.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("flight_number2".into(), records.iter().map(|r| r.flight_number2.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("aircraft_rotation_layover".into(), records.iter().map(|r| r.aircraft_rotation_layover.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("operational_suffix2".into(), records.iter().map(|r| r.operational_suffix2.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("flight_transit_layover".into(), records.iter().map(|r| r.flight_transit_layover.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("operating_airline_disclosure".into(), records.iter().map(|r| r.operating_airline_disclosure.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("traffic_restriction_code".into(), records.iter().map(|r| r.traffic_restriction_code.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("traffic_restriction_code_leg_overflow_indicator".into(), records.iter().map(|r| r.traffic_restriction_code_leg_overflow_indicator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("aircraft_configuration".into(), records.iter().map(|r| r.aircraft_configuration.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("date_variation".into(), records.iter().map(|r| r.date_variation.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("record_type".into(), records.iter().map(|r| r.record_type.to_string()).collect::<Vec<String>>()),
            polars::prelude::Series::new("record_serial_number".into(), records.iter().map(|r| r.record_serial_number.clone()).collect::<Vec<String>>()),
        ])
    }
}


impl SegmentRecords {
    pub fn get_columns(records: &[SegmentRecords]) -> PolarsResult<Vec<Series>> {
        Ok(vec![
            polars::prelude::Series::new("flight_designator".into(), records.iter().map(|r| r.flight_designator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("operational_suffix".into(), records.iter().map(|r| r.operational_suffix.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("airline_designator".into(), records.iter().map(|r| r.airline_designator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("flight_number".into(), records.iter().map(|r| r.flight_number.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("itinerary_variation_identifier".into(), records.iter().map(|r| r.itinerary_variation_identifier.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("leg_sequence_number".into(), records.iter().map(|r| r.leg_sequence_number.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("itinerary_variation_identifier_overflow".into(), records.iter().map(|r| r.itinerary_variation_identifier_overflow.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("board_point_indicator".into(), records.iter().map(|r| r.board_point_indicator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("off_point_indicator".into(), records.iter().map(|r| r.off_point_indicator.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("data_element_identifier".into(), records.iter().map(|r| r.data_element_identifier.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("board_point".into(), records.iter().map(|r| r.board_point.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("off_point".into(), records.iter().map(|r| r.off_point.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("data".into(), records.iter().map(|r| r.data.clone()).collect::<Vec<String>>()),
            polars::prelude::Series::new("record_type".into(), records.iter().map(|r| r.record_type.to_string()).collect::<Vec<String>>()),
            polars::prelude::Series::new("record_serial_number".into(), records.iter().map(|r| r.record_serial_number.clone()).collect::<Vec<String>>()),
        ])
    }
}
