use crate::utils::ssim_parser::{CarrierRecord, FlightLegRecord, SegmentRecords};
use polars::prelude::*;

/// Build a carrier DataFrame from carrier records.
/// Carriers are typically single records, so no parallelization needed.
fn build_carrier_dataframe(carrier: Option<&CarrierRecord>) -> PolarsResult<DataFrame> {
    let carriers: Vec<&CarrierRecord> = carrier.into_iter().collect();
    if !carriers.is_empty() {
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
        }
    } else {
        df! {
            "airline_designator" => Vec::<&str>::new(),
            "control_duplicate_indicator" => Vec::<&str>::new(),
            "time_mode" => Vec::<&str>::new(),
            "season" => Vec::<&str>::new(),
            "period_of_schedule_validity_from" => Vec::<&str>::new(),
            "period_of_schedule_validity_to" => Vec::<&str>::new(),
            "creation_date" => Vec::<&str>::new(),
            "title_of_data" => Vec::<&str>::new(),
            "release_date" => Vec::<&str>::new(),
            "schedule_status" => Vec::<&str>::new(),
            "general_information" => Vec::<&str>::new(),
            "in_flight_service_information" => Vec::<&str>::new(),
            "electronic_ticketing_information" => Vec::<&str>::new(),
            "creation_time" => Vec::<&str>::new(),
            "record_type" => Vec::<String>::new(),
            "record_serial_number" => Vec::<&str>::new(),
        }
    }
}

/// Build a flight DataFrame with parallel column construction.
/// Each column is built in parallel using rayon for better CPU utilization.
fn build_flight_dataframe(flights: &[FlightLegRecord<'_>]) -> PolarsResult<DataFrame> {
    if flights.is_empty() {
        return df! {
            "flight_designator" => Vec::<&str>::new(),
            "operational_suffix" => Vec::<&str>::new(),
            "airline_designator" => Vec::<&str>::new(),
            "control_duplicate_indicator" => Vec::<&str>::new(),
            "flight_number" => Vec::<&str>::new(),
            "itinerary_variation_identifier" => Vec::<&str>::new(),
            "leg_sequence_number" => Vec::<&str>::new(),
            "service_type" => Vec::<&str>::new(),
            "period_of_operation_from" => Vec::<&str>::new(),
            "period_of_operation_to" => Vec::<&str>::new(),
            "days_of_operation" => Vec::<&str>::new(),
            "frequency_rate" => Vec::<&str>::new(),
            "departure_station" => Vec::<&str>::new(),
            "scheduled_time_of_passenger_departure" => Vec::<&str>::new(),
            "scheduled_time_of_aircraft_departure" => Vec::<&str>::new(),
            "time_variation_departure" => Vec::<&str>::new(),
            "passenger_terminal_departure" => Vec::<&str>::new(),
            "arrival_station" => Vec::<&str>::new(),
            "scheduled_time_of_aircraft_arrival" => Vec::<&str>::new(),
            "scheduled_time_of_passenger_arrival" => Vec::<&str>::new(),
            "time_variation_arrival" => Vec::<&str>::new(),
            "passenger_terminal_arrival" => Vec::<&str>::new(),
            "aircraft_type" => Vec::<&str>::new(),
            "passenger_reservations_booking_designator" => Vec::<&str>::new(),
            "passenger_reservations_booking_modifier" => Vec::<&str>::new(),
            "meal_service_note" => Vec::<&str>::new(),
            "joint_operation_airline_designators" => Vec::<&str>::new(),
            "min_connecting_time_status_departure" => Vec::<&str>::new(),
            "min_connecting_time_status_arrival" => Vec::<&str>::new(),
            "secure_flight_indicator" => Vec::<&str>::new(),
            "itinerary_variation_identifier_overflow" => Vec::<&str>::new(),
            "aircraft_owner" => Vec::<&str>::new(),
            "cockpit_crew_employer" => Vec::<&str>::new(),
            "cabin_crew_employer" => Vec::<&str>::new(),
            "onward_flight" => Vec::<&str>::new(),
            "airline_designator2" => Vec::<&str>::new(),
            "flight_number2" => Vec::<&str>::new(),
            "aircraft_rotation_layover" => Vec::<&str>::new(),
            "operational_suffix2" => Vec::<&str>::new(),
            "flight_transit_layover" => Vec::<&str>::new(),
            "operating_airline_disclosure" => Vec::<&str>::new(),
            "traffic_restriction_code" => Vec::<&str>::new(),
            "traffic_restriction_code_leg_overflow_indicator" => Vec::<&str>::new(),
            "aircraft_configuration" => Vec::<&str>::new(),
            "date_variation" => Vec::<&str>::new(),
            "record_type" => Vec::<&str>::new(),
            "record_serial_number" => Vec::<&str>::new(),
        };
    }

    // Build columns in parallel groups using rayon::join
    // Each join runs two closures in parallel, returning a tuple of results
    
    // First batch: columns 1-8
    let ((c1, c2), (c3, c4)) = rayon::join(
        || rayon::join(
            || Column::new("flight_designator".into(), flights.iter().map(|r| r.flight_designator.as_ref()).collect::<Vec<_>>()),
            || Column::new("operational_suffix".into(), flights.iter().map(|r| r.operational_suffix.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("airline_designator".into(), flights.iter().map(|r| r.airline_designator.as_ref()).collect::<Vec<_>>()),
            || Column::new("control_duplicate_indicator".into(), flights.iter().map(|r| r.control_duplicate_indicator.as_ref()).collect::<Vec<_>>()),
        ),
    );
    
    let ((c5, c6), (c7, c8)) = rayon::join(
        || rayon::join(
            || Column::new("flight_number".into(), flights.iter().map(|r| r.flight_number.as_ref()).collect::<Vec<_>>()),
            || Column::new("itinerary_variation_identifier".into(), flights.iter().map(|r| r.itinerary_variation_identifier.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("leg_sequence_number".into(), flights.iter().map(|r| r.leg_sequence_number.as_ref()).collect::<Vec<_>>()),
            || Column::new("service_type".into(), flights.iter().map(|r| r.service_type.as_ref()).collect::<Vec<_>>()),
        ),
    );

    // Second batch: columns 9-16
    let ((c9, c10), (c11, c12)) = rayon::join(
        || rayon::join(
            || Column::new("period_of_operation_from".into(), flights.iter().map(|r| r.period_of_operation_from.as_ref()).collect::<Vec<_>>()),
            || Column::new("period_of_operation_to".into(), flights.iter().map(|r| r.period_of_operation_to.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("days_of_operation".into(), flights.iter().map(|r| r.days_of_operation.as_ref()).collect::<Vec<_>>()),
            || Column::new("frequency_rate".into(), flights.iter().map(|r| r.frequency_rate.as_ref()).collect::<Vec<_>>()),
        ),
    );
    
    let ((c13, c14), (c15, c16)) = rayon::join(
        || rayon::join(
            || Column::new("departure_station".into(), flights.iter().map(|r| r.departure_station.as_ref()).collect::<Vec<_>>()),
            || Column::new("scheduled_time_of_passenger_departure".into(), flights.iter().map(|r| r.scheduled_time_of_passenger_departure.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("scheduled_time_of_aircraft_departure".into(), flights.iter().map(|r| r.scheduled_time_of_aircraft_departure.as_ref()).collect::<Vec<_>>()),
            || Column::new("time_variation_departure".into(), flights.iter().map(|r| r.time_variation_departure.as_ref()).collect::<Vec<_>>()),
        ),
    );

    // Third batch: columns 17-24
    let ((c17, c18), (c19, c20)) = rayon::join(
        || rayon::join(
            || Column::new("passenger_terminal_departure".into(), flights.iter().map(|r| r.passenger_terminal_departure.as_ref()).collect::<Vec<_>>()),
            || Column::new("arrival_station".into(), flights.iter().map(|r| r.arrival_station.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("scheduled_time_of_aircraft_arrival".into(), flights.iter().map(|r| r.scheduled_time_of_aircraft_arrival.as_ref()).collect::<Vec<_>>()),
            || Column::new("scheduled_time_of_passenger_arrival".into(), flights.iter().map(|r| r.scheduled_time_of_passenger_arrival.as_ref()).collect::<Vec<_>>()),
        ),
    );
    
    let ((c21, c22), (c23, c24)) = rayon::join(
        || rayon::join(
            || Column::new("time_variation_arrival".into(), flights.iter().map(|r| r.time_variation_arrival.as_ref()).collect::<Vec<_>>()),
            || Column::new("passenger_terminal_arrival".into(), flights.iter().map(|r| r.passenger_terminal_arrival.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("aircraft_type".into(), flights.iter().map(|r| r.aircraft_type.as_ref()).collect::<Vec<_>>()),
            || Column::new("passenger_reservations_booking_designator".into(), flights.iter().map(|r| r.passenger_reservations_booking_designator.as_ref()).collect::<Vec<_>>()),
        ),
    );

    // Fourth batch: columns 25-32
    let ((c25, c26), (c27, c28)) = rayon::join(
        || rayon::join(
            || Column::new("passenger_reservations_booking_modifier".into(), flights.iter().map(|r| r.passenger_reservations_booking_modifier.as_ref()).collect::<Vec<_>>()),
            || Column::new("meal_service_note".into(), flights.iter().map(|r| r.meal_service_note.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("joint_operation_airline_designators".into(), flights.iter().map(|r| r.joint_operation_airline_designators.as_ref()).collect::<Vec<_>>()),
            || Column::new("min_connecting_time_status_departure".into(), flights.iter().map(|r| r.min_connecting_time_status_departure.as_ref()).collect::<Vec<_>>()),
        ),
    );
    
    let ((c29, c30), (c31, c32)) = rayon::join(
        || rayon::join(
            || Column::new("min_connecting_time_status_arrival".into(), flights.iter().map(|r| r.min_connecting_time_status_arrival.as_ref()).collect::<Vec<_>>()),
            || Column::new("secure_flight_indicator".into(), flights.iter().map(|r| r.secure_flight_indicator.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("itinerary_variation_identifier_overflow".into(), flights.iter().map(|r| r.itinerary_variation_identifier_overflow.as_ref()).collect::<Vec<_>>()),
            || Column::new("aircraft_owner".into(), flights.iter().map(|r| r.aircraft_owner.as_ref()).collect::<Vec<_>>()),
        ),
    );

    // Fifth batch: columns 33-40
    let ((c33, c34), (c35, c36)) = rayon::join(
        || rayon::join(
            || Column::new("cockpit_crew_employer".into(), flights.iter().map(|r| r.cockpit_crew_employer.as_ref()).collect::<Vec<_>>()),
            || Column::new("cabin_crew_employer".into(), flights.iter().map(|r| r.cabin_crew_employer.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("onward_flight".into(), flights.iter().map(|r| r.onward_flight.as_ref()).collect::<Vec<_>>()),
            || Column::new("airline_designator2".into(), flights.iter().map(|r| r.airline_designator2.as_ref()).collect::<Vec<_>>()),
        ),
    );
    
    let ((c37, c38), (c39, c40)) = rayon::join(
        || rayon::join(
            || Column::new("flight_number2".into(), flights.iter().map(|r| r.flight_number2.as_ref()).collect::<Vec<_>>()),
            || Column::new("aircraft_rotation_layover".into(), flights.iter().map(|r| r.aircraft_rotation_layover.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("operational_suffix2".into(), flights.iter().map(|r| r.operational_suffix2.as_ref()).collect::<Vec<_>>()),
            || Column::new("flight_transit_layover".into(), flights.iter().map(|r| r.flight_transit_layover.as_ref()).collect::<Vec<_>>()),
        ),
    );

    // Sixth batch: columns 41-47
    let ((c41, c42), (c43, c44)) = rayon::join(
        || rayon::join(
            || Column::new("operating_airline_disclosure".into(), flights.iter().map(|r| r.operating_airline_disclosure.as_ref()).collect::<Vec<_>>()),
            || Column::new("traffic_restriction_code".into(), flights.iter().map(|r| r.traffic_restriction_code.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("traffic_restriction_code_leg_overflow_indicator".into(), flights.iter().map(|r| r.traffic_restriction_code_leg_overflow_indicator.as_ref()).collect::<Vec<_>>()),
            || Column::new("aircraft_configuration".into(), flights.iter().map(|r| r.aircraft_configuration.as_ref()).collect::<Vec<_>>()),
        ),
    );
    
    let ((c45, c46), c47) = rayon::join(
        || rayon::join(
            || Column::new("date_variation".into(), flights.iter().map(|r| r.date_variation.as_ref()).collect::<Vec<_>>()),
            || Column::new("record_type".into(), flights.iter().map(|r| r.record_type.to_string()).collect::<Vec<_>>()),
        ),
        || Column::new("record_serial_number".into(), flights.iter().map(|r| r.record_serial_number.as_ref()).collect::<Vec<_>>()),
    );

    // Assemble all columns in order
    let columns = vec![
        c1, c2, c3, c4, c5, c6, c7, c8,
        c9, c10, c11, c12, c13, c14, c15, c16,
        c17, c18, c19, c20, c21, c22, c23, c24,
        c25, c26, c27, c28, c29, c30, c31, c32,
        c33, c34, c35, c36, c37, c38, c39, c40,
        c41, c42, c43, c44, c45, c46, c47,
    ];

    DataFrame::new(columns)
}

/// Build a segment DataFrame with parallel column construction.
fn build_segment_dataframe(segments: &[SegmentRecords<'_>]) -> PolarsResult<DataFrame> {
    if segments.is_empty() {
        return df! {
            "flight_designator" => Vec::<&str>::new(),
            "operational_suffix" => Vec::<&str>::new(),
            "airline_designator" => Vec::<&str>::new(),
            "control_duplicate_indicator" => Vec::<&str>::new(),
            "flight_number" => Vec::<&str>::new(),
            "itinerary_variation_identifier" => Vec::<&str>::new(),
            "leg_sequence_number" => Vec::<&str>::new(),
            "itinerary_variation_identifier_overflow" => Vec::<&str>::new(),
            "board_point_indicator" => Vec::<&str>::new(),
            "off_point_indicator" => Vec::<&str>::new(),
            "data_element_identifier" => Vec::<&str>::new(),
            "board_point" => Vec::<&str>::new(),
            "off_point" => Vec::<&str>::new(),
            "data" => Vec::<&str>::new(),
            "record_type" => Vec::<String>::new(),
            "record_serial_number" => Vec::<&str>::new(),
        };
    }

    // Build segment columns in parallel using rayon::join
    let ((s1, s2), (s3, s4)) = rayon::join(
        || rayon::join(
            || Column::new("flight_designator".into(), segments.iter().map(|r| r.flight_designator.as_ref()).collect::<Vec<_>>()),
            || Column::new("operational_suffix".into(), segments.iter().map(|r| r.operational_suffix.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("airline_designator".into(), segments.iter().map(|r| r.airline_designator.as_ref()).collect::<Vec<_>>()),
            || Column::new("control_duplicate_indicator".into(), segments.iter().map(|r| r.control_duplicate_indicator.as_ref()).collect::<Vec<_>>()),
        ),
    );
    
    let ((s5, s6), (s7, s8)) = rayon::join(
        || rayon::join(
            || Column::new("flight_number".into(), segments.iter().map(|r| r.flight_number.as_ref()).collect::<Vec<_>>()),
            || Column::new("itinerary_variation_identifier".into(), segments.iter().map(|r| r.itinerary_variation_identifier.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("leg_sequence_number".into(), segments.iter().map(|r| r.leg_sequence_number.as_ref()).collect::<Vec<_>>()),
            || Column::new("itinerary_variation_identifier_overflow".into(), segments.iter().map(|r| r.itinerary_variation_identifier_overflow.as_ref()).collect::<Vec<_>>()),
        ),
    );

    let ((s9, s10), (s11, s12)) = rayon::join(
        || rayon::join(
            || Column::new("board_point_indicator".into(), segments.iter().map(|r| r.board_point_indicator.as_ref()).collect::<Vec<_>>()),
            || Column::new("off_point_indicator".into(), segments.iter().map(|r| r.off_point_indicator.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("data_element_identifier".into(), segments.iter().map(|r| r.data_element_identifier.as_ref()).collect::<Vec<_>>()),
            || Column::new("board_point".into(), segments.iter().map(|r| r.board_point.as_ref()).collect::<Vec<_>>()),
        ),
    );

    let ((s13, s14), (s15, s16)) = rayon::join(
        || rayon::join(
            || Column::new("off_point".into(), segments.iter().map(|r| r.off_point.as_ref()).collect::<Vec<_>>()),
            || Column::new("data".into(), segments.iter().map(|r| r.data.as_ref()).collect::<Vec<_>>()),
        ),
        || rayon::join(
            || Column::new("record_type".into(), segments.iter().map(|r| r.record_type.to_string()).collect::<Vec<_>>()),
            || Column::new("record_serial_number".into(), segments.iter().map(|r| r.record_serial_number.as_ref()).collect::<Vec<_>>()),
        ),
    );

    let columns = vec![
        s1, s2, s3, s4, s5, s6, s7, s8,
        s9, s10, s11, s12, s13, s14, s15, s16,
    ];

    DataFrame::new(columns)
}

pub fn convert_to_dataframes(
    carrier: Option<&CarrierRecord>,
    flights: Vec<FlightLegRecord<'_>>,
    segments: Vec<SegmentRecords<'_>>,
) -> PolarsResult<(DataFrame, DataFrame, DataFrame)> {
    // Build all three DataFrames in parallel
    let (carrier_df, (flight_df, segment_df)) = rayon::join(
        || build_carrier_dataframe(carrier),
        || {
            rayon::join(
                || build_flight_dataframe(&flights),
                || build_segment_dataframe(&segments),
            )
        },
    );

    Ok((carrier_df?, flight_df?, segment_df?))
}
