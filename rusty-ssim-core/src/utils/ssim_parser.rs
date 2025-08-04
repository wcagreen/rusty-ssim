pub use crate::records::carrier_record::CarrierRecord;
pub use crate::records::flight_leg_records::FlightLegRecord;
pub use crate::records::segment_records::SegmentRecords;

pub fn parse_flight_record_legs(
    line: &str,
    persistent_carriers: &[CarrierRecord],
) -> Option<FlightLegRecord> {
    let airline_designator = &line[2..5];
    let control_duplicate_indicator =
        get_control_duplicate_indicator_for_airline(airline_designator, persistent_carriers);

    Some(FlightLegRecord {
        flight_designator: format!(
            "{}_{}{}{} {} {}",
            &line[2..5],
            control_duplicate_indicator,
            &line[5..9],
            &line[1..2],
            &line[9..11],
            &line[127..128]
        ),
        operational_suffix: line[1..2].to_string(),
        airline_designator: line[2..5].to_string(),
        control_duplicate_indicator,
        flight_number: line[5..9].to_string(),
        itinerary_variation_identifier: line[9..11].to_string(),
        leg_sequence_number: line[11..13].to_string(),
        service_type: line[13..14].to_string(),
        period_of_operation_from: line[14..21].to_string(),
        period_of_operation_to: line[21..28].to_string(),
        days_of_operation: line[28..35].to_string(),
        frequency_rate: line[35..36].to_string(),
        departure_station: line[36..39].to_string(),
        scheduled_time_of_passenger_departure: line[39..43].to_string(),
        scheduled_time_of_aircraft_departure: line[43..47].to_string(),
        time_variation_departure: line[47..52].to_string(),
        passenger_terminal_departure: line[52..54].to_string(),
        arrival_station: line[54..57].to_string(),
        scheduled_time_of_aircraft_arrival: line[57..61].to_string(),
        scheduled_time_of_passenger_arrival: line[61..65].to_string(),
        time_variation_arrival: line[65..70].to_string(),
        passenger_terminal_arrival: line[70..72].to_string(),
        aircraft_type: line[72..75].to_string(),
        passenger_reservations_booking_designator: line[75..95].trim().to_string(),
        passenger_reservations_booking_modifier: line[95..100].to_string(),
        meal_service_note: line[100..110].to_string(),
        joint_operation_airline_designators: line[110..119].to_string(),
        min_connecting_time_status_departure: line[119..120].to_string(),
        min_connecting_time_status_arrival: line[120..121].to_string(),
        secure_flight_indicator: line[121..122].to_string(),
        itinerary_variation_identifier_overflow: line[127..128].to_string(),
        aircraft_owner: line[128..131].trim().to_string(),
        cockpit_crew_employer: line[131..134].trim().to_string(),
        cabin_crew_employer: line[134..137].trim().to_string(),
        onward_flight: line[137..146].trim().to_string(),
        airline_designator2: line[137..140].to_string(),
        flight_number2: line[140..144].to_string(),
        aircraft_rotation_layover: line[144..145].to_string(),
        operational_suffix2: line[145..146].to_string(),
        flight_transit_layover: line[147..148].to_string(),
        operating_airline_disclosure: line[148..149].to_string(),
        traffic_restriction_code: line[149..160].to_string(),
        traffic_restriction_code_leg_overflow_indicator: line[160..161].to_string(),
        aircraft_configuration: line[172..192].to_string(),
        date_variation: line[192..194].to_string(),
        record_serial_number: line[194..200].to_string(),
        record_type: line.chars().next().unwrap(),
    })
}

pub fn parse_segment_record(
    line: &str,
    persistent_carriers: &[CarrierRecord],
) -> Option<SegmentRecords> {
    let airline_designator = &line[2..5];
    let control_duplicate_indicator =
        get_control_duplicate_indicator_for_airline(airline_designator, persistent_carriers);

    Some(SegmentRecords {
        flight_designator: format!(
            "{}_{}{}{} {} {}",
            &line[2..5],
            control_duplicate_indicator,
            &line[5..9],
            &line[1..2],
            &line[9..11],
            &line[127..128]
        ),
        operational_suffix: line[1..2].to_string(),
        airline_designator: line[2..5].to_string(),
        control_duplicate_indicator,
        flight_number: line[5..9].to_string(),
        itinerary_variation_identifier: line[9..11].to_string(),
        leg_sequence_number: line[11..13].to_string(),
        service_type: line[13..14].to_string(),
        itinerary_variation_identifier_overflow: line[27..28].to_string(),
        board_point_indicator: line[28..29].to_string(),
        off_point_indicator: line[29..30].to_string(),
        data_element_identifier: line[30..33].to_string(),
        board_point: line[33..36].to_string(),
        off_point: line[36..39].to_string(),
        data: line[39..194].trim().to_string(),
        record_serial_number: line[194..200].to_string(),
        record_type: line.chars().next().unwrap(),
    })
}

pub fn parse_carrier_record(line: &str) -> Option<CarrierRecord> {
    Some(CarrierRecord {
        airline_designator: line[2..5].to_string(),
        control_duplicate_indicator: line[107..108].to_string(),
        time_mode: line[1..2].to_string(),
        season: line[10..13].to_string(),
        period_of_schedule_validity_from: line[14..21].to_string(),
        period_of_schedule_validity_to: line[21..28].to_string(),
        creation_date: line[28..35].to_string(),
        title_of_data: line[35..64].trim().to_string(),
        release_date: line[64..71].to_string(),
        schedule_status: line[71..72].to_string(),
        general_information: line[108..169].trim().to_string(),
        in_flight_service_information: line[169..188].trim_start().to_string(),
        electronic_ticketing_information: line[188..190].to_string(),
        creation_time: line[190..194].to_string(),
        record_type: line.chars().next().unwrap(),
        record_serial_number: line[194..200].to_string(),
    })
}

/// Helper function to get the appropriate control duplicate indicator for an airline
/// from the persistent carriers context
fn get_control_duplicate_indicator_for_airline(
    airline_designator: &str,
    persistent_carriers: &[CarrierRecord],
) -> String {
    for carrier in persistent_carriers {
        if carrier.airline_designator == airline_designator {
            return carrier.control_duplicate_indicator.clone();
        }
    }

    String::new()
}
