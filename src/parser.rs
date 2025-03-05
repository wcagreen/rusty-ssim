pub use crate::records::flight_leg_records::FlightLegRecord;
pub use crate::records::segment_records::SegmentRecords;
pub use crate::records::carrier_record::CarrierRecord;


pub fn parse_flight_record_legs(line: &str) -> Option<FlightLegRecord> {
    Some(FlightLegRecord {
        flight_designator: format!(
            "{}{}{} {} {}",
            &line[2..5], &line[5..9], &line[1..2], &line[9..11], &line[127..128]
        ),
        operational_suffix: &line[1..2],
        airline_designator: &line[2..5],
        flight_number: &line[5..9],
        itinerary_variation_identifier: &line[9..11],
        leg_sequence_number: &line[11..13],
        service_type: &line[13..14],
        period_of_operation_from: &line[14..21],
        period_of_operation_to: &line[21..28],
        days_of_operation: &line[28..35],
        frequency_rate: &line[35..36],
        departure_station: &line[36..39],
        scheduled_time_of_passenger_departure: &line[39..43],
        scheduled_time_of_aircraft_departure: &line[43..47],
        time_variation_departure: &line[47..52],
        passenger_terminal_departure: &line[52..54],
        arrival_station: &line[54..57],
        scheduled_time_of_aircraft_arrival: &line[57..61],
        scheduled_time_of_passenger_arrival: &line[61..65],
        time_variation_arrival: &line[65..70],
        passenger_terminal_arrival: &line[70..72],
        aircraft_type: &line[72..75],
        passenger_reservations_booking_designator: &line[75..95].trim(),
        passenger_reservations_booking_modifier: &line[95..100],
        meal_service_note: &line[100..110],
        joint_operation_airline_designators: &line[110..119],
        min_connecting_time_status_departure: &line[119..120],
        min_connecting_time_status_arrival: &line[120..121],
        secure_flight_indicator: &line[121..122],
        itinerary_variation_identifier_overflow: &line[127..128],
        aircraft_owner: &line[128..131].trim(),
        cockpit_crew_employer: &line[131..134].trim(),
        cabin_crew_employer: &line[134..137].trim(),
        onward_flight: &line[137..146].trim(),
        airline_designator2: &line[137..140],
        flight_number2: &line[140..144],
        aircraft_rotation_layover: &line[144..145],
        operational_suffix2: &line[145..146],
        flight_transit_layover: &line[147..148],
        operating_airline_disclosure: &line[148..149],
        traffic_restriction_code: &line[149..160],
        traffic_restriction_code_leg_overflow_indicator: &line[160..161],
        aircraft_configuration: &line[172..192],
        date_variation: &line[192..194],
        record_serial_number: &line[194..200],
        record_type: line.chars().next().unwrap(),
    })
}


pub fn parse_segment_record(line: &str) -> Option<SegmentRecords>{
    Some(SegmentRecords {
        flight_designator: format!("{}{}{} {} {}", &line[2..5], &line[5..9], &line[1..2], &line[9..11], &line[27..28]),
        operational_suffix: &line[1..2],
        airline_designator: &line[2..5],
        flight_number: &line[5..9],
        itinerary_variation_identifier: &line[9..11],
        leg_sequence_number: &line[11..13],
        service_type: &line[13..14],
        itinerary_variation_identifier_overflow: &line[27..28],
        board_point_indicator: &line[28..29],
        off_point_indicator: &line[29..30],
        data_element_identifier: &line[30..33],
        board_point: &line[33..36],
        off_point: &line[36..39],
        data: &line[39..194].trim(),
        record_serial_number: &line[194..200],
        record_type: line.chars().next().unwrap(),
    })
}

pub fn parse_carrier_record(line: &str) -> Option<CarrierRecord>{
    Some(CarrierRecord {
        airline_designator: &line[2..5],
        control_duplicate_indicator: &line[107..108],
        time_mode: &line[1..2],
        season: &line[10..13],
        period_of_schedule_validity_from: &line[14..21],
        period_of_schedule_validity_to: &line[21..28],
        creation_date: &line[28..35],
        title_of_data: &line[35..64].trim(),
        release_date: &line[64..71],
        schedule_status: &line[71..72],
        general_information: &line[108..169].trim(),
        in_flight_service_information: &line[169..188].trim_start(),
        electronic_ticketing_information: &line[188..190],
        creation_time: &line[190..194],
        record_type: line.chars().next().unwrap(),
        record_serial_number: &line[194..200],
    })
}
