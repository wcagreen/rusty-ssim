pub use crate::records::carrier_record::CarrierRecord;
pub use crate::records::flight_leg_records::FlightLegRecord;
pub use crate::records::segment_records::SegmentRecords;
use std::borrow::Cow;

pub fn parse_flight_record_legs<'a>(
    line: &'a str,
    persistent_carriers: &CarrierRecord,
) -> Option<FlightLegRecord<'a>> {
    // These must be Owned: format! creates new string, carrier has different lifetime
    let flight_designator = Cow::Owned(format!(
        "{}_{}{}{} {} {}",
        &line[2..5],
        &persistent_carriers.control_duplicate_indicator,
        &line[5..9],
        &line[1..2],
        &line[9..11],
        &line[127..128]
    ));
    let control_duplicate_indicator = Cow::Owned(
        persistent_carriers.control_duplicate_indicator.clone()
    );

    Some(FlightLegRecord {
        flight_designator,
        operational_suffix: Cow::Borrowed(&line[1..2]),
        airline_designator: Cow::Borrowed(&line[2..5]),
        control_duplicate_indicator,
        flight_number: Cow::Borrowed(&line[5..9]),
        itinerary_variation_identifier: Cow::Borrowed(&line[9..11]),
        leg_sequence_number: Cow::Borrowed(&line[11..13]),
        service_type: Cow::Borrowed(&line[13..14]),
        period_of_operation_from: Cow::Borrowed(&line[14..21]),
        period_of_operation_to: Cow::Borrowed(&line[21..28]),
        days_of_operation: Cow::Borrowed(&line[28..35]),
        frequency_rate: Cow::Borrowed(&line[35..36]),
        departure_station: Cow::Borrowed(&line[36..39]),
        scheduled_time_of_passenger_departure: Cow::Borrowed(&line[39..43]),
        scheduled_time_of_aircraft_departure: Cow::Borrowed(&line[43..47]),
        time_variation_departure: Cow::Borrowed(&line[47..52]),
        passenger_terminal_departure: Cow::Borrowed(&line[52..54]),
        arrival_station: Cow::Borrowed(&line[54..57]),
        scheduled_time_of_aircraft_arrival: Cow::Borrowed(&line[57..61]),
        scheduled_time_of_passenger_arrival: Cow::Borrowed(&line[61..65]),
        time_variation_arrival: Cow::Borrowed(&line[65..70]),
        passenger_terminal_arrival: Cow::Borrowed(&line[70..72]),
        aircraft_type: Cow::Borrowed(&line[72..75]),
        // trim() returns a new slice, so we need Owned for trimmed fields
        passenger_reservations_booking_designator: Cow::Owned(line[75..95].trim().to_string()),
        passenger_reservations_booking_modifier: Cow::Borrowed(&line[95..100]),
        meal_service_note: Cow::Borrowed(&line[100..110]),
        joint_operation_airline_designators: Cow::Borrowed(&line[110..119]),
        min_connecting_time_status_departure: Cow::Borrowed(&line[119..120]),
        min_connecting_time_status_arrival: Cow::Borrowed(&line[120..121]),
        secure_flight_indicator: Cow::Borrowed(&line[121..122]),
        itinerary_variation_identifier_overflow: Cow::Borrowed(&line[127..128]),
        // trim() requires Owned
        aircraft_owner: Cow::Owned(line[128..131].trim().to_string()),
        cockpit_crew_employer: Cow::Owned(line[131..134].trim().to_string()),
        cabin_crew_employer: Cow::Owned(line[134..137].trim().to_string()),
        onward_flight: Cow::Owned(line[137..146].trim().to_string()),
        airline_designator2: Cow::Borrowed(&line[137..140]),
        flight_number2: Cow::Borrowed(&line[140..144]),
        aircraft_rotation_layover: Cow::Borrowed(&line[144..145]),
        operational_suffix2: Cow::Borrowed(&line[145..146]),
        flight_transit_layover: Cow::Borrowed(&line[147..148]),
        operating_airline_disclosure: Cow::Borrowed(&line[148..149]),
        traffic_restriction_code: Cow::Borrowed(&line[149..160]),
        traffic_restriction_code_leg_overflow_indicator: Cow::Borrowed(&line[160..161]),
        aircraft_configuration: Cow::Borrowed(&line[172..192]),
        date_variation: Cow::Borrowed(&line[192..194]),
        record_serial_number: Cow::Borrowed(&line[194..200]),
        record_type: line.chars().next().unwrap(),
    })
}

pub fn parse_segment_record<'a>(
    line: &'a str,
    persistent_carriers: &CarrierRecord,
) -> Option<SegmentRecords<'a>> {
    // Must be Owned: format! and carrier lifetime
    let flight_designator = Cow::Owned(format!(
        "{}_{}{}{} {} {}",
        &line[2..5],
        &persistent_carriers.control_duplicate_indicator,
        &line[5..9],
        &line[1..2],
        &line[9..11],
        &line[127..128]
    ));
    let control_duplicate_indicator = Cow::Owned(
        persistent_carriers.control_duplicate_indicator.clone()
    );

    Some(SegmentRecords {
        flight_designator,
        operational_suffix: Cow::Borrowed(&line[1..2]),
        airline_designator: Cow::Borrowed(&line[2..5]),
        control_duplicate_indicator,
        flight_number: Cow::Borrowed(&line[5..9]),
        itinerary_variation_identifier: Cow::Borrowed(&line[9..11]),
        leg_sequence_number: Cow::Borrowed(&line[11..13]),
        service_type: Cow::Borrowed(&line[13..14]),
        itinerary_variation_identifier_overflow: Cow::Borrowed(&line[27..28]),
        board_point_indicator: Cow::Borrowed(&line[28..29]),
        off_point_indicator: Cow::Borrowed(&line[29..30]),
        data_element_identifier: Cow::Borrowed(&line[30..33]),
        board_point: Cow::Borrowed(&line[33..36]),
        off_point: Cow::Borrowed(&line[36..39]),
        // trim() requires Owned
        data: Cow::Owned(line[39..194].trim().to_string()),
        record_serial_number: Cow::Borrowed(&line[194..200]),
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


