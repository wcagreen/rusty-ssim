pub use crate::records::carrier_record::CarrierRecord;
pub use crate::records::flight_leg_records::FlightLegRecord;
pub use crate::records::segment_records::SegmentRecords;
use std::borrow::Cow;

pub fn parse_flight_record_legs(
    line: &str,
    persistent_carriers: &CarrierRecord,
) -> Option<FlightLegRecord> {
    let control_duplicate_indicator = persistent_carriers.control_duplicate_indicator.clone();

    Some(FlightLegRecord {
        flight_designator: Cow::Owned(format!(
            "{}_{}{}{} {} {}",
            &line[2..5],
            control_duplicate_indicator,
            &line[5..9],
            &line[1..2],
            &line[9..11],
            &line[127..128]
        )),
        operational_suffix: Cow::Owned(line[1..2].to_string()),
        airline_designator: Cow::Owned(line[2..5].to_string()),
        control_duplicate_indicator: Cow::Owned(control_duplicate_indicator),
        flight_number: Cow::Owned(line[5..9].to_string()),
        itinerary_variation_identifier: Cow::Owned(line[9..11].to_string()),
        leg_sequence_number: Cow::Owned(line[11..13].to_string()),
        service_type: Cow::Owned(line[13..14].to_string()),
        period_of_operation_from: Cow::Owned(line[14..21].to_string()),
        period_of_operation_to: Cow::Owned(line[21..28].to_string()),
        days_of_operation: Cow::Owned(line[28..35].to_string()),
        frequency_rate: Cow::Owned(line[35..36].to_string()),
        departure_station: Cow::Owned(line[36..39].to_string()),
        scheduled_time_of_passenger_departure: Cow::Owned(line[39..43].to_string()),
        scheduled_time_of_aircraft_departure: Cow::Owned(line[43..47].to_string()),
        time_variation_departure: Cow::Owned(line[47..52].to_string()),
        passenger_terminal_departure: Cow::Owned(line[52..54].to_string()),
        arrival_station: Cow::Owned(line[54..57].to_string()),
        scheduled_time_of_aircraft_arrival: Cow::Owned(line[57..61].to_string()),
        scheduled_time_of_passenger_arrival: Cow::Owned(line[61..65].to_string()),
        time_variation_arrival: Cow::Owned(line[65..70].to_string()),
        passenger_terminal_arrival: Cow::Owned(line[70..72].to_string()),
        aircraft_type: Cow::Owned(line[72..75].to_string()),
        passenger_reservations_booking_designator: Cow::Owned(line[75..95].trim().to_string()),
        passenger_reservations_booking_modifier: Cow::Owned(line[95..100].to_string()),
        meal_service_note: Cow::Owned(line[100..110].to_string()),
        joint_operation_airline_designators: Cow::Owned(line[110..119].to_string()),
        min_connecting_time_status_departure: Cow::Owned(line[119..120].to_string()),
        min_connecting_time_status_arrival: Cow::Owned(line[120..121].to_string()),
        secure_flight_indicator: Cow::Owned(line[121..122].to_string()),
        itinerary_variation_identifier_overflow: Cow::Owned(line[127..128].to_string()),
        aircraft_owner: Cow::Owned(line[128..131].trim().to_string()),
        cockpit_crew_employer: Cow::Owned(line[131..134].trim().to_string()),
        cabin_crew_employer: Cow::Owned(line[134..137].trim().to_string()),
        onward_flight: Cow::Owned(line[137..146].trim().to_string()),
        airline_designator2: Cow::Owned(line[137..140].to_string()),
        flight_number2: Cow::Owned(line[140..144].to_string()),
        aircraft_rotation_layover: Cow::Owned(line[144..145].to_string()),
        operational_suffix2: Cow::Owned(line[145..146].to_string()),
        flight_transit_layover: Cow::Owned(line[147..148].to_string()),
        operating_airline_disclosure: Cow::Owned(line[148..149].to_string()),
        traffic_restriction_code: Cow::Owned(line[149..160].to_string()),
        traffic_restriction_code_leg_overflow_indicator: Cow::Owned(line[160..161].to_string()),
        aircraft_configuration: Cow::Owned(line[172..192].to_string()),
        date_variation: Cow::Owned(line[192..194].to_string()),
        record_serial_number: Cow::Owned(line[194..200].to_string()),
        record_type: line.chars().next().unwrap(),
    })
}

pub fn parse_segment_record(
    line: &str,
    persistent_carriers: &CarrierRecord,
) -> Option<SegmentRecords> {
    let control_duplicate_indicator = persistent_carriers.control_duplicate_indicator.clone();
    Some(SegmentRecords {
        flight_designator: Cow::Owned(format!(
            "{}_{}{}{} {} {}",
            &line[2..5],
            control_duplicate_indicator,
            &line[5..9],
            &line[1..2],
            &line[9..11],
            &line[127..128]
        )),
        operational_suffix: Cow::Owned(line[1..2].to_string()),
        airline_designator: Cow::Owned(line[2..5].to_string()),
        control_duplicate_indicator: Cow::Owned(control_duplicate_indicator),
        flight_number: Cow::Owned(line[5..9].to_string()),
        itinerary_variation_identifier: Cow::Owned(line[9..11].to_string()),
        leg_sequence_number: Cow::Owned(line[11..13].to_string()),
        service_type: Cow::Owned(line[13..14].to_string()),
        itinerary_variation_identifier_overflow: Cow::Owned(line[27..28].to_string()),
        board_point_indicator: Cow::Owned(line[28..29].to_string()),
        off_point_indicator: Cow::Owned(line[29..30].to_string()),
        data_element_identifier: Cow::Owned(line[30..33].to_string()),
        board_point: Cow::Owned(line[33..36].to_string()),
        off_point: Cow::Owned(line[36..39].to_string()),
        data: Cow::Owned(line[39..194].trim().to_string()),
        record_serial_number: Cow::Owned(line[194..200].to_string()),
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


