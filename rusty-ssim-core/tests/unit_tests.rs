use rusty_ssim_core::records::carrier_record::CarrierRecord;
use rusty_ssim_core::utils::ssim_parser::{
    parse_carrier_record, parse_flight_record_legs, parse_segment_record,
};

#[cfg(test)]
mod parser_tests {
    use super::*;
    #[test]
    fn test_parse_carrier_record_valid() {
        let line = "2UXX  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002";

        let result = parse_carrier_record(line);
        assert!(result.is_some());

        let record = result.unwrap();
        assert_eq!(record.airline_designator, "XX ");
        assert_eq!(record.control_duplicate_indicator, " ");
        assert_eq!(record.time_mode, "U");
        assert_eq!(record.season, "S18");
        assert_eq!(record.period_of_schedule_validity_from, "25MAR18");
        assert_eq!(record.period_of_schedule_validity_to, "27OCT18");
        assert_eq!(record.creation_date, "13OCT17");
        assert_eq!(record.electronic_ticketing_information, "  ");
        assert_eq!(record.record_type, '2');
        assert_eq!(record.record_serial_number, "000002");
    }

    #[test]
    fn test_parse_flight_record_valid() {
        let line = "3 XX   120102P28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003";
        let carriers = vec![CarrierRecord {
            airline_designator: "XX ".to_string(),
            control_duplicate_indicator: " ".to_string(),
            time_mode: "U".to_string(),
            season: "S18".to_string(),
            period_of_schedule_validity_from: "25MAR18".to_string(),
            period_of_schedule_validity_to: "25MAR18".to_string(),
            creation_date: "13OCT17".to_string(),
            title_of_data: "SECRET AIRLINE".to_string(),
            release_date: "        ".to_string(),
            schedule_status: " ".to_string(),
            general_information: "".to_string(),
            in_flight_service_information: " ".to_string(),
            electronic_ticketing_information: "  ".to_string(),
            creation_time: "    ".to_string(),
            record_type: '2',
            record_serial_number: "000002".to_string(),
        }];

        let result = parse_flight_record_legs(line, &carriers);
        assert!(result.is_some());

        let record = result.unwrap();
        assert_eq!(record.operational_suffix, " ");
        assert_eq!(record.airline_designator, "XX ");
        assert_eq!(record.control_duplicate_indicator, " ");
        assert_eq!(record.flight_number, "  12");
        assert_eq!(record.itinerary_variation_identifier, "01");
        assert_eq!(record.leg_sequence_number, "02");
        assert_eq!(record.service_type, "P");
        assert_eq!(record.period_of_operation_from, "28MAR18");
        assert_eq!(record.period_of_operation_to, "03APR18");
        assert_eq!(record.days_of_operation, " 2     ");
        assert_eq!(record.frequency_rate, " ");
        assert_eq!(record.departure_station, "KEF");
        assert_eq!(record.scheduled_time_of_passenger_departure, "0510");
        assert_eq!(record.scheduled_time_of_aircraft_departure, "0510");
        assert_eq!(record.time_variation_departure, "+0000");
        assert_eq!(record.passenger_terminal_departure, "  ");
        assert_eq!(record.arrival_station, "AMS");
        assert_eq!(record.scheduled_time_of_aircraft_arrival, "0800");
        assert_eq!(record.scheduled_time_of_passenger_arrival, "0800");
        assert_eq!(record.time_variation_arrival, "+0200");
        assert_eq!(record.passenger_terminal_arrival, "  ");
        assert_eq!(record.aircraft_type, "73H");
        assert_eq!(
            record.passenger_reservations_booking_designator,
            "Y"
        );
        assert_eq!(record.passenger_reservations_booking_modifier, "     ");
        assert_eq!(record.meal_service_note, "          ");
        assert_eq!(record.joint_operation_airline_designators, "         ");
        assert_eq!(record.min_connecting_time_status_departure, " ");
        assert_eq!(record.min_connecting_time_status_arrival, " ");
        assert_eq!(record.secure_flight_indicator, " ");
        assert_eq!(record.itinerary_variation_identifier_overflow, " ");
        assert_eq!(record.aircraft_owner, "");
        assert_eq!(record.cockpit_crew_employer, "");
        assert_eq!(record.cabin_crew_employer, "");
        assert_eq!(record.onward_flight, "XY   13");
        assert_eq!(record.airline_designator2, "XY ");
        assert_eq!(record.flight_number2, "  13");
        assert_eq!(record.aircraft_rotation_layover, " ");
        assert_eq!(record.operational_suffix2, " ");
        assert_eq!(record.flight_transit_layover, " ");
        assert_eq!(record.operating_airline_disclosure, " ");
        assert_eq!(record.traffic_restriction_code, "           ");
        assert_eq!(record.traffic_restriction_code_leg_overflow_indicator, " ");
        assert_eq!(record.aircraft_configuration, "Y189VV738H189       ");
        assert_eq!(record.date_variation, "  ");
        assert_eq!(record.record_type, '3');
        assert_eq!(record.record_serial_number, "000003");
    }

    #[test]
    fn test_parse_segment_record_valid() {
        let line = "4 XX   130101J              AB050AMSGRQKL 2562                                                                                                                                                    000006";
        let carriers = vec![CarrierRecord {
            airline_designator: "XX ".to_string(),
            control_duplicate_indicator: " ".to_string(),
            time_mode: "A".to_string(),
            season: "SUM".to_string(),
            period_of_schedule_validity_from: "160625".to_string(),
            period_of_schedule_validity_to: "170929".to_string(),
            creation_date: "190725".to_string(),
            title_of_data: "AMERICAN AIRLINES INC".to_string(),
            release_date: "2022070".to_string(),
            schedule_status: "1".to_string(),
            general_information: "".to_string(),
            in_flight_service_information: "".to_string(),
            electronic_ticketing_information: "".to_string(),
            creation_time: "".to_string(),
            record_type: '2',
            record_serial_number: "000002".to_string(),
        }];

        let result = parse_segment_record(line, &carriers);
        assert!(result.is_some());

        let record = result.unwrap();
        assert_eq!(record.operational_suffix, " ");
        assert_eq!(record.airline_designator, "XX ");
        assert_eq!(record.control_duplicate_indicator, " ");
        assert_eq!(record.flight_number, "  13");
        assert_eq!(record.itinerary_variation_identifier, "01");
        assert_eq!(record.leg_sequence_number, "01");
        assert_eq!(record.service_type, "J");
        assert_eq!(record.itinerary_variation_identifier_overflow, " ");
        assert_eq!(record.board_point_indicator, "A");
        assert_eq!(record.off_point_indicator, "B");
        assert_eq!(record.board_point, "AMS");
        assert_eq!(record.off_point, "GRQ");
        assert_eq!(record.data_element_identifier, "050");
        assert_eq!(record.data, "KL 2562");
        assert_eq!(record.record_type, '4');
        assert_eq!(record.record_serial_number, "000006");
    }
}