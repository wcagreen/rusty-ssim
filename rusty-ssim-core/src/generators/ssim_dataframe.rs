use crate::utils::ssim_parser::{CarrierRecord, FlightLegRecord, SegmentRecords};
use polars::prelude::*;

/// Build a Polars `DataFrame` from a slice of records.
///
/// Each field mapping is `"column_name" => field_name,` which uses `.as_ref()`
/// to extract a `&str` from `Cow<str>` or `String` fields.
///
/// For fields that are not `AsRef<str>` (e.g. `char`), use the `display` marker:
/// `"column_name" => display field_name,` which calls `.to_string()` instead.
macro_rules! build_dataframe {
    ($records:expr, $($rest:tt)*) => {{
        let mut columns: Vec<Column> = Vec::new();
        build_dataframe!(@step $records, columns, $($rest)*);
        DataFrame::new(columns)
    }};
    (@step $records:expr, $columns:ident,) => {};
    (@step $records:expr, $columns:ident, $col:expr => display $field:ident, $($rest:tt)*) => {
        $columns.push(Column::new(
            PlSmallStr::from_static($col),
            $records.iter().map(|r| r.$field.to_string()).collect::<Vec<_>>(),
        ));
        build_dataframe!(@step $records, $columns, $($rest)*);
    };
    (@step $records:expr, $columns:ident, $col:expr => $field:ident, $($rest:tt)*) => {
        $columns.push(Column::new(
            PlSmallStr::from_static($col),
            $records.iter().map(|r| r.$field.as_ref() as &str).collect::<Vec<_>>(),
        ));
        build_dataframe!(@step $records, $columns, $($rest)*);
    };
}

/// Build a carrier DataFrame from a carrier record.
fn build_carrier_dataframe(carrier: Option<&CarrierRecord>) -> PolarsResult<DataFrame> {
    let carriers: Vec<&CarrierRecord> = carrier.into_iter().collect();
    build_dataframe!(&carriers,
        "airline_designator" => airline_designator,
        "control_duplicate_indicator" => control_duplicate_indicator,
        "time_mode" => time_mode,
        "season" => season,
        "period_of_schedule_validity_from" => period_of_schedule_validity_from,
        "period_of_schedule_validity_to" => period_of_schedule_validity_to,
        "creation_date" => creation_date,
        "title_of_data" => title_of_data,
        "release_date" => release_date,
        "schedule_status" => schedule_status,
        "general_information" => general_information,
        "in_flight_service_information" => in_flight_service_information,
        "electronic_ticketing_information" => electronic_ticketing_information,
        "creation_time" => creation_time,
        "record_type" => display record_type,
        "record_serial_number" => record_serial_number,
    )
}

/// Build a flight DataFrame from flight leg records.
fn build_flight_dataframe(flights: &[FlightLegRecord<'_>]) -> PolarsResult<DataFrame> {
    build_dataframe!(flights,
        "flight_designator" => flight_designator,
        "operational_suffix" => operational_suffix,
        "airline_designator" => airline_designator,
        "control_duplicate_indicator" => control_duplicate_indicator,
        "flight_number" => flight_number,
        "itinerary_variation_identifier" => itinerary_variation_identifier,
        "leg_sequence_number" => leg_sequence_number,
        "service_type" => service_type,
        "period_of_operation_from" => period_of_operation_from,
        "period_of_operation_to" => period_of_operation_to,
        "days_of_operation" => days_of_operation,
        "frequency_rate" => frequency_rate,
        "departure_station" => departure_station,
        "scheduled_time_of_passenger_departure" => scheduled_time_of_passenger_departure,
        "scheduled_time_of_aircraft_departure" => scheduled_time_of_aircraft_departure,
        "time_variation_departure" => time_variation_departure,
        "passenger_terminal_departure" => passenger_terminal_departure,
        "arrival_station" => arrival_station,
        "scheduled_time_of_aircraft_arrival" => scheduled_time_of_aircraft_arrival,
        "scheduled_time_of_passenger_arrival" => scheduled_time_of_passenger_arrival,
        "time_variation_arrival" => time_variation_arrival,
        "passenger_terminal_arrival" => passenger_terminal_arrival,
        "aircraft_type" => aircraft_type,
        "passenger_reservations_booking_designator" => passenger_reservations_booking_designator,
        "passenger_reservations_booking_modifier" => passenger_reservations_booking_modifier,
        "meal_service_note" => meal_service_note,
        "joint_operation_airline_designators" => joint_operation_airline_designators,
        "min_connecting_time_status_departure" => min_connecting_time_status_departure,
        "min_connecting_time_status_arrival" => min_connecting_time_status_arrival,
        "secure_flight_indicator" => secure_flight_indicator,
        "itinerary_variation_identifier_overflow" => itinerary_variation_identifier_overflow,
        "aircraft_owner" => aircraft_owner,
        "cockpit_crew_employer" => cockpit_crew_employer,
        "cabin_crew_employer" => cabin_crew_employer,
        "onward_flight" => onward_flight,
        "airline_designator2" => airline_designator2,
        "flight_number2" => flight_number2,
        "aircraft_rotation_layover" => aircraft_rotation_layover,
        "operational_suffix2" => operational_suffix2,
        "flight_transit_layover" => flight_transit_layover,
        "operating_airline_disclosure" => operating_airline_disclosure,
        "traffic_restriction_code" => traffic_restriction_code,
        "traffic_restriction_code_leg_overflow_indicator" => traffic_restriction_code_leg_overflow_indicator,
        "aircraft_configuration" => aircraft_configuration,
        "date_variation" => date_variation,
        "record_type" => display record_type,
        "record_serial_number" => record_serial_number,
    )
}

/// Build a segment DataFrame from segment records.
fn build_segment_dataframe(segments: &[SegmentRecords<'_>]) -> PolarsResult<DataFrame> {
    build_dataframe!(segments,
        "flight_designator" => flight_designator,
        "operational_suffix" => operational_suffix,
        "airline_designator" => airline_designator,
        "control_duplicate_indicator" => control_duplicate_indicator,
        "flight_number" => flight_number,
        "itinerary_variation_identifier" => itinerary_variation_identifier,
        "leg_sequence_number" => leg_sequence_number,
        "itinerary_variation_identifier_overflow" => itinerary_variation_identifier_overflow,
        "board_point_indicator" => board_point_indicator,
        "off_point_indicator" => off_point_indicator,
        "data_element_identifier" => data_element_identifier,
        "board_point" => board_point,
        "off_point" => off_point,
        "data" => data,
        "record_type" => display record_type,
        "record_serial_number" => record_serial_number,
    )
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
