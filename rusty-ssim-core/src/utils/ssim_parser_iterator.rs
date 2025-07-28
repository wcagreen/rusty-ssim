use crate::utils::ssim_parser::{parse_carrier_record, parse_flight_record_legs, parse_segment_record};

use crate::records::carrier_record::CarrierRecord;
use crate::records::flight_leg_records::FlightLegRecord;
use crate::records::segment_records::SegmentRecords;

pub fn ssim_iterator(
    ssim: String,
) -> Result<
    (
        Vec<CarrierRecord>,
        Vec<FlightLegRecord>,
        Vec<SegmentRecords>,
    ),
    String,
> {
    let mut record_type_2 = Vec::new();
    let mut record_type_3s = Vec::new();
    let mut record_type_4s = Vec::new();
    for line in ssim.lines() {
        match line.chars().nth(0) {
            Some('2') => {
                if let Some(r) = parse_carrier_record(line) {
                    record_type_2.push(r);
                }
            }

            Some('3') => {
                if let Some(r) = parse_flight_record_legs(line) {
                    record_type_3s.push(r);
                }
            }

            Some('4') => {
                if let Some(r) = parse_segment_record(line) {
                    record_type_4s.push(r);
                }
            }

            _ => continue,
        }
    }

    Ok((record_type_2, record_type_3s, record_type_4s))
}
