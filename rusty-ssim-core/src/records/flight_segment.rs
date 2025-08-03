use crate::records::flight_leg_records::FlightLegRecord;
use crate::records::segment_records::SegmentRecords;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct FlightSegment {
    pub flight_leg: FlightLegRecord,
    pub segments: Vec<Option<SegmentRecords>>,
}
