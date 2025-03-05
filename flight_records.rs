use serde::Serialize;
use crate::records::flight_leg_records::FlightLegRecord;
use crate::records::segment_records::SegmentRecords;

#[derive(Debug, Serialize)]
pub struct FlightSegment<'a>  {
    pub flight_leg: FlightLegRecord<'a>,
    pub segments: Vec<SegmentRecords<'a>>,
}