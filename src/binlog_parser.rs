use crate::{
    binlog_error::BinlogError,
    constants,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::*, event_header::EventHeader,
        gtid_event::GtidEvent, previous_gtids_event::PreviousGtidsEvent, query_event::QueryEvent,
        rotate_event::RotateEvent, rows_query_event::RowsQueryEvent,
        table_map_event::TableMapEvent, transaction_payload_event::TransactionPayloadEvent,
        update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
        xa_prepare_event::XaPrepareEvent, xid_event::XidEvent,
    },
    event::{event_type::EventType, format_description_event::FormatDescriptionEvent},
};

use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek, SeekFrom},
};

pub struct BinlogParser {
    pub checksum_length: u8,
    pub table_map_event_by_table_id: HashMap<u64, TableMapEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct ParserState {
    pub checksum_length: u8,
    pub table_map_event_by_table_id: HashMap<u64, TableMapEvent>,
}

const MAGIC_VALUE: [u8; 4] = [0xfeu8, 0x62, 0x69, 0x6e];

impl BinlogParser {
    pub fn snapshot_state(&self) -> ParserState {
        ParserState {
            checksum_length: self.checksum_length,
            table_map_event_by_table_id: self.table_map_event_by_table_id.clone(),
        }
    }

    pub fn restore_state(&mut self, state: ParserState) {
        self.checksum_length = state.checksum_length;
        self.table_map_event_by_table_id = state.table_map_event_by_table_id;
    }

    pub fn check_magic<S: Read + Seek>(&mut self, stream: &mut S) -> Result<(), BinlogError> {
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic)?;
        match magic {
            MAGIC_VALUE => Ok(()),
            _ => Err(BinlogError::UnexpectedData("bad magic".into())),
        }
    }

    pub fn next<S: Read + Seek>(
        &mut self,
        stream: &mut S,
    ) -> Result<(EventHeader, EventData), BinlogError> {
        let header = EventHeader::parse(stream)?;
        let data_length = header.event_length as usize
            - constants::EVENT_HEADER_LENGTH
            - self.checksum_length as usize;

        let buf = self.read_event_data(stream, data_length)?;
        let mut cursor = Cursor::new(&buf);

        let event_type = EventType::from_code(header.event_type);
        match event_type {
            EventType::FormatDescription => {
                let event_data = FormatDescriptionEvent::parse(&mut cursor, data_length)?;
                self.checksum_length = event_data.checksum_type.get_length();
                Ok((header, EventData::FormatDescription(event_data)))
            }

            EventType::PreviousGtids => Ok((
                header,
                EventData::PreviousGtids(PreviousGtidsEvent::parse(&mut cursor)?),
            )),

            EventType::Gtid => Ok((header, EventData::Gtid(GtidEvent::parse(&mut cursor)?))),

            EventType::Query => Ok((header, EventData::Query(QueryEvent::parse(&mut cursor)?))),

            EventType::TableMap => {
                let event_data = TableMapEvent::parse(&mut cursor)?;
                self.table_map_event_by_table_id
                    .insert(event_data.table_id, event_data.clone());
                Ok((header, EventData::TableMap(event_data)))
            }

            EventType::WriteRows | EventType::ExtWriteRows => {
                let row_event_version = Self::get_row_event_version(&event_type);
                let event_data = WriteRowsEvent::parse(
                    &mut cursor,
                    &mut self.table_map_event_by_table_id,
                    row_event_version,
                )?;
                Ok((header, EventData::WriteRows(event_data)))
            }

            EventType::UpdateRows | EventType::ExtUpdateRows => {
                let row_event_version = Self::get_row_event_version(&event_type);
                let event_data = UpdateRowsEvent::parse(
                    &mut cursor,
                    &mut self.table_map_event_by_table_id,
                    row_event_version,
                )?;
                Ok((header, EventData::UpdateRows(event_data)))
            }

            EventType::DeleteRows | EventType::ExtDeleteRows => {
                let row_event_version = Self::get_row_event_version(&event_type);
                let event_data = DeleteRowsEvent::parse(
                    &mut cursor,
                    &mut self.table_map_event_by_table_id,
                    row_event_version,
                )?;
                Ok((header, EventData::DeleteRows(event_data)))
            }

            EventType::Xid => Ok((header, EventData::Xid(XidEvent::parse(&mut cursor)?))),

            EventType::XaPrepare => Ok((
                header,
                EventData::XaPrepare(XaPrepareEvent::parse(&mut cursor)?),
            )),

            EventType::TransactionPayload => Ok((
                header,
                EventData::TransactionPayload(TransactionPayloadEvent::parse(&mut cursor)?),
            )),

            EventType::RowsQuery => Ok((
                header,
                EventData::RowsQuery(RowsQueryEvent::parse(&mut cursor)?),
            )),

            EventType::Rotate => Ok((header, EventData::Rotate(RotateEvent::parse(&mut cursor)?))),

            EventType::HeartBeat => Ok((header, EventData::HeartBeat)),

            _ => Ok((header, EventData::NotSupported)),
        }
    }

    fn read_event_data<S: Read + Seek>(
        &mut self,
        stream: &mut S,
        data_length: usize,
    ) -> Result<Vec<u8>, BinlogError> {
        // read data for current event
        let mut buf = vec![0u8; data_length];
        stream.read_exact(&mut buf)?;
        // skip checksum
        stream.seek(SeekFrom::Current(self.checksum_length as i64))?;
        Ok(buf)
    }

    fn get_row_event_version(event_type: &EventType) -> u8 {
        match event_type {
            EventType::ExtWriteRows | EventType::ExtUpdateRows | EventType::ExtDeleteRows => 2,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BinlogParser, ParserState};
    use crate::event::table_map_event::TableMapEvent;

    #[test]
    fn parser_state_round_trips_checksum_and_table_map_cache() {
        let mut parser = BinlogParser {
            checksum_length: 4,
            table_map_event_by_table_id: Default::default(),
        };
        parser.table_map_event_by_table_id.insert(
            42,
            TableMapEvent {
                table_id: 42,
                database_name: "test".to_string(),
                table_name: "users".to_string(),
                column_types: vec![3],
                column_metas: vec![0],
                null_bits: vec![false],
                table_metadata: None,
            },
        );

        let state = parser.snapshot_state();
        let mut restored = BinlogParser {
            checksum_length: 0,
            table_map_event_by_table_id: Default::default(),
        };
        restored.restore_state(state);

        assert_eq!(restored.checksum_length, 4);
        assert_eq!(
            restored
                .table_map_event_by_table_id
                .get(&42)
                .expect("table map should be restored")
                .table_name,
            "users"
        );
    }

    #[test]
    fn empty_parser_state_is_valid() {
        let mut parser = BinlogParser {
            checksum_length: 4,
            table_map_event_by_table_id: Default::default(),
        };
        parser.restore_state(ParserState::default());

        assert_eq!(parser.checksum_length, 0);
        assert!(parser.table_map_event_by_table_id.is_empty());
    }
}
