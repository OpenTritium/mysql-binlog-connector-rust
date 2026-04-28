use std::io::Cursor;

use byteorder::ReadBytesExt;

use crate::{
    binlog_error::BinlogError,
    binlog_parser::{BinlogParser, ParserState},
    command::command_util::CommandUtil,
    constants::MysqlRespCode,
    event::{event_data::EventData, event_header::EventHeader},
    network::packet_channel::PacketChannel,
};

pub struct BinlogStream {
    pub channel: PacketChannel,
    pub parser: BinlogParser,
}

pub struct BinlogEvent {
    pub header: EventHeader,
    pub data: EventData,
}

impl BinlogStream {
    pub async fn read(&mut self) -> Result<(EventHeader, EventData), BinlogError> {
        let event = self.read_event().await?;
        Ok((event.header, event.data))
    }

    pub async fn read_event(&mut self) -> Result<BinlogEvent, BinlogError> {
        let buf = self.channel.read().await?;
        let mut cursor = Cursor::new(&buf);

        if cursor.read_u8()? == MysqlRespCode::ERROR {
            CommandUtil::parse_result(&buf)?;
        }

        // parse events, execute the callback
        let (header, data) = self.parser.next(&mut cursor)?;
        Ok(BinlogEvent { header, data })
    }

    pub fn snapshot_parser_state(&self) -> ParserState {
        self.parser.snapshot_state()
    }

    pub fn restore_parser_state(&mut self, state: ParserState) {
        self.parser.restore_state(state);
    }

    pub async fn close(&mut self) -> Result<(), BinlogError> {
        self.channel.close().await?;
        Ok(())
    }
}
