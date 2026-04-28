use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    IdleWait,
    Reconnect,
    Fatal,
}

#[derive(Error, Debug)]
pub enum BinlogError {
    #[error("unsupported column type: {0}")]
    UnsupportedColumnType(String),

    #[error("unexpected binlog data: {0}")]
    UnexpectedData(String),

    #[error("connect error: {0}")]
    ConnectError(String),

    #[error("binlog stream idle timeout: {0}")]
    IdleTimeout(String),

    #[error("network timeout: {0}")]
    NetworkTimeout(String),

    #[error("connection closed: {0}")]
    ConnectionClosed(String),

    #[error("mysql server error {code} ({sql_state}): {message}")]
    ServerError {
        code: u16,
        sql_state: String,
        message: String,
    },

    #[error("missing TableMap event for table_id {table_id} while parsing {event}")]
    MissingTableMap { table_id: u64, event: &'static str },

    #[error("fmt error: {0}")]
    FmtError(#[from] std::fmt::Error),

    #[error("parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("parse utf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("parse url error: {0}")]
    ParseUrlError(#[from] url::ParseError),

    #[error("parse json error: {0}")]
    ParseJsonError(String),

    #[error("invalid gtid: {0}")]
    InvalidGtid(String),
}

impl BinlogError {
    pub fn idle_timeout(message: impl Into<String>) -> Self {
        Self::IdleTimeout(message.into())
    }

    pub fn network_timeout(message: impl Into<String>) -> Self {
        Self::NetworkTimeout(message.into())
    }

    pub fn connection_closed(message: impl Into<String>) -> Self {
        Self::ConnectionClosed(message.into())
    }

    pub fn server_error(
        code: u16,
        sql_state: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::ServerError {
            code,
            sql_state: sql_state.into(),
            message: message.into(),
        }
    }

    pub fn missing_table_map(table_id: u64, event: &'static str) -> Self {
        Self::MissingTableMap { table_id, event }
    }

    pub fn recovery_action(&self) -> RecoveryAction {
        if self.is_idle_timeout() {
            RecoveryAction::IdleWait
        } else if self.is_retryable_network_error() {
            RecoveryAction::Reconnect
        } else {
            RecoveryAction::Fatal
        }
    }

    pub fn is_idle_timeout(&self) -> bool {
        matches!(self, Self::IdleTimeout(_))
    }

    pub fn is_retryable_network_error(&self) -> bool {
        match self {
            Self::NetworkTimeout(_) | Self::ConnectionClosed(_) => true,
            Self::IoError(err) => matches!(
                err.kind(),
                std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
            ),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BinlogError;
    use super::RecoveryAction;
    use std::io::{Error, ErrorKind};

    #[test]
    fn recovery_action_distinguishes_idle_from_reconnect_and_fatal() {
        assert_eq!(
            BinlogError::idle_timeout("no events").recovery_action(),
            RecoveryAction::IdleWait
        );
        assert_eq!(
            BinlogError::connection_closed("closed").recovery_action(),
            RecoveryAction::Reconnect
        );
        assert_eq!(
            BinlogError::UnexpectedData("bad packet".to_string()).recovery_action(),
            RecoveryAction::Fatal
        );
    }

    #[test]
    fn retryable_network_error_detection_accepts_io_disconnects() {
        assert!(
            BinlogError::IoError(Error::new(ErrorKind::UnexpectedEof, "eof"))
                .is_retryable_network_error()
        );
        assert!(
            BinlogError::IoError(Error::new(ErrorKind::ConnectionReset, "rst"))
                .is_retryable_network_error()
        );
    }

    #[test]
    fn retryable_network_error_detection_accepts_structured_network_errors() {
        assert!(BinlogError::network_timeout("connection timed out").is_retryable_network_error());
        assert!(BinlogError::connection_closed("connection closed by peer")
            .is_retryable_network_error());
    }

    #[test]
    fn retryable_network_error_detection_rejects_idle_timeout() {
        assert!(!BinlogError::idle_timeout("no events").is_retryable_network_error());
        assert!(BinlogError::idle_timeout("no events").is_idle_timeout());
    }

    #[test]
    fn retryable_network_error_detection_rejects_protocol_errors() {
        assert!(
            !BinlogError::UnexpectedData("bad packet header".to_string())
                .is_retryable_network_error()
        );
        assert!(
            !BinlogError::ConnectError("connect mysql failed".to_string())
                .is_retryable_network_error()
        );
    }
}
