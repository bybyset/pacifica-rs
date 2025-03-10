use anyerror::AnyError;
use thiserror::Error;
use crate::LogId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorVerb {
    Read,
    Write,
    Seek,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorSubject {
    OpenWriter,
    OpenReader,

    FlushWriter,
    AppendEntries { index: usize },
    TruncatePrefix { first_log_index_kept: usize },
    TruncateSuffix { last_log_index_kept: usize },
    ResetLogStorage { next_log_index: usize },
    GetLogEntry { log_index: usize },
    ReadLogId,
    WriteLogId { log_id: LogId},

    DownloadSnapshot {reader_id: usize},
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub struct StorageError {
    subject: ErrorSubject,
    verb: ErrorVerb,
    source: AnyError,
    backtrace: Option<String>,
}

impl StorageError {
    pub fn new(subject: ErrorSubject, verb: ErrorVerb, source: impl Into<AnyError>) -> StorageError {
        Self {
            subject,
            verb,
            source: source.into(),
            backtrace: anyerror::backtrace_str(),
        }
    }

    pub fn open_reader(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::OpenReader, ErrorVerb::Read, source)
    }

    pub fn get_log_entry(log_index: usize, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::GetLogEntry { log_index }, ErrorVerb::Read, source)
    }

    pub fn open_writer(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::OpenWriter, ErrorVerb::Write, source)
    }

    pub fn append_entries(index: usize, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::AppendEntries { index }, ErrorVerb::Write, source)
    }

    pub fn flush_writer(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::FlushWriter, ErrorVerb::Write, source)
    }

    pub fn truncate_prefix(first_log_index_kept: usize, source: impl Into<AnyError>) -> Self {
        Self::new(
            ErrorSubject::TruncatePrefix { first_log_index_kept },
            ErrorVerb::Write,
            source,
        )
    }

    pub fn truncate_suffix(last_log_index_kept: usize, source: impl Into<AnyError>) -> Self {
        Self::new(
            ErrorSubject::TruncateSuffix { last_log_index_kept },
            ErrorVerb::Write,
            source,
        )
    }

    pub fn reset(next_log_index: usize, source: impl Into<AnyError>) -> Self {
        Self::new(
            ErrorSubject::ResetLogStorage { next_log_index },
            ErrorVerb::Write,
            source,
        )
    }

    pub fn read_log_id(source: impl Into<AnyError>) -> Self {
        Self::new(
            ErrorSubject::ReadLogId,
            ErrorVerb::Read,
            source,
        )
    }

    pub fn write_log_id(log_id: LogId, source: impl Into<AnyError>) -> Self {
        Self::new(
            ErrorSubject::WriteLogId {log_id},
            ErrorVerb::Write,
            source,
        )
    }

    pub fn download_snapshot(reader_id: usize, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::DownloadSnapshot {reader_id}, ErrorVerb::Write, source)
    }



}
