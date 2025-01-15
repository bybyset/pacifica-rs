use anyerror::AnyError;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorVerb {
    Read,
    Write,
    Seek,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorSubject {

    OpenLogWriter,
    FlushLogWriter,
    AppendEntries {
        index: usize,
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub struct StorageError
{
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


    pub fn open_log_writer(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::OpenLogWriter, ErrorVerb::Write, source)
    }

    pub fn append_entries(index: usize, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::AppendEntries{index}, ErrorVerb::Write, source)
    }

    pub fn flush_log_writer(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::FlushLogWriter, ErrorVerb::Write, source)
    }


}





pub enum LogEntryCodecError {

    EncodeError,

    DecodeError,
}