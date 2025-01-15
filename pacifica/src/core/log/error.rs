
#[derive(Clone)]
pub(crate) enum LogManagerError {

    NotFoundLogEntry {
        log_index: u64
    }

}

