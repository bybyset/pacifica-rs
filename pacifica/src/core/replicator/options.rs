pub(crate) struct ReplicatorOptions {

    pub max_payload_entries_num: u64,

    pub max_payload_entries_bytes: u64,
}

impl ReplicatorOptions {
    pub fn new(max_payload_entries_num: u64, max_payload_entries_bytes: u64) -> Self {
        ReplicatorOptions {
            max_payload_entries_num,
            max_payload_entries_bytes,
        }
    }
}
