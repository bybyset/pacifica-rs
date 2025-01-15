use crate::options::error::OptionError;
use clap::Parser;
use std::str::FromStr;
use std::time::Duration;

fn parse_bytes_with_unit(src: &str) -> Result<u64, OptionError> {
    let bytes = byte_unit::Byte::from_str(src).map_err(|e| OptionError::InvalidStr {
        reason: e.to_string(),
        parse_str: String::from(src),
    })?;
    Ok(bytes.as_u64())
}

#[derive(Debug, Clone, Parser)]
pub struct ReplicaOption {
    /// Grace Period. If the time limit of the Secondary detection is exceeded,
    /// the Primary is considered to be faulty, and the Primary Change Request is sent.
    /// Default 120 s
    #[clap(long, default_value = "120000")]
    pub grace_period_timeout_ms: u64,

    ///
    /// Lease Period. lease_period_timeout_ms= grace_period_timeout_ms * lease_period_timeout_ratio/100
    #[clap(long, default_value = "80")]
    pub lease_period_timeout_ratio: u64,

    /// factor of heartbeat intervals between replicas
    /// heartbeat intervals = grace_period_timeout_ms * heartbeat_factor / 100
    #[clap(long, default_value = "30")]
    pub heartbeat_factor: u64,

    /// Take snapshots periodically, in milliseconds since the last snapshot.
    /// Default 180 s
    #[clap(long, default_value = "180000")]
    pub snapshot_timeout_ms: u64,

    /// Take recover periodically for Candidate, in milliseconds since the last recover.
    ///
    /// Default 30s
    #[clap(long, default_value = "30000")]
    pub recover_timeout_ms: u64,

    /// Recover Request timeout ms
    /// Default 10min
    #[clap(long, default_value = "600000")]
    pub recover_request_timeout_ms: u64,

    /// The maximum number of entries per payload allowed to be transmitted during replication
    ///
    #[clap(long, default_value = "100")]
    pub max_payload_entries_num: u64,

    /// The maximum number of entries per payload allowed to be transmitted during replication
    /// Default 10m
    #[clap(long, default_value = "10MiB", value_parser=parse_bytes_with_unit)]
    pub max_payload_entries_bytes: u64,

    ///
    #[clap(bool, default_value = "true")]
    pub log_entry_checksum_enable: bool,
}

impl Default for ReplicaOption {
    fn default() -> Self {
        Self::parse_from(Vec::<&'static str>::new())
    }
}

impl ReplicaOption {
    pub fn lease_period_timeout(&self) -> Duration {
        Duration::from_millis(self.grace_period_timeout_ms * self.lease_period_timeout_ratio / 100)
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.grace_period_timeout_ms * self.heartbeat_factor / 100)
    }
}
