mod error;
mod replica_option;
#[cfg(test)]
mod replica_option_test;

pub use self::replica_option::ReplicaOption;
pub use self::error::OptionError;