pub trait Checksum {
    fn checksum(&self) -> u64;

    fn is_corrupted(&self) -> bool {
        false
    }
}

pub fn checksum(value1: u64, value2: u64) -> u64 {
    value1 ^ value2
}
