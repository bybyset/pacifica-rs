use crate::options::replica_option::ReplicaOption;

#[test]
fn test_replica_option_default() {
    let replica_option = ReplicaOption::default();
    println!("{:?}", replica_option);

    assert_eq!(120000, replica_option.grace_period_timeout_ms);
    assert_eq!(80, replica_option.lease_period_timeout_ratio);
    assert_eq!(30, replica_option.heartbeat_factor);
    let lease_period_timeout = replica_option.lease_period_timeout();
    assert_eq!(120000 * 80 / 100, lease_period_timeout.as_millis());
    let heartbeat_interval = replica_option.heartbeat_interval();
    assert_eq!(120000 * 40 / 100, heartbeat_interval.as_millis());

    assert_eq!(100, replica_option.max_payload_entries_num);
    assert_eq!(10 * 1024 * 1024, replica_option.max_payload_entries_bytes);
}

