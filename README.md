# pacifica-rs

[中文](./README_zh_CN.md)

#### Overview
pacifica-rs is a production-grade high-performance log replication framework implemented in rust based on the consistency algorithm PacificA.
Unlike other "majority" algorithms such as paxos, raft, zab, etc.,
it adopts a Quorum mechanism (W+R>N) of N+1>N, that is,
"all nodes" are required for write requests, and only one node is required for read requests,
which is very friendly for "read" performance scenarios.
pacifica-rs is a general framework that you can use to implement multiple data replicas in your application,
allowing for high availability of read services and consistency across multiple data replicas.
You only need to implement your own "state machine," and pacifica-rs will automatically coordinate between them for you.


## Features
- Consistency guarantee for multiple data replicas
- Fault tolerance: N-1 data replicas are allowed to fail without affecting the overall system availability
- Log replication and replica recovery
- Snapshot and log compression
- Active change Primary
- Partition tolerance in symmetric networks
- Tolerance of asymmetric network partitions


## Doc

### How to use
To see example [counter](./examples/counter)

### GUIDE
To see [GUIDELINES](./GUIDELINES.md)


## Contribution
Contributions to the development of JPacificA are welcome!
If you have any suggestions, questions, or want to contribute code, see our [how-to-contribution](./HOW-TO-CONTRIBUTE.md)


## License
JPacificA is released as open source under the Apache License 2.0 or MIT. See the [LICENSE-APACHE](./LICENSE-APACHE) or [LICENSE-MIT](./LICENSE-MIT) agreement file for details.
