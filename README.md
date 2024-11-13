# pacifica-rs

#### Overview
pacifica-rs is a production-grade high-performance log replication framework implemented in rust based on the consistency algorithm PacificA.
Unlike other "majority" algorithms such as paxos, raft, zab, etc.,
it adopts a Quorum mechanism (W+R>N) of N+1>N, that is,
"all nodes" are required for write requests, and only one node is required for read requests,
which is very friendly for "read" performance scenarios.
pacifica-rs is a general framework that you can use to implement multiple data replicas in your application,
allowing for high availability of read services and consistency across multiple data replicas.
You only need to implement your own "state machine," and pacifica-rs will automatically coordinate between them for you.