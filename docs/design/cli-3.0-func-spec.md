Newly added commands for 3.0 features:
----------------------------------------

 **- delta recovery  (MB-10456)**

    couchbase-cli recovery OPTIONS:

recovery OPTIONS:

      --server-recovery=HOST[:PORT]     server to recover
      --recovery-type=TYPE[delta|full]  type of recovery to be performed for a node

Use cases:

  Set recovery type to a server:

    couchbase-cli recovery -c 192.168.0.1:8091 \\
       --server-recovery=192.168.0.2 \\
       --recovery-type=full \\
       -u Administrator -p password

  Set a failover, readd, recovery and rebalance sequence operations

    couchbase-cli failover -c 192.168.0.1:8091 \\
       --server-failover=192.168.0.2 \\
       -u Administrator -p password

    couchbase-cli server-readd -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2 \\
       -u Administrator -p password

    couchbase-cli recovery -c 192.168.0.1:8091 \\
       --server-recovery=192.168.0.2 \\
       --recovery-type=delta \\
       -u Administrator -p password

    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       -u Administrator -p password


Newly added options for existed commands
----------------------------------------

 **- Tuable memory for bucket (MB-10366)**
    couchbase-cli bucket-create/bucket-edit

OPTIONS:

     --bucket-eviction-policy=[valueOnly|fullEviction]   policy how to retain meta in memory

Use cases:

  Create a new sasl bucket:

    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-password=password \\
       --bucket-ramsize=200 \\
       --bucket-eviction-policy=valueOnly \\
       --enable-flush=1 \\
       -u Administrator -p password

  Modify a dedicated port bucket:

    couchbase-cli bucket-edit -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-port=11222 \\
       --bucket-ramsize=400 \\
       --bucket-eviction-policy=fullEviction \\
       --enable-flush=1 \\
       -u Administrator -p password

 **- Pause/Resume XDCR replication (MB-10056)**

    couchbase-cli xdcr-replicate

OPTIONS:

    --pause                                pause the replication
    --resume                               resume the replication
    --settings                             update settings for the replication
    --xdcr-replicator=REPLICATOR           replication id
    --max-concurrent-reps=[32]             maximum concurrent replications per bucket, 8 to 256.
    --checkpoint-interval=[1800]           intervals between checkpoints, 60 to 14400 seconds.
    --worker-batch-size=[500]              doc batch size, 500 to 10000.
    --doc-batch-size=[2048]KB              document batching size, 10 to 100000 KB
    --failure-restart-interval=[30]        interval for restarting failed xdcr, 1 to 300 seconds
    --optimistic-replication-threshold=[256] document body size threshold (bytes) to trigger optimistic replication

Use cases:

  Pause a running replication stream

    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --pause \\
        --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default \\
        -u Administrator -p password

  Resume a paused replication stream

    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --resume \\
        --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default \\
        -u Administrator -p password

  Update settings for a replication stream

    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --settings \\
        --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default \\
        --max-concurrent-reps=32   \\
        --checkpoint-interval=1800 \\
        --worker-batch-size=500    \\
        --doc-batch-size=2048      \\
        --failure-restart-interval=30 \\
        --optimistic-replication-threshold=256 \\
        -u Administrator -p password

 **- support graceful failover  (CBD-1283)**

    couchbase-cli failover

OPTIONS:

    --force    failover node from cluster right away. Without specified, it will be graceful failover


**- bucket priority  (MB-10369)**

     couchbase-cli bucket-create/bucket-edit

OPTIONS:

     --bucket-priority=[low|high]      priority when compared to other buckets

Use cases:

  Create a new couchbase bucket with high priority:

    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1 \\
       --bucket-priority=high \\
       -u Administrator -p password

  Modify a bucket to low priority:

    couchbase-cli bucket-edit -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-priority=low \\
       -u Administrator -p password
