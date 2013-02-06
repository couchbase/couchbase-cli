**Newly added commands for 2.0 features:**

    couchbase-cli COMMAND
    COMMAND:
    bucket-flush             flush all bucket data
    bucket-compact           compact bucket database and index data
    bucket-cancel-compact    stop compaction process
    xdcr-create              create remote cluster connection
    xdcr-edit                edit remote cluster connection
    xdcr-delete              delete remote cluster connection
    xdcr-replicate-init      create replication session
    xdcr-replicate-cancel    stop and cancel replication session

**Newly added options for existed commands**

    bucket-create
    --enable-flush=[0|1]     enable/disable flush
    --enable-replica-index   enable index replication

    node-init
    --node-init-index-path=<path>   specify index path

    cluster-init
    --cluster-init-enable-autofailover=[0|1]  enable/disable auto failover


 - **Enable/Disable bucket flush**

Extend option for bucket-create / bucket-edit with --enable-flush

    couchbase-cli bucket-create --enable-flush=[0|1]

where 1 means to enable flush and 0 means to disable flush. Default is 0.

 - **Bucket flush command**

Flush all data under a bucket when flush option is enabled.

    couchbase-cli bucket-flush [bucket-* OPTIONS]

/pools/default/buckets/default/controller/doFlush

Success:  Bucket flushing is successful
Fail :  You have to use bucket-edit to enable flush option first before running flush request.

 - **Enable bucket replicaIndex**

Extend option for bucket-create with --enable-replica-index

    couchbase-cli bucket-create --enable-replica-index [bucket-* OPTIONS]

By default, replicaIndex is set to false. You can enable it to add option --enable-replicaIndex
Note, this option won't be changed after the bucket is created. So you won't see it on bucket-edit options.

 - **Bucket compact command**

Compact all bucket data including datdabase and view index

    couchbase-cli bucket-compact [bucket-* OPTIONS]

/pools/default/buckets/default/controller/compactBucket

Compact bucket database data only --bucket --database-only
/pools/default/buckets/default/controller/compactDatabases

 - **Cancel compact operation for a bucket**

    couchbase-cli bucket-cancel-compact [bucket-* OPTIONS]

/pools/default/buckets/default/controller/cancelBucketCompaction

 - **Set index-path for node**

Extend node-init option with --node-init-index-path

    couchbase-cli node-init --node-init-index-path=PATH

 - **Enable/disable auto failover**

Extend cluster-init option with --cluster-init-enable-autofailover

    couchbase-cli cluster-init --cluster-init-enable-autofailover [cluster-init OPTIONS]

By default, it is false, i.e. --cluster-init-enable-autofailover is not specified

 - **XDCR create / edit**

Create/Edit remote cluster connection

    couchbase-cli xdcr-create
    --xdcr-cluster-name <cluster_name>
    --xdcr-hostname <hostname>
    --xdcr-username <username>
    --xdcr-password <password>

    couchbase-cli xdcr-edit
    --xdcr-cluster-name <cluster_name>
    --xdcr-hostname <hostname>
    --xdcr-username <username>
    --xdcr-password <password>

POST  /pools/default/remoteClusters?name=<>&hostname=<>&username=<>&password=<>
PUT /pools/default/remoteClusters?name=<>&hostname=<>&username=<>&password=<>

 - **XDCR delete**

Delete XDCRr replication session

    cochbase-cli xdcr-delete --xdcr-delete


 - **XDCR create replicate**

Create a new replication session

    couchbase-cli xdcr-replicate-init
    --xdcr-replicate-from-bucket
    --xdcr-replicate-to-cluster
    --xdcr-replicate-to-bucket
    --xdcr-replicate-type = "continuous"

 - **XDCR cancel replicate**

Cancel replicate

    couchbase-cli xdcr-replicate-cancel

