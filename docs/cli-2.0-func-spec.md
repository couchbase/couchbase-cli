**Newly added commands for 2.0 features:**

    couchbase-cli COMMAND
    COMMAND:
    bucket-purge             purge all bucket data from disk
    bucket-compact           compact bucket database and index data
    bucket-cancel-compaction stop compaction process
    bucket-observe-diskqueue wait for disk queue draining complete

    cluster-edit             modify cluster wide settings

    xdcr-setup               create/edit/delete remote cluster connection
    xdcr-replicate           create/delete replication session

**Newly added options for existed commands**

    bucket-create
    --enable-purge=[0|1]            enable/disable purge [MB-7235]
    --enable-replica-index=[0|1]    enable index replication

    node-init
    --node-init-index-path=<path>   specify index path [MB-7323/MB-7372]

    cluster-init
    --cluster-username=Administrator        same as cluster-init-username
    --cluster-password=Password             same as cluster-init-password
    --cluster-port=8080                     same as cluster-init-port
    --cluster-ramsize=300                   same as cluster-init-ramsize
    --cluster-enable-autofailover=[0|1]     enable/disable auto failover
    --cluster-enable-notification=[0|1]     enable software update notification
    --cluster-autofailover-timeout=30       specify timeout value to trigger autofailover
    --cluster-compaction-db-percentage=30   specify percentage at which point db compaction is triggered
    --cluster-compaction-db-size            specify db size at which point compaction is triggered
    --cluster-compaction-view-percentage=30 specify percentage at which point view compaction is triggered
    --cluster-compaction-period-from[hh:mm] start time for compaction period
    --cluster-compaction-period-to[hh:mm]   stop time for compaction period
    --cluster-enable-compaction-abort=[0|1] allow to abort compaction if run time exceeds period
    --cluster-enable-compaction-parallel=[0|1]  allow to process database and view compaction in parallel

    cluster-
 - **Enable/Disable bucket flush**

Extend option for bucket-create / bucket-edit with --enable-purge

    couchbase-cli bucket-create --enable-purge=[0|1]

where 1 means to enable purging and 0 means to disable purging. Default is 0.

 - **Bucket purge command**

Flush all data under a bucket when flush option is enabled. By default, a confirmation question will be asked before proceeding.

    couchbase-cli bucket-purge [bucket-* OPTIONS] --force

/pools/default/buckets/default/controller/doFlush

    --force         purge bucket data without confirmation

Success:  Bucket flushing is successful

Fail :  You have to use bucket-edit to enable purge option first before running purging request.

 - **Enable bucket replica index**

Extend option for bucket-create with --enable-replica-index

    couchbase-cli bucket-create --enable-replica-index [bucket-* OPTIONS]

By default, replica index is set to false. You can enable it to add option --enable-replicaIndex
Note, this option won't be changed after the bucket is created. So you won't see it on bucket-edit options.

 - **Bucket compact command**

Compact all bucket data including datdabase and view index

    couchbase-cli bucket-compact [bucket-* OPTIONS]
    --data-only     compact database only
    --view-only     compact view only

/pools/default/buckets/default/controller/compactBucket

Compact bucket database data only
/pools/default/buckets/default/controller/compactDatabases

 - **Cancel compact operation for a bucket**

    couchbase-cli bucket-cancel-compact [bucket-* OPTIONS]

/pools/default/buckets/default/controller/cancelBucketCompactiond

 - **Cluster edit command**

c

    couchbase-cli cluster-edit [cluster OPTIONS]
    --cluster-username=USER                 admin username
    --cluster-password=password             admin password
    --cluster-port=PORT                     new cluster REST/http PORT
    --cluster-enable-autofailover=[0|1]     enable/disable auto failover
    --cluster-enable-notification=[0|1]     enable software update notification
    --cluster-autofailover-timeout=30       specify timeout value to trigger autofailover
    --cluster-compaction-db-percentage=30   specify percentage at which point db compaction is triggered
    --cluster-compaction-db-size            specify db size at which point compaction is triggered
    --cluster-compaction-view-percentage=30 specify percentage at which point view compaction is triggered
    --cluster-compaction-period-from[hh:mm] start time for compaction period
    --cluster-compaction-period-to[hh:mm]   stop time for compaction period
    --cluster-enable-compaction-abort=[0|1] allow to abort compaction if run time exceeds period
    --cluster-enable-compaction-parallel=[0|1]  allow to process database and view compaction in parallel

 - **Set index-path for node**

Extend node-init option with --node-init-index-path

    couchbase-cli node-init --node-init-index-path=PATH

 - **Enable/disable auto failover**

Extend cluster-init option with --cluster-init-enable-autofailover

    couchbase-cli cluster-init --cluster-init-enable-autofailover [cluster-init OPTIONS]

By default, it is false, i.e. --cluster-init-enable-autofailover is not specified

 - **XDCR setup**

Create/Edit remote cluster connection

    couchbase-cli xdcr-setup [--create|--edit]
    --xdcr-cluster-name <cluster_name>
    --xdcr-hostname <hostname>
    --xdcr-username <username>
    --xdcr-password <password>

POST  /pools/default/remoteClusters?name=<>&hostname=<>&username=<>&password=<>

PUT /pools/default/remoteClusters?name=<>&hostname=<>&username=<>&password=<>

Delete XDCRr replication session

    cochbase-cli xdcr-setup --delete


 - **XDCR replication management**

Create a new replication session

    couchbase-cli xdcr-replicate --create
    --xdcr-replicate-from-bucket
    --xdcr-replicate-to-cluster
    --xdcr-replicate-to-bucket
    --xdcr-replicate-type = "continuous"

Cancel/stop replication

    couchbase-cli xdcr-replicate --delete
    --xdcr-replication-id <session_id>

