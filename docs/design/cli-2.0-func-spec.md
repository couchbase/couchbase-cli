**Newly added commands for 2.0 features:**

    couchbase-cli COMMAND
    COMMAND:
    bucket-flush             flush all bucket data from disk
    bucket-compact           compact bucket database and index data
    cluster-edit             modify cluster wide settings
    xdcr-setup               create/edit/delete remote cluster connection
    xdcr-replicate           create/delete replication session
    setting-compaction       set auto compaction settings
    setting-notification     set notification settings
    setting-alert            set email alert settings
    setting-autofailover     set auto failover settings
    setting-xdcr             set xdcr related settings

**Newly added options for existed commands**

    bucket-create
    --enable-flush=[0|1]            enable/disable flush [MB-7235]
    --enable-replica-index=[0|1]    enable index replication

    node-init
    --node-init-index-path=<path>   specify index path [MB-7323/MB-7372]

    cluster-init / cluster-edit
    --cluster-username=Administrator        same as cluster-init-username
    --cluster-password=Password             same as cluster-init-password
    --cluster-port=8080                     same as cluster-init-port
    --cluster-ramsize=300                   same as cluster-init-ramsize


 - **Enable/Disable bucket flush**

Extend option for bucket-create / bucket-edit with --enable-flush

    couchbase-cli bucket-create --enable-flush=[0|1]

where 1 means to enable flush and 0 means to disable flush. Default is 0.

 - **Bucket flush command**

Flush all data under a bucket when flush option is enabled. By default, a confirmation question will be asked before proceeding.

    couchbase-cli bucket-flush [bucket-* OPTIONS] --force

/pools/default/buckets/default/controller/doFlush

    --force        flush bucket data without confirmation

Success:  Bucket flushing is successful

Fail :  You have to use bucket-edit to enable flush option first before running flushing request.

 - **Enable bucket replica index**

Extend option for bucket-create with --enable-replica-index

    couchbase-cli bucket-create --enable-replica-index [bucket-* OPTIONS]

By default, replica index is set to false. You can enable it to add option --enable-replicaIndex
Note, this option won't be changed after the bucket is created. So you won't see it on bucket-edit options.

 - **Bucket compact command**

Compact all bucket data including datdabase and view index

    couchbase-cli bucket-compact [bucket-* OPTIONS]
    --data-only     compact database data only
    --view-only     compact view data only

/pools/default/buckets/default/controller/compactBucket

Compact bucket database data only
/pools/default/buckets/default/controller/compactDatabases

 - **Cluster edit command**

c

    couchbase-cli cluster-edit [cluster OPTIONS]
    --cluster-username=USER                 admin username
    --cluster-password=password             admin password
    --cluster-port=PORT                     new cluster REST/http PORT
    --cluster-ramsize=RAMSIZE               set cluster ramsize

 - **Set index-path for node**

Extend node-init option with --node-init-index-path

    couchbase-cli node-init --node-init-index-path=PATH

 - **Compaction settings**

A new setting-compaction cmd for compaction related settings

    setting-compaction OPTIONS:

    --compaction-db-percentage=PERCENTAGE     at which point database compaction is triggered
    --compaction-db-size=SIZE[MB]             at which point database compaction is triggered
    --compaction-view-percentage=PERCENTAGE   at which point view compaction is triggered
    --compaction-view-size=SIZE[MB]           at which point view compaction is triggered
    --compaction-period-from=HH:MM            allow compaction time period from
    --compaction-period-to=HH:MM              allow compaction time period to
    --enable-compaction-abort=[0|1]           allow compaction abort when time expires
    --enable-compaction-parallel=[0|1]        allow parallel compaction for database and view


 - **Enable/disable auto failover**

A new setting-autofailover command is used for auto failover settings

    setting-autofailover OPTIONS
    --enable-auto-failover=[0|1]            allow auto failover
    --auto-failover-timeout=TIMEOUT (>=30)  specify timeout that expires to trigger auto failover

 - **Enable/disable notification**

A new setting-notificaiton is used for notification settings

    setting-notification OPTIONS
    --enable-notification=[0|1]   allow software update notification

 - **Enable/disable notification**

A new setting-alert is used for email alert settings

    setting-alert OPTIONS
    --enable-email-alert=[0|1]   allow email alerts

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


Cancel/stop replication

    couchbase-cli xdcr-replicate --delete
    --xdcr-replication-id <session_id>

 - **Cluster setting management**

setting-compaction OPTIONS:

      --compaction-db-percentage=PERCENTAGE     at which point database compaction is triggered
      --compaction-db-size=SIZE[MB]             at which point database compaction is triggered
      --compaction-view-percentage=PERCENTAGE   at which point view compaction is triggered
      --compaction-view-size=SIZE[MB]           at which point view compaction is triggered
      --compaction-period-from=HH:MM            allow compaction time period from
      --compaction-period-to=HH:MM              allow compaction time period to
      --enable-compaction-abort=[0|1]           allow compaction abort when time expires
      --enable-compaction-parallel=[0|1]        allow parallel compaction for database and view

setting-notification OPTIONS:

      --enable-notification=[0|1]               allow notification

setting-alert OPTIONS:

      --enable-email-alert=[0|1]                allow email alert
      --email-recipients=RECIPIENT              email recipents, separate addresses with , or ;
      --email-sender=SENDER                     sender email address
      --email-user=USER                         email server username
      --email-password=PWD                      email server password
      --email-host=HOST                         email server host
      --email-port=PORT                         email server port
      --enable-email-encrypt=[0|1]              email encrypt
      --alert-auto-failover-node                node was auto failover
      --alert-auto-failover-max-reached         maximum number of auto failover nodes was reached
      --alert-auto-failover-node-down           node wasn't auto failover as other nodes are down at the same time\\
      --alert-auto-failover-cluster-small       node wasn't auto fail over as cluster was too small
      --alert-ip-changed                        node ip address has changed unexpectedly
      --alert-disk-space                        disk space used for persistent storgage has reached at least 90% capacity
      --alert-meta-overhead                     metadata overhead is more than 50%
      --alert-meta-oom                          bucket memory on a node is entirely used for metadata
      --alert-write-failed                      writing data to disk for a specific bucket has failed

setting-autofailover OPTIONS:

      --enable-auto-failover=[0|1]              allow auto failover
      --auto-failover-timeout=TIMEOUT (>=30)    specify timeout that expires to trigger auto failover

setting-xdcr OPTIONS:

      --max-concurrent-reps=[32]             maximum concurrent replications per bucket, 8 to 256.
      --checkpoint-interval=[1800]           intervals between checkpoints, 60 to 14400 seconds.
      --worker-batch-size=[500]              doc batch size, 500 to 10000.
      --doc-batch-size=[2048]KB              document batching size, 10 to 100000 KB
      --failure-restart-interval=[30]        interval for restarting failed xdcr, 1 to 300 seconds
      --optimistic-replication-threshold=[256] document body size threshold (bytes) to trigger optimistic replication

xdcr-setup OPTIONS:

      --create                               create a new xdcr configuration
      --edit                                 modify existed xdcr configuration
      --delete                               delete existed xdcr configuration
      --xdcr-cluster-name=CLUSTERNAME        cluster name
      --xdcr-hostname=HOSTNAME               remote host name to connect to
      --xdcr-username=USERNAME               remote cluster admin username
      --xdcr-password=PASSWORD               remtoe cluster admin password

xdcr-replicate OPTIONS:

      --create                               create and start a new replication
      --delete                               stop and cancel a replication
      --xdcr-from-bucket=BUCKET              local bucket name to replicate from
      --xdcr-cluster-name=CLUSTERNAME        remote cluster to replicate to
      --xdcr-to-bucket=BUCKETNAME            remote bucket to replicate to

