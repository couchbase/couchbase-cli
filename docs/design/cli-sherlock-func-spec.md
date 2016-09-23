
Newly added options for existed commands
----------------------------------------

 **- Add servers with data, index and query services**

    couchbase-cli server-add

OPTIONS:

     --services=[data|index|query|moxi]   services that run on the server

Use cases:

  Add a new server with service data and query:

    couchbase-cli server-add -c 192.168.0.1:8091 \\
       --server-add=10.11.12.13:8091 \\
       --server-add-username=Admin1 \\
       --server-add-password=password \\
       --services=data;n1ql

  
  Add a new server with service index:

    couchbase-cli server-add -c 192.168.0.1:8091 \\
       --server-add=10.11.12.13:8091 \\
       --server-add-username=Admin1 \\
       --server-add-password=password \\
       --services=index

 **- Add servers with data, index and query services to a group**

    couchbase-cli group-manage

OPTIONS:

     --services=[data|index|n1ql|moxi]   services that run on the server

Use cases:

  Add a new server with service data and query to a group:

    couchbase-cli group-manage -c 192.168.0.1:8091 \\
       --group-name=group1 \\
       --create \\
       --add-servers=10.11.12.13:8091 \\
       --services=data;query

  Add a new server with index service:

    couchbase-cli group-manage -c 192.168.0.1:8091 \\
       --group-name=group2 \\
       --create \\
       --add-servers=10.11.12.13:8091;10.11.12.14 \\
       --services=index

 **- Add filtering to xdcr replication**

    couchbase-cli bucket-create
    couchbase-cli bucket-edit

Use cases:

  Modify existed bucket:

    couchbase-cli bucket-edit -c 192.168.0.1:8091 -u Administrator -p password


  Create a new bucket:

    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1 \\
       --bucket-priority=high \\
       -u Administrator -p password

  Add a new server with index service:

    couchbase-cli group-manage -c 192.168.0.1:8091 \\
       --group-name=group2 \\
       --create \\
       --add-servers=10.11.12.13:8091;10.11.12.14 \\
       --services=index

 **- Add filter to xdcr replication**

    couchbase-cli xdcr-replicate

OPTIONS:

     --regex=REGEX   regular expression as filter

Use cases:

  Create a xdcr replication:

  Start a replication stream in memcached protocol
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --create \\
        --xdcr-cluster-name=test \\
        --xdcr-from-bucket=default \\
        --xdcr-to-bucket=default1 \\
        --xdcr-replication-mode=xmem \\
        --regex="192\.168\.1\.\d{1,3}" \\
        -u Administrator -p password
