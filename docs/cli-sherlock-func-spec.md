
Newly added options for existed commands
----------------------------------------

 **- Cluster management support for index and n1ql**

    couchbase-cli cluster-init

OPTIONS:

     --services=[data|index|n1ql]   services that run on servers

Use cases:

  Create a new cluster with default data service:

    couchbase-cli cluster-init -c 192.168.0.1:8091 \\
       --cluster-username=Administrator \\
       --cluster-password=password \\
       --cluster-port=8080 \\
       --cluster-ramsize=300

  Create a new cluster with both data and index service:

    couchbase-cli cluster-init -c 192.168.0.1:8091 \\
       --cluster-username=Administrator \\
       --cluster-password=password \\
       --cluster-port=8080 \\
       --cluster-ramsize=300 \\
       --services=data;index

  Create a new cluster with both data,index and n1ql service:

    couchbase-cli cluster-init -c 192.168.0.1:8091 \\
       --cluster-username=Administrator \\
       --cluster-password=password \\
       --cluster-port=8080 \\
       --cluster-ramsize=300 \\
       --services=data;index;n1ql

Note: If --services option specified but without data option included,
data service will always be included automatically.

 **- Add servers with data, index and n1ql services**

    couchbase-cli server-add

OPTIONS:

     --services=[data|index|n1ql]   services that run on the server

Use cases:

  Add a new server with service data and n1ql:

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

 **- Add servers with data, index and n1ql services to a group**

    couchbase-cli group-manage

OPTIONS:

     --services=[data|index|n1ql]   services that run on the server

Use cases:

  Add a new server with service data and n1ql to a group:

    couchbase-cli group-manage -c 192.168.0.1:8091 \\
       --group-name=group1 \\
       --create \\
       --add-servers=10.11.12.13:8091 \\
       --services=data;n1ql

  
  Add a new server with index service:

    couchbase-cli grlou -c 192.168.0.1:8091 \\
       --group-name=group2 \\
       --create \\
       --add-servers=10.11.12.13:8091;10.11.12.14 \\
       --services=index

