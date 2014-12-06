
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
       --services=data;query

  
  Add a new server with service index:

    couchbase-cli server-add -c 192.168.0.1:8091 \\
       --server-add=10.11.12.13:8091 \\
       --server-add-username=Admin1 \\
       --server-add-password=password \\
       --services=index

 **- Add servers with data, index and query services to a group**

    couchbase-cli group-manage

OPTIONS:

     --services=[data|index|query|moxi]   services that run on the server

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

