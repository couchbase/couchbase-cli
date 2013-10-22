Newly added commands for 2.5 features:
----------------------------------------

 **- Rack/Zone awareness - configure
   servers into groups**

    couchbase-cli group CLUSTER OPTIONS:

CLUSTER:

     --cluster=HOST[:PORT] or -c HOST[:PORT]

OPTONS:

     -u USERNAME, --username=USERNAME       admin username of the cluster
     -p PASSWORD, --password=PASSWORD       admin password of the cluster

     --create=GROUPNAME                     group name to be added
     --delete=GROUPNAME                     group name to be removed
     --list-map                             show group/server relationship map
     --add-server=HOST[:PORT];HOST[:PORT]     add a list of servers to the group
     --remove-server=HOST[:PORT];HOST[:PORT]  remove a list of servers from the group
     --move-server=HOST[:PORT];HOST[:PORT]   move a list of servers from one group to another
     --from-group=GROUPNAME                 group name that to move servers from group
     --to-group=GROUPNAME                   group name that to move servers to group

Question:

What if a server is removed from a group, but never added back to another server? Do we allow such scenario?

Modify existed commands
-----------------------

    couchbase-cli server-add/server-readd

OPTIONS:

     --server-add=HOST[:PORT]               server to be added
     --server-add-username=USERNAME         admin username for the server to be added
     --server-add-password=PASSWORD         admin password for the server to be added
     --group=GROUPNAME                      group name for the server to be added

    couchbase-cli server-list OPTIONS    show not only servers, but also groups that servers belong to


Newly added options for existed commands
----------------------------------------

 - Enable XDCR Data security using SSL

    couchbase-cli xdcr-replicate

OPTIONS:

      --create                               create and start a new replication
      --delete                               stop and cancel a replication
      --xdcr-from-bucket=BUCKET              local bucket name to replicate from
      --xdcr-clucter-name=CLUSTERNAME        remote cluster to replicate to
      --xdcr-to-bucket=BUCKETNAME            remote bucket to replicate to
      --enable-data-security=[0|1]           allow data security using ssl