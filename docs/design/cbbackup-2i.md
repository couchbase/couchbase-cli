How current backup/restore tools deal with design docs?
----------------------------------------

   In the extended options for cbtransfer, we have the following two options 

       "data_only":       (0,      "For value 1, only transfer data from a backup file or cluster"),
       "design_doc_only": (0,      "For value 1, transfer design documents only from a backup file or cluster"),

   By default, we back up or restore both the data and design docs, i.e. view definitions. But users can choose either data only or design doc only.

   cbtransfer will use memcached/dcp protocol to backup/restore data. And data are groups by bucket/node pair.

   cbtrasnfer will use REST api to backup/restore design docs. Design docs are bucket specific and applied to any node within a cluster.

How will we extend the current backup/restore tool for 2i?
----------------------------------------

It will depend on how we deal with the current view feature in the coming release.

If we keep the view feature,  we shoud introduce new options to cbtransfer tools.

       "index_meta_only":  (0,     "For value 1, transfer index meta files from backup files or a cluster")

Otherwise, we will obsolete the "design_doc_only" option and replace it with "index_meta_only" option.

**Dependencies**

It will depend on ns_server to provide REST api to get/put index meta inforamtion.

**Scope**

Similar to view data,  backup/restore tool won't backup/restore index data.


