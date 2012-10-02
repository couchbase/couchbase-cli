cbworkloadgen
=============

We use this tool to generate workload for couchbase server. The tool can generate predefined workload or run in loop mode until it is stopped.

Run command
------------

    cbworkloadgen OPTIONS

OPTIONS:

  `-n HOST[:PORT]`, --node=HOST[:PORT] Default port is 8091

  `-u USERNAME`, --user=USERNAME       REST username of the cluster.

  `-p PASSWORD`, --password=PASSWORD   REST password of the cluster.

  `-b BUCKETNAME`, --bucket=BUCKETNAME Specific bucket name. Default is default bucket. Data can be inserted to a different bucket other than default.

  `-i ITEMS`, --item=ITEMS             Number of items to be inserted.

  `-r RATIO`, --ratio=RATIO            set/get operation ratio, which is a value between 0 and 1. Default .95 means 95% are set operations.

  `-s SIZE`, --size=SIZE               minimum value size. Default is 10 bytes.

  `-j`, --json                         Insert json format data. Default is false

  `-l`, --loop                         Loop forever until it is interrupted by users

  `-t THREADS`, --threads=THREADS      Number of concurrent workers to generate loads. Default is 1.

  `--prefix`                           prefix to use for memcached keys for json doc ids. Default is 'pymc'

  `-v`, --verbose                      Verbose logging; more -v's provide more verbosity

  `-h` --help                          Show this help message and exit

Example
-------


Upload 10000 json documents to localhost and to default bucket

    ./cbworkloadgen -n localhost -i 10000 -j


Generate continuous workload to node 10.3.121.192 with 75% set and 25% get operations

    ./cbworkloadgen -n 10.3.121.192:8091 -r .75 -l

Errors
------

These are kinds of error cases to consider ...

* If inserted to bucket other than default, REST username and password are needed
