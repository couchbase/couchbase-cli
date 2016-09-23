New option added
----------------

**OPTIONS:**

    -u USERNAME, --user=USERNAME      admin username of the cluster
    -p PASSWORD, --password=PASSWORD  admin password of the cluster
    -o KIND, --output=KIND            KIND is json or standard
    -d, --debug
    -s, --ssl                         uses SSL for communication with secure servers

Example
-------

  Set data path and hostname for an unprovisioned cluster:

    couchbse-cli node-init -c 192.168.0.1:8091 \\
       --node-init-data-path=/tmp/data \\
       --node-init-index-path=/tmp/index \\
       --node-init-hostname=myhostname \\
       -u Administrator -p password \\
       --ssl
