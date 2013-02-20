cbrecovery command usage
========================


        cbrecovery [options] restore_from restore_to

        where
        restore_from source cluster to restore data from
                     such as http://10.1.2.3:8091

        restore_to   destination cluster to restore data to
                     such as http://10.3.4.5:8091

        options are:

        -b  --bucket-source
                     type="string", default="default",
                     metavar="default",
                     help="""source bucket to recover from """

        -B  --bucket-destination
                     type="string", default="default",
                     metavar="default",
                     help="""destination bucket to recover to """

        -u  --username
                     type="string", default=None,
                     help="REST username for source cluster"

        -p  --password
                     type="string", default=None,
                     help="REST password for source cluster"

        -U  --username-destination
                     type="string", default=None,
                     help="REST username for destination cluster or server node"

        -P  --password-destination
                     type="string", default=None,
                     help="REST password for destination cluster or server node"

        -l  --vbucket-list
                     type="string", default=None,
                     help="""transmit data only from specified vbuckets"""

        -a  --add-node
                    type="string", default=None,
                    help="Add new node to cluster for data recovery"

        -r  --remove-node
                    type="string", default=None,
                    help="Remove failover node"

        -v  --verbose
                     action="count", default=0,
                     help="verbose logging; more -v's provide more verbosity"

New REST API proposal for cbrecovery tool
----------------------------------------

 - Retrieve missing vbucket list

> GET
> /pools/default/buckets/bucket_name/vbucketsMissing
> [500, 612, 712]

 - Add replacement node to cluster

> POST
> /pools/default/buckets/bucket_name/replaceNode
> 
> add=newnode&remove=oldnode

*ok*: 202

*error*:

401: node is not found

402: node exists already

403: node is still in active mode, you have to failover it first

 - set vbucket state from missing to replica

> POST
> /pools/default/buckets/bucket_name/changeVbucketStates
>
> vbucket=500&vbucket=612&vbucket=712&state=replica

ok: 202

error:

410: vbucket is not found

411: state is not valid, it has to be active, replica, pending or dead

 - set vbucket state from replica to active

> POST
> /pools/default/buckets/bucket_name/changeVbucketStates
>
> vbucket=500&vbucket=612&vbucket=712&state=active

 

- update vbucketmap

> post
> /pools/default/buckets/<bucket_name>/updateVbucketMap

ok: 202

error:

410: update failure

Main control function:
----------------------

    def main():
        #analysis arguments and options
        opt_construct(argv)

        #pre transfer phase
        1. retrieve missing vbucket list: /pools/default/buckets/%s/vbucketsMissing
        2. replace failover node with new node: /pools/default/buckets/%s/replaceNode
        3. change missing vbucket states to replica: /pools/default/buckets/%s/changeVbucketStates

        #pump_transfer.main()

        #post transfer phase
        1. change vbucket stats from replica to active: /pools/default/buckets/%s/changeVbucketStates
        2. update vbucketmap: /pools/default/buckets/%s/updateVbucketMap

    def find_handlers(self, opts, source, sink):
       return pump_tap.TAPDumpSource, pump_tap.TapSink


Tap Source extension:
---------------------

We need to extend the current TapDumpSource to allow dumping data from vbucket list
new option: --vbucket-list, string, default is none

    get_tap_conn():
       if self.vbucket_list:
          tap_opts[coucbaseConstants.TAP_FLAG_LIST_VBUCKETS] = ''

Tap Sink design:
----------------

TapSink will subclass from pump_mc.CBSink with the following overwriting functions:

- check_base()

>   //allow destrination vbucket state
> to be replica and pending


 - fineconn(mconns, vbucket_id)

        rc, conn = super(TapSink, self).findconn(mconns, vbucket_id)
        if rc != 0:
            return rc, None
        tap_opts = {memcacheConstants.TAP_FLAG_SUPPORT_ACK: ''}
        conn.tap_fix_flag_byteorder = version.split(".") >= ["2", "0", "0"]
        if self.tap_conn.tap_fix_flag_byteorder:
          tap_opts[memcacheConstants.TAP_FLAG_TAP_FIX_FLAG_BYTEORDER] = ''
        ext, val = TapSink.encode_tap_connect_opts(tap_opts)
        conn._sendCmd(memcacheConstants.CMD_TAP_CONNECT,
                     self.tap_name, val, 0, ext)
        return rv, conn

