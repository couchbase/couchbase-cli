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

        -v  --verbose
                     action="count", default=0,
                     help="verbose logging; more -v's provide more verbosity"

New REST API proposal for cbrecovery tool
----------------------------------------

 - Recovery begin

> GET
> /pools/default/buckets/bucket_name/beginRecovery
> 
> {"node1": [200, 201, 340],
>  "node2": [102, 521, 900, 904]
> }


*ok*: 202

*error*:

401: cannot start a data recovery.

402: no data missing in active vbuckets

 - Vbucket data recovery complete

> POST
> /pools/default/buckets/bucket_name/commitVbucketRecovery
>
> vbucket=200
>

*ok* 202

*error*:

401: vbucket doesn't exist

402: vbucket is not in recovery mode

- Recovery end

> POST
> /pools/default/buckets/bucket_name/endRecovery
>

*ok*: 202

*error*:

401: cluster is not in recovery mode


- Recovery abort

> post
> /pools/default/buckets/<bucket_name>/abortRecovery

*ok*: 202

*error*:

401: cluster is not in recovery mode

Main control function:
----------------------

    main()
    #analysis arguments and options
    opt_construct(argv)

    #pre transfer phase
     1. begin recovery: /pools/default/buckets/%s/beginRecovery

    #pump_transfer.main()

    #post transfer phase
     1. end recovery: /pools/default/buckets/%s/endRecovery

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

 - sendMsg(vbucket_id)

        rv = super(Tapsink, self).sendMsg(conn, msgs, self.operation(),
                                         vbucket_id=vbucket_id)
        #send vbucket recovery commit msg

cbrecovery tool reentry
-----------------------

 - cbrecovery can be interrupted any time during data transferring, it can be restarted again.
   Data will be retrieved again for the vbucket which is interrupted during last last run.
 - Everytime cbrecovery will retrieve a vbucket list which has data missing.
 - data will be transferred vbucket by vbucket. A commit message will be sent out after all messages
   for the vbucket are transferred.