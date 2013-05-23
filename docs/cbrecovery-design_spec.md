cbrecovery command usage
========================


        cbrecovery [options] restore_from_cluster restore_to_cluster

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

 - When a membase (couchbase) bucket has some vbuckets missing it can be put into a recovery mode using startRecovery REST call:


>       curl -sX POST -u Administrator:password
> http://localhost:8091/pools/default/buckets/default/controller/startRecovery

 In case of success, the response looks as follows:

    {
    "code": "ok",
    "recoveryMap": [
    {
      "node": "n_1@10.17.40.207",
      "vbuckets": [
         33,
         34,
         35,
         36,
         37,
         38,
         39,
         40,
         41,
         42,
         54,
         55,
         56,
         57,
         58,
         59,
         60,
         61,
         62,
         63
        ]
      }
    ],
    "uuid": "8e02b3a84e0bbf58cbbb58919f1a6563"
    }

So in this case replica vbuckets 33-42 and 54-63 were created on node n_2@10.17.40.207. Now the client can start pushing data to these vbuckets.

All the important recovery URIs are advertised via tasks:

> curl -sX GET -u 'Administrator:password'
> http://localhost:8091/pools/default/tasks

    [
    {
    "bucket": "default",
    "commitVbucketURI": "/pools/default/buckets/default/controller/commitVBucket?recovery_uuid=8e02b3a84e0bbf58cbbb58919f1a6563",
    "recommendedRefreshPeriod": 10.0,
    "recoveryStatusURI": "/pools/default/buckets/default/recoveryStatus?recovery_uuid=8e02b3a84e0bbf58cbbb58919f1a6563",
    "stopURI": "/pools/default/buckets/default/controller/stopRecovery?recovery_uuid=8e02b3a84e0bbf58cbbb58919f1a6563",
    "type": "recovery",
    "uuid": "8e02b3a84e0bbf58cbbb58919f1a6563"
    },
    {
    "status": "notRunning",
    "type": "rebalance"
    }
    ]

- stopURI can be used to abort the recovery
- recoveryStatusURI will return information about the recovery in the
same format as startRecovery
- commitVBucketURI will activate certain vbucket

 This call should be used after the client is done with pushing
the data to it. VBucket is passed as a POST parameter:

> curl -sX POST -u
> 'Administrator:password' \
> http://localhost:8091/pools/default/buckets/default/controller/commitVBucket?recovery_uuid=8e02b3a84e0bbf58cbbb58919f1a6563 \
> -d vbucket=33
> 
> { "code": "ok" }

All the recovery related REST calls return a JSON object having a
"code" field. This (together with HTTP status code) indicates if the
call was successful.

Here's a complete list of possible REST calls replies.

 - startRecovery

> 200 | ok  |   Recovery started. Recovery map is returned in recoveryMap field.
>
> 400 | unsupported | Not all nodes in the cluster support recovery.
>
> 402 | not needed | Recovery is not needed.
>
> 404 | not present | Specified bucket not found.
>
> 500 | failed node | Could not start recovery because some nodes failed. A list of failed nodes can be found in the "failedNodes" field of the reply.
>
> 503 | rebalance running | Could not start recovery because rebalance is running.

 - stopRecovery


>     +
>     | 200 | ok |Recovery stopped successfully.
>     | 400| uuid_missing |recovery_uuid query parameter has not been specified.
>     | 404| bad_recovery |Either no recovery is in progress or provided uuid does not match the uuid of running recovery.
>

- commitVBucket

>     +
>     | 200| ok |VBucket commited successfully. |
>     | 200| recovery_completed |VBucket commited successfully. No more vbuckets to recover. So the cluster is not in recovery mode anymore.
>     | 400| uuid_missing |recovery_uuid query parameter has not been specified.
>     | 400| bad_or_missing_vbucket |VBucket is either unspecified or couldn't be converted to integer. 
>     | 404| vbucket_not_found |Specified VBucket is not part of the recovery map.
>     | 404| bad_recovery |Either no recovery is in progress or provided uuid does not match the uuid of running recovery.
>     | 500| failed_nodes |Could not commit vbucket because some nodes faileed. A list of failed nodes can be found in the "failedNodes" field of the reply.


Main control function:
----------------------

    main()
    #analysis arguments and options
    opt_construct(argv)

    #pre transfer phase to retrive missing vbucket list
    begin recovery: /pools/default/buckets/default/startRecovery

    #for each vbucket, transfer data between source to destination cluster
       #pump_transfer.main()

       #post transfer phase - activate vbucket after data transferring
       end recovery: pools/default/buckets/default/controller/commitVBucket

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

>   //allow streaming data to replica vbucket


 - sendMsg(vbucket_id)


cbrecovery tool re-entry
-----------------------

 - cbrecovery can be interrupted at any time during data transferring. it can be restarted again.
 - Everytime cbrecovery will retrieve brand new missing vbucket list.
 - data will be transferred vbucket by vbucket. A commit message will be sent out to activate vbucket
   state from replica to active after all messages for the vbucket be transferred.
