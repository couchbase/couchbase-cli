
import stats_buffer
import util_cli as util

class NodeList:
    def run(self, accessor):
        result = []
        for node, node_info in stats_buffer.nodes.iteritems():
            result.append({"ip": node, "port": node_info['port'], "version" :node_info['version'], "os": node_info['os'], "status" :node_info['status']})
        return result

class NumNodes:
    def run(self, accessor):
        result = []
        result.append(len(stats_buffer.nodes))
        return result

class NumDownNodes:
    def run(self, accessor):
        result = []
        result.append(len(filter(lambda (a, b): b["status"]=="down", stats_buffer.nodes.items())))
        return result

class NumWarmupNodes:
    def run(self, accessor):
        result = []
        result.append(len(filter(lambda (a, b): b["status"]=="warmup", stats_buffer.nodes.items())))
        return result

class NumFailOverNodes:
    def run(self, accessor):
        result = []
        result.append(len(filter(lambda (a, b): b["clusterMembership"]!="active", stats_buffer.nodes.items())))
        return result

class BucketList:
    def run(self, accessor):
        result = []
        for bucket in stats_buffer.bucket_info.keys():
            result.append({"name": bucket})

        return result

class NodeStorageStats:
    def run(self, accessor):
        result = []
        for node, values in stats_buffer.nodes.iteritems():
            if values["StorageInfo"].has_key("hdd"):
                result.append({"ip": values["host"],
                           "port": values["port"],
                           "type" : "hdd",
                           "free": util.size_label(values["StorageInfo"]["hdd"]["free"]),
                           "quotaTotal" : util.size_label(values["StorageInfo"]["hdd"]["quotaTotal"]),
                           "used" : util.size_label(values["StorageInfo"]["hdd"]["used"]),
                           "usedByData" : util.size_label(values["StorageInfo"]["hdd"]["usedByData"]),
                           "total" : util.size_label(values["StorageInfo"]["hdd"]["total"])})
            if values["StorageInfo"].has_key("ram"):
                result.append({"ip": values["host"],
                           "port": values["port"],
                           "type" : "ram",
                           "quotaTotal" : util.size_label(values["StorageInfo"]["ram"]["quotaTotal"]),
                           "used" : util.size_label(values["StorageInfo"]["ram"]["used"]),
                           "usedByData" : util.size_label(values["StorageInfo"]["ram"]["usedByData"]),
                           "total" : util.size_label(values["StorageInfo"]["ram"]["total"])})
        return result

class NodeSystemStats:
    def run(self, accessor):
        result = []
        for node, values in stats_buffer.nodes.iteritems():
            result.append({"ip": values["host"],
                           "port": values["port"],
                           "cpuUtilization" :util.pretty_float(values["systemStats"]["cpu_utilization_rate"]),
                           "swapTotal": util.size_label(values["systemStats"]["swap_total"]),
                           "swapUsed" : util.size_label(values["systemStats"]["swap_used"]),
                           "currentItems" : values["systemStats"]["currentItems"],
                           "currentItemsTotal" : values["systemStats"]["currentItemsTotal"],
                           "replicaCurrentItems" : values["systemStats"]["replicaCurrentItems"]})

        return result

class ConnectionTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            trend = []
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                trend.append((node, a, vals[-1]))
            result[bucket] = trend
        return result

class CalcTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            trend = []
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                trend.append((node, a))
            result[bucket] = trend
        return result

class NodePerformanceStats:
    def run(self, accessor):
        result = {}
        for bucket, bucket_stats in stats_buffer.node_stats.iteritems():
            stats = []
            for node, stats_info in bucket_stats.iteritems():

                for key, value in stats_info.iteritems():
                    if key.find(accessor["counter"]) >= 0:
                        if accessor.has_key("threshold"):
                            if int(value) > accessor["threshold"]:
                                stats.append((node, (key, value)))
                        else:
                            if accessor.has_key("unit"):
                                if accessor["unit"] == "time":
                                    stats.append((node, util.time_label(value)))
                                elif accessor["unit"] == "size":
                                    stats.append((node, util.size_label(int(value))))
                            else:
                                stats.append((node, (key,value)))

            result[bucket] = stats
        return result

NodeCapsule = [
    {"name" : "NodeStatus",
     "ingredients" : [
        {
            "name" : "nodeList",
            "description" : "Node list",
            "code" : "NodeList",
        },
        {
            "name" : "numNodes",
            "description" : "Number of Nodes",
            "code" : "NumNodes",
        },
        {
            "name" : "numDownNodes",
            "description" : "Number of Down Nodes",
            "code" : "NumDownNodes",
        },
        {
            "name" : "numWarmupNodes",
            "description" : "Number of Warmup Nodes",
            "code" : "NumWarmupNodes",
        },
        {
            "name" : "numFailedOverNodes",
            "description" : "Number of Nodes failed over",
            "code" : "NumFailOverNodes",
        },
      ],
      "clusterwise" : False,
      "nodewise" : True,
      "perNode" : False,
      "perBucket" : False,
    },
    {"name" : "NumberOfConnection",
    "ingredients" : [
        {
            "name" : "connectionTrend",
            "description" : "Connection Trend",
            "counter" : "curr_connections",
            "scale" : "minute",
            "code" : "ConnectionTrend",
            "threshold" : {
                "high" : 10000,
            },
        },
     ],
     "nodewise" : True,
     "perNode" : True,
    },
    {"name" : "OOMError",
     "ingredients" : [
        {
            "name" : "oomErrors",
            "description" : "OOM Errors",
            "counter" : "ep_oom_errors",
            "scale" : "hour",
            "code" : "CalcTrend",
        },
        {
            "name" : "tempOomErrors",
            "description" : "Temporary OOM Errors",
            "counter" : "ep_tmp_oom_errors",
            "scale" : "hour",
            "code" : "CalcTrend",
        },
     ]
    },
    {"name" : "bucketList",
     "ingredients" : [
        {
            "name" : "bucketList",
            "description" : "Bucket list",
            "code" : "BucketList",
        },
     ],
     "nodewise" : True,
    },
    {"name" : "nodeStorageStats",
     "ingredients" : [
        {
            "name" : "nodeStorageStats",
            "description" : "Node storage stats",
            "code" : "NodeStorageStats",
        },
     ],
     "nodewise" : True,
    },
    {"name" : "nodeSystemStats",
     "ingredients" : [
        {
            "name" : "nodeSystemStats",
            "description" : "Node system stats",
            "code" : "NodeSystemStats",
        },
     ],
     "nodewise" : True,
    },
    {"name" : "tapPerformance",
     "ingredients" : [
        {
            "name" : "backfillRemaining",
            "description" : "Number of backfill remaining",
            "counter" : "ep_tap_queue_backfillremaining",
            "code" : "NodePerformanceStats",
        },
        {
            "name" : "tapNack",
            "description" : "Number of nacks",
            "counter" : "num_tap_nack",
            "code" : "NodePerformanceStats",
        },
        {
            "name" : "tapIdle",
            "description" : "Idle tap streams",
            "counter" : "idle",
            "code" : "NodePerformanceStats",
        },
     ],
     "perBucket" : True,
    },
    {"name" : "checkpointPerformance",
     "ingredients" : [
        {
            "name" : "openCheckPoint",
            "description" : "Items for open checkpoints",
            "counter" : "num_checkpoint_items",
            "code" : "NodePerformanceStats",
            "threshold" : 10000,
        },
     ],
     "perBucket" : True,
    },
    {"name" : "diskPerformance",
     "ingredients" : [
        {
            "name" : "diskCommit",
            "description" : "Averge disk commit time",
            "counter" : "disk_commit",
            "code" : "NodePerformanceStats",
            "unit" : "time",
        },
        {
            "name" : "diskUpdate",
            "description" : "Averge disk update time",
            "counter" : "disk_update",
            "code" : "NodePerformanceStats",
            "unit" : "time",
        },
        {
            "name" : "diskInsert",
            "description" : "Averge disk insert time",
            "counter" : "disk_insert",
            "code" : "NodePerformanceStats",
            "unit" : "time",
        },
        {
            "name" : "diskDelete",
            "description" : "Averge disk delete time",
            "counter" : "disk_del",
            "code" : "NodePerformanceStats",
            "unit" : "time",
        },
     ],
     "perBucket" : True,
    },
    {"name" : "AverageDocumentSize",
     "ingredients" : [
        {
            "name" : "averageDocumentSize",
            "description" : "Average Document Size",
            "counter" : "item_alloc_sizes",
            "code" : "NodePerformanceStats",
            "unit" : "size",
        },
     ],
     "perBucket" : True,
    },
    {"name" : "MemoryUsage",
     "ingredients" : [
        {
            "name" : "totalMemoryUsage",
            "description" : "Total memory usage",
            "counter" : "total_heap_bytes",
            "code" : "NodePerformanceStats",
            "unit" : "size",
        },
        {
            "name" : "totalFragmentation",
            "description" : "Total memory fragmentation",
            "counter" : "total_fragmentation_bytes",
            "code" : "NodePerformanceStats",
            "unit" : "size",
        },
        {
            "name" : "totalInternalMemory",
            "description" : "Total internal memory usage",
            "counter" : "mem_used",
            "code" : "NodePerformanceStats",
            "unit" : "size",
        },
        {
            "name" : "overhead",
            "description" : "Memory overhead",
            "counter" : "ep_overhead",
            "scale" : "hour",
            "code" : "NodePerformanceStats",
            "unit" : "size",
        },
     ],
     "perBucket" : True,
    },
    {"name" : "EPEnginePerformance",
     "ingredients" : [
        {
            "name" : "flusherState",
            "description" : "Engine flusher state",
            "counter" : "ep_flusher_state",
            "code" : "NodePerformanceStats",
        },
        {
            "name" : "flusherCompleted",
            "description" : "Flusher completed",
            "counter" : "ep_flusher_num_completed",
            "code" : "NodePerformanceStats",
        },
        {
            "name" : "avgItemLoadTime",
            "description" : "Average item loaded time",
            "counter" : "ep_bg_load_avg",
            "code" : "NodePerformanceStats",
            "unit" : "time"
        },
        {
            "name" : "avgItemWaitTime",
            "description" : "Averge item waited time",
            "counter" : "ep_bg_wait_avg",
            "code" : "NodePerformanceStats",
            "unit" : "time",
        },
     ],
     "perNode" : True,
    },
]


