import stats_buffer
import util_cli as util

class BucketSummary:
    def run(self, accessor):
        return  stats_buffer.bucket_info

class DGMRatio:
    def run(self, accessor):
        result = []
        hdd_total = 0
        ram_total = 0
        for node, nodeinfo in stats_buffer.nodes.iteritems():
            if nodeinfo["StorageInfo"].has_key("hdd"):
                hdd_total += nodeinfo['StorageInfo']['hdd']['usedByData']
            if nodeinfo["StorageInfo"].has_key("ram"):
                ram_total += nodeinfo['StorageInfo']['ram']['usedByData']
        if ram_total > 0:
            ratio = hdd_total / ram_total
        else:
            ratio = 0
        return ratio

class ARRatio:
    def run(self, accessor):
        result = {}
        cluster = 0
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            item_avg = {
                "curr_items": [],
                "vb_replica_curr_items": [],
            }
            num_error = []
            for counter in accessor["counter"]:
                values = stats_info[accessor["scale"]][counter]
                nodeStats = values["nodeStats"]
                samplesCount = values["samplesCount"]
                for node, vals in nodeStats.iteritems():
                    avg = sum(vals) / samplesCount
                    item_avg[counter].append((node, avg))
            res = []
            active_total = replica_total = 0
            for active, replica in zip(item_avg['curr_items'], item_avg['vb_replica_curr_items']):
                if replica[1] == 0:
                    res.append((active[0], "No replica"))
                else:
                    ratio = 1.0 * active[1] / replica[1]
                    res.append((active[0], util.pretty_float(ratio)))
                    if ratio < accessor["threshold"]:
                        num_error.append({"node":active[0], "value": ratio})
                active_total += active[1]
                replica_total += replica[1]
            if replica_total == 0:
                res.append(("total", "no replica"))
            else:
                ratio = active_total * 1.0 / replica_total
                cluster += ratio
                res.append(("total", util.pretty_float(ratio)))
                if ratio != accessor["threshold"]:
                    num_error.append({"node":"total", "value": ratio})
            #if len(num_error) > 0:
            #    result[bucket] = {"error" : num_error}
            #else:
            result[bucket] = res
        if len(stats_buffer.buckets) > 0:
            result["cluster"] = util.pretty_float(cluster / len(stats_buffer.buckets))

        return result

class OpsRatio:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            ops_avg = {
                "cmd_get": [],
                "cmd_set": [],
                "delete_hits" : [],
            }
            for counter in accessor["counter"]:
                values = stats_info[accessor["scale"]][counter]
                nodeStats = values["nodeStats"]
                samplesCount = values["samplesCount"]
                for node, vals in nodeStats.iteritems():
                    avg = sum(vals) / samplesCount
                    ops_avg[counter].append((node, avg))
            res = []
            read_total = write_total = del_total = 0
            for read, write, delete in zip(ops_avg['cmd_get'], ops_avg['cmd_set'], ops_avg['delete_hits']):
                count = read[1] + write[1] + delete[1]
                if count == 0:
                    res.append((read[0], "0:0:0"))
                else:
                    read_ratio = read[1] *100 / count
                    read_total += read_ratio
                    write_ratio = write[1] * 100 / count
                    write_total += write_ratio
                    del_ratio = delete[1] * 100 / count
                    del_total += del_ratio
                    res.append((read[0], "{0}:{1}:{2}".format(int(read_ratio+.5), int(write_ratio+.5), int(del_ratio+.5))))
            read_total /= len(ops_avg['cmd_get'])
            write_total /= len(ops_avg['cmd_set'])
            del_total /= len(ops_avg['delete_hits'])
            res.append(("total", "{0}:{1}:{2}".format(int(read_total+.5), int(write_total+.5), int(del_total+.5))))
            result[bucket] = res

        return result

class CacheMissRatio:
    def run(self, accessor):
        result = {}
        cluster = 0
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            trend = []
            total = 0
            data = []
            for node, vals in nodeStats.iteritems():
                #a, b = util.linreg(timestamps, vals)
                value = sum(vals) / samplesCount
                #value = a * timestamps[-1] + b
                total += value
                trend.append((node, util.pretty_float(value)))
                data.append(value)
            total /= len(nodeStats)
            trend.append(("total", util.pretty_float(total)))
            trend.append(("variance", util.two_pass_variance(data)))
            cluster += total
            result[bucket] = trend
        if len(stats_buffer.buckets) > 0:
            result["cluster"] = util.pretty_float(cluster / len(stats_buffer.buckets))
        return result

class MemUsed:
    def run(self, accessor):
        result = {}
        cluster = 0
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            trend = []
            total = 0
            data = []
            for node, vals in nodeStats.iteritems():
                avg = sum(vals) / samplesCount
                trend.append((node, util.size_label(avg)))
                data.append(avg)
            #print data
            trend.append(("variance", util.two_pass_variance(data)))
            result[bucket] = trend
        return result

class ItemGrowth:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            trend = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                if b < 1:
                    trend.append((node, 0))
                else:
                    start_val = b
                    end_val = a * timestamps[-1] + b
                    rate = (end_val * 1.0 / b - 1.0) * 100
                    trend.append((node, util.pretty_float(rate)))
            result[bucket] = trend
        return result

class NumVbuckt:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            num_error = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            nodeStats = values["nodeStats"]
            for node, vals in nodeStats.iteritems():
                if vals[-1] < accessor["threshold"]:
                    num_error.append({"node":node, "value":vals[-1]})
            if len(num_error) > 0:
                result[bucket] = {"error" : num_error}
        return result

class RebalanceStuck:
    def run(self, accessor):
        result = {}
        for bucket, bucket_stats in stats_buffer.node_stats.iteritems():
            num_error = []
            for node, stats_info in bucket_stats.iteritems():
                for key, value in stats_info.iteritems():
                    if key.find(accessor["counter"]) >= 0:
                        if accessor.has_key("threshold"):
                            if int(value) > accessor["threshold"]:
                                num_error.append({"node":node, "value": (key, value)})
                        else:
                            num_error.append({"node":node, "value": (key, value)})
            if len(num_error) > 0:
                result[bucket] = {"error" : num_error}
        return result

class MemoryFramentation:
    def run(self, accessor):
        result = {}
        for bucket, bucket_stats in stats_buffer.node_stats.iteritems():
            num_error = []
            for node, stats_info in bucket_stats.iteritems():
                for key, value in stats_info.iteritems():
                    if key.find(accessor["counter"]) >= 0:
                        if accessor.has_key("threshold"):
                            if int(value) > accessor["threshold"]:
                                if accessor.has_key("unit"):
                                    if accessor["unit"] == "time":
                                        num_error.append({"node":node, "value": (key, util.time_label(value))})
                                    elif accessor["unit"] == "size":
                                        num_error.append({"node":node, "value": (key, util.size_label(value))})
                                    else:
                                        num_error.append({"node":node, "value": (key, value)})
                                else:
                                    num_error.append({"node":node, "value": (key, value)})
            if len(num_error) > 0:
                result[bucket] = {"error" : num_error}
        return result

class EPEnginePerformance:
    def run(self, accessor):
        result = {}
        for bucket, bucket_stats in stats_buffer.node_stats.iteritems():
            num_error = []
            for node, stats_info in bucket_stats.iteritems():
                for key, value in stats_info.iteritems():
                    if key.find(accessor["counter"]) >= 0:
                        if accessor.has_key("threshold"):
                            if accessor["counter"] == "flusherState" and value != accessor["threshold"]:
                                num_error.append({"node":node, "value": (key, value)})
                            elif accessor["counter"] == "flusherCompleted" and value == accessor["threshold"]:
                                num_error.append({"node":node, "value": (key, value)})
                            else:
                                if value > accessor["threshold"]:
                                    num_error.append({"node":node, "value": (key, value)})
            if len(num_error) > 0:
                result[bucket] = {"error" : num_error}
        return result

class TotalDataSize:
    def run(self, accessor):
        result = []
        total = 0
        for node, nodeinfo in stats_buffer.nodes.iteritems():
            if nodeinfo["StorageInfo"].has_key("hdd"):
                total += nodeinfo['StorageInfo']['hdd']['usedByData']
        result.append(util.size_label(total))
        return result

class AvailableDiskSpace:
    def run(self, accessor):
        result = []
        total = 0
        for node, nodeinfo in stats_buffer.nodes.iteritems():
            if nodeinfo["StorageInfo"].has_key("hdd"):
                total += nodeinfo['StorageInfo']['hdd']['free']
        result.append(util.size_label(total))
        return result

ClusterCapsule = [
    {"name" : "TotalDataSize",
     "ingredients" : [
        {
            "name" : "totalDataSize",
            "description" : "Total Data Size across cluster",
            "code" : "TotalDataSize",
        }
     ],
     "clusterwise" : True,
     "perNode" : False,
     "perBucket" : False,
    },
    {"name" : "AvailableDiskSpace",
     "ingredients" : [
        {
            "name" : "availableDiskSpace",
            "description" : "Available disk space",
            "code" : "AvailableDiskSpace",
        }
     ],
     "clusterwise" : True,
     "perNode" : False,
     "perBucket" : False,
    },
   {"name" : "CacheMissRatio",
     "ingredients" : [
        {
            "name" : "cacheMissRatio",
            "description" : "Cache miss ratio",
            "counter" : "ep_cache_miss_rate",
            "scale" : "hour",
            "code" : "CacheMissRatio",
            "unit" : "percentage",
            "threshold" : 2,
        },
     ],
     "clusterwise" : True,
     "perNode" : True,
     "perBucket" : True,
     "indicator" : False,
     "nodeDisparate" : True,
    },
    {"name" : "DGM",
     "ingredients" : [
        {
            "name" : "dgm",
            "description" : "Disk to Memory Ratio",
            "code" : "DGMRatio"
        },
     ],
     "clusterwise" : True,
     "perNode" : False,
     "perBucket" : False,
    },
    {"name" : "BucketSummary",
     "ingredients" : [
        {
            "name" : "bucketSummary",
            "description" : "Bucket performance summary",
            "code" : "BucketSummary",
        },
     ],
     "clusterwise" : True,
    },
    {"name" : "ActiveReplicaResidentRatio",
     "ingredients" : [
        {
            "name" : "activeReplicaResidencyRatio",
            "description" : "Active and Replica Resident Ratio",
            "counter" : ["curr_items", "vb_replica_curr_items"],
            "scale" : "minute",
            "code" : "ARRatio",
            "threshold" : 1,
        },
     ],
     "clusterwise" : True,
     "perNode" : True,
     "perBucket" : True,
     "indicator" : True,
    },
    {"name" : "OPSPerformance",
     "ingredients" : [
        {
            "name" : "opsPerformance",
            "description" : "Read/Write/Delete ops ratio",
            "scale" : "minute",
            "counter" : ["cmd_get", "cmd_set", "delete_hits"],
            "code" : "OpsRatio",
        },
     ],
     "perBucket" : True,
    },
    {"name" : "GrowthRate",
     "ingredients" : [
        {
            "name" : "dataGrowthRateForItems",
            "description" : "Data Growth rate for items",
            "counter" : "curr_items",
            "scale" : "day",
            "code" : "ItemGrowth",
            "unit" : "percentage",
        },
     ]
    },
    {"name" : "VBucketNumber",
     "ingredients" : [
        {
            "name" : "activeVbucketNumber",
            "description" : "Active VBucket number is less than expected",
            "counter" : "vb_active_num",
            "scale" : "hour",
            "code" : "NumVbuckt",
            "threshold" : 1024,
        },
        {
            "name" : "replicaVBucketNumber",
            "description" : "Replica VBucket number is less than expected",
            "counter" : "vb_replica_num",
            "scale" : "hour",
            "code" : "NumVbuckt",
            "threshold" : 1024,
        },
     ],
     "indicator" : True,
    },
    {"name" : "MemoryUsage",
     "ingredients" : [
        {
            "name" : "memoryUsage",
            "description" : "Check memory usage",
            "counter" : "mem_used",
            "scale" : "hour",
            "code" : "MemUsed",
        },
     ],
     "nodeDisparate" : True,
    },
    {"name" : "RebalancePerformance",
     "ingredients" : [
        {
            "name" : "rebalanceStuck",
            "description" : "Check if rebalance is stuck",
            "counter" : "idle",
            "code" : "RebalanceStuck",
        },
        {
            "name" : "highBackfillRemaing",
            "description" : "Tap queue backfilll remaining is too high",
            "counter" : "ep_tap_queue_backfillremaining",
            "code" : "RebalanceStuck",
            "threshold" : 1000,
        },
     ],
     "indicator" : True,
    },
    {"name" : "MemoryFragmentation",
     "ingredients" : [
        {
            "name" : "totalFragmentation",
            "description" : "Total memory fragmentation",
            "counter" : "total_fragmentation_bytes",
            "code" : "MemoryFramentation",
            "unit" : "size",
            "threshold" : 1073741824,  # 1GB
        },
        {
            "name" : "diskDelete",
            "description" : "Averge disk delete time",
            "counter" : "disk_del",
            "code" : "MemoryFramentation",
            "unit" : "time",
            "threshold" : 1000     #1ms
        },
        {
            "name" : "diskUpdate",
            "description" : "Averge disk update time",
            "counter" : "disk_update",
            "code" : "MemoryFramentation",
            "unit" : "time",
            "threshold" : 1000     #1ms
        },
        {
            "name" : "diskInsert",
            "description" : "Averge disk insert time",
            "type" : "python",
            "counter" : "disk_insert",
            "code" : "MemoryFramentation",
            "unit" : "time",
            "threshold" : 1000     #1ms
        },
        {
            "name" : "diskInsert",
            "description" : "Averge disk insert time",
            "counter" : "disk_commit",
            "code" : "MemoryFramentation",
            "unit" : "time",
            "threshold" : 5000000     #10s
        },
     ],
     "indicator" : True,
    },
    {"name" : "EPEnginePerformance",
     "ingredients" : [
        {
            "name" : "flusherState",
            "description" : "Engine flusher state",
            "counter" : "ep_flusher_state",
            "code" : "EPEnginePerformance",
            "threshold" : "running",
        },
        {
            "name" : "flusherCompleted",
            "description" : "Flusher completed",
            "counter" : "ep_flusher_num_completed",
            "code" : "EPEnginePerformance",
            "threshold" : 0
        },
        {
            "name" : "avgItemLoadTime",
            "description" : "Average item loaded time",
            "counter" : "ep_bg_load_avg",
            "code" : "EPEnginePerformance",
            "threshold" : 100,
        },
        {
            "name" : "avgItemWaitTime",
            "description" : "Averge item waited time",
            "counter" : "ep_bg_wait_avg",
            "code" : "EPEnginePerformance",
            "threshold" : 100
        },
     ],
     "indicator" : True,
    },
]
