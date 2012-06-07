import stats_buffer
import util_cli as util

class AvgDiskQueue:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            #print bucket, stats_info
            disk_queue_avg_error = []
            disk_queue_avg_warn = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                avg = sum(vals) / samplesCount
                if avg > accessor["threshold"]["high"]:
                    disk_queue_avg_error.append({"node":node, "level":"red", "value":avg})
                elif avg > accessor["threshold"]["low"]:
                    disk_queue_avg_warn.append({"node":node, "level":"yellow", "value":avg})
            if len(disk_queue_avg_error) > 0:
                result[bucket] = {"error" : disk_queue_avg_error}
            if len(disk_queue_avg_warn) > 0:
                result[bucket] = {"warn" : disk_queue_avg_warn}
        return result

class DiskQueueTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            trend_error = []
            trend_warn = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                if a > accessor["threshold"]["high"]:
                    trend_error.append({"node":node, "level":"red", "value":a})
                elif a > accessor["threshold"]["low"]:
                    trend_warn.append({"node":node, "level":"yellow", "value":a})
            if len(trend_error) > 0:
                result[bucket] = {"error" : trend_error}
            if len(trend_warn) > 0:
                result[bucket] = {"warn" : trend_warn}
        return result

class TapQueueTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            trend_error = []
            trend_warn = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                if a > accessor["threshold"]["high"]:
                    trend_error.append({"node":node, "level":"red", "value":a})
                elif a > accessor["threshold"]["low"]:
                    trend_warn.append({"node":node, "level":"yellow", "value":a})
            if len(trend_error) > 0:
                result[bucket] = {"error" : trend_error}
            if len(trend_warn) > 0:
                result[bucket] = {"warn" : trend_warn}
        return result

class DiskQueueDrainingRate:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            #print bucket, stats_info
            disk_queue_avg_error = []
            disk_queue_avg_warn = []
            drain_values = stats_info[accessor["scale"]][accessor["counter"][0]]
            len_values = stats_info[accessor["scale"]][accessor["counter"][1]]
            nodeStats = drain_values["nodeStats"]
            samplesCount = drain_values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                avg = sum(vals) / samplesCount
                disk_len_vals = len_values["nodeStats"][node]
                len_avg = sum(disk_len_vals) / samplesCount
                if avg < accessor["threshold"]["drainRate"] and len_avg > accessor["threshold"]["diskLength"]:
                    disk_queue_avg_error.append({"node":node, "level":"red", "value":avg})
            if len(disk_queue_avg_error) > 0:
                result[bucket] = {"error" : disk_queue_avg_error}
        return result

DiskQueueCapsule = [
    {"name" : "DiskQueueDiagnosis",
     "description" : "",
     "ingredients" : [
        {
            "name" : "avgDiskQueueLength",
            "description" : "Persistence severely behind - averge disk queue length is above threshold",
            "counter" : "disk_write_queue",
            "pernode" : True,
            "scale" : "minute",
            "code" : "AvgDiskQueue",
            "threshold" : {
                "low" : 50000000,
                "high" : 1000000000
            },
        },
        {
            "name" : "diskQueueTrend",
            "description" : "Persistence severely behind - disk write queue continues growing",
            "counter" : "disk_write_queue",
            "pernode" : True,
            "scale" : "hour",
            "code" : "DiskQueueTrend",
            "threshold" : {
                "low" : 0,
                "high" : 0.25
            },
        },
     ],
     "indicator" : True,
    },
    {"name" : "ReplicationTrend",
     "ingredients" : [
        {
            "name" : "replicationTrend",
            "description" : "Replication severely behind - ",
            "counter" : "ep_tap_total_total_backlog_size",
            "pernode" : True,
            "scale" : "hour",
            "code" : "TapQueueTrend",
            "threshold" : {
                "low" : 0,
                "high" : 0.2
            },
        }
     ],
     "indicator" : True,
    },
     {"name" : "DiskQueueDrainingAnalysis",
     "description" : "",
     "ingredients" : [
        {
            "name" : "activeDiskQueueDrainRate",
            "description" : "Persistence severely behind - active disk queue draining rate is below threshold",
            "counter" : ["vb_active_queue_drain", "disk_write_queue"],
            "pernode" : True,
            "scale" : "minute",
            "code" : "DiskQueueDrainingRate",
            "threshold" : {
                "drainRate" : 0,
                "diskLength" : 100000,
            },
        },
        {
            "name" : "replicaDiskQueueDrainRate",
            "description" : "Persistence severely behind - replica disk queue draining rate is below threshold",
            "counter" : ["vb_replica_queue_drain", "disk_write_queue"],
            "pernode" : True,
            "scale" : "minute",
            "code" : "DiskQueueDrainingRate",
            "threshold" : {
                "drainRate" : 0,
                "diskLength" : 100000,
            },
        },
     ],
     "indicator" : True,
    },
]