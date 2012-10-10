import datetime
import util_cli as util
import sys

import cluster_stats
import diskqueue_stats
import node_stats

import stats_buffer

capsules = [
    (node_stats.NodeCapsule, "node_stats"),
    (cluster_stats.ClusterCapsule, "cluster_stats"),
    (diskqueue_stats.DiskQueueCapsule, "diskqueue_stats"),
]

node_list = {}
bucket_list = []
cluster_symptoms = {}
bucket_symptoms = {}
bucket_node_symptoms = {}
node_symptoms = {}
indicator_error = {}
indicator_warn = {}
node_disparate = {}

class StatsAnalyzer:
    def __init__(self, log):
        self.log = log

    def run_analysis(self):

        for bucket in stats_buffer.buckets.iterkeys():
            bucket_list.append(bucket)
            bucket_symptoms[bucket] = []
            bucket_node_symptoms[bucket] = {}

        for capsule, package_name in capsules:
            for pill in capsule:
                self.log.debug(pill['name'])
                for counter in pill['ingredients']:
                    result = eval("{0}.{1}().run(counter)".format(package_name, counter['code']))

                    self.log.debug(counter)
                    if pill.has_key("clusterwise") and pill["clusterwise"] :
                        if isinstance(result, dict):
                            if result.has_key("cluster"):
                                cluster_symptoms[counter["name"]] = {"description" : counter["description"], "value":result["cluster"]}
                            else:
                                cluster_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}
                        else:
                            cluster_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}

                    if pill.has_key("perBucket") and pill["perBucket"] :
                        #bucket_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}
                        for bucket, values in result.iteritems():
                            if bucket == "cluster":
                                continue
                            for val in values:
                                if val[0] == "variance":
                                    continue
                                elif val[0] == "total":
                                    bucket_symptoms[bucket].append({"description" : counter["description"], "value" : values[-1][1]})
                                else:
                                    if bucket_node_symptoms[bucket].has_key(val[0]) == False:
                                        bucket_node_symptoms[bucket][val[0]] = []
                                    bucket_node_symptoms[bucket][val[0]].append({"description" : counter["description"], "value" : val[1]})

                    if pill.has_key("perNode") and pill["perNode"] :
                        node_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}
                    if pill.has_key("nodewise") and pill["nodewise"]:
                        node_list[counter["name"]] = {"description" : counter["description"], "value":result}

    def run_report(self, txtfile):
        dict = {
            "globals" : globals,
            "cluster_symptoms" : cluster_symptoms,
            "bucket_symptoms" : bucket_symptoms,
            "bucket_node_symptoms" : bucket_node_symptoms,
            "node_symptoms" : node_symptoms,
            "node_list" : node_list,
            "bucket_list" : bucket_list,
        }

        f = open(txtfile, 'w')
        report = {}
        report["Report Time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        report["Nodelist Overview"] = node_list
        report["Cluster Overview"] = cluster_symptoms
        report["Node Metrics"] = node_symptoms
        report["Bucket Metrics"] = bucket_symptoms
        report["Bucket Node Metrics"] = bucket_node_symptoms

        print >> f, util.pretty_print(report)
        f.close()
        sys.stderr.write("The run finished successfully. Please find output result at '{0}'".format(txtfile))